#define DUCKDB_EXTENSION_MAIN

#include "holtfs_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "holt_ffi.h"

#include <limits>

namespace duckdb {

namespace {

static string HoltfsVersionString() {
#ifdef EXT_VERSION_HOLTFS
	return EXT_VERSION_HOLTFS;
#else
	return "holtfs-dev";
#endif
}

static void HoltfsVersionFunction(DataChunk &, ExpressionState &, Vector &result) {
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	result.SetValue(0, Value(HoltfsVersionString()));
}

struct HoltfsFilesBindData : public TableFunctionData {
	string index_path;
	string prefix;
	string delimiter;
	string start_after;
	idx_t max_files = 0;
	bool include_value = false;
};

struct HoltTreeHandle {
	~HoltTreeHandle() {
		if (tree) {
			holt_tree_close(tree);
		}
	}

	HoltTree *tree = nullptr;
};

static string HoltfsLastError(const char *op) {
	auto msg = holt_last_error_message();
	if (!msg || !msg[0]) {
		return string(op) + " failed";
	}
	return string(op) + " failed: " + msg;
}

static void ReadFilesNamedParameters(TableFunctionBindInput &input, HoltfsFilesBindData &result) {
	for (auto &kv : input.named_parameters) {
		if (kv.second.IsNull()) {
			throw BinderException("holtfs named parameters cannot be NULL");
		}
		if (kv.first == "prefix") {
			result.prefix = StringValue::Get(kv.second);
		} else if (kv.first == "delimiter") {
			result.delimiter = StringValue::Get(kv.second);
			if (result.delimiter.size() > 1) {
				throw BinderException("holtfs delimiter must be empty or a single byte");
			}
		} else if (kv.first == "start_after") {
			result.start_after = StringValue::Get(kv.second);
		} else if (kv.first == "max_files") {
			auto value = UBigIntValue::Get(kv.second);
			if (value > std::numeric_limits<idx_t>::max()) {
				throw BinderException("holtfs max_files exceeds idx_t");
			}
			result.max_files = static_cast<idx_t>(value);
		} else if (kv.first == "include_value") {
			result.include_value = BooleanValue::Get(kv.second);
		}
	}
}

static unique_ptr<FunctionData> HoltfsFilesBind(ClientContext &, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 1) {
		throw BinderException("holt_files(index_path, ...) expects exactly one VARCHAR argument");
	}

	auto result = make_uniq<HoltfsFilesBindData>();
	result->index_path = StringValue::Get(input.inputs[0]);
	ReadFilesNamedParameters(input, *result);

	names.emplace_back("entry_type");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("path");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("value");
	return_types.emplace_back(LogicalType::BLOB);
	names.emplace_back("version");
	return_types.emplace_back(LogicalType::UBIGINT);

	return std::move(result);
}

struct HoltfsFilesGlobalState : public GlobalTableFunctionState {
	~HoltfsFilesGlobalState() override {
		if (iter) {
			holt_iter_close(iter);
		}
		if (tree) {
			holt_tree_close(tree);
		}
	}

	HoltTree *tree = nullptr;
	HoltIter *iter = nullptr;
	idx_t emitted = 0;
};

static const uint8_t *OptionalData(const string &value) {
	if (value.empty()) {
		return nullptr;
	}
	return reinterpret_cast<const uint8_t *>(value.data());
}

static idx_t OptionalSize(const string &value) {
	return value.empty() ? 0 : value.size();
}

static int32_t DelimiterByte(const HoltfsFilesBindData &bind) {
	if (bind.delimiter.empty()) {
		return -1;
	}
	return static_cast<unsigned char>(bind.delimiter[0]);
}

static unique_ptr<GlobalTableFunctionState> HoltfsFilesInit(ClientContext &, TableFunctionInitInput &input) {
	auto &bind = input.bind_data->CastNoConst<HoltfsFilesBindData>();
	auto state = make_uniq<HoltfsFilesGlobalState>();

	if (holt_tree_open_with_wal_commit(bind.index_path.c_str(), HOLT_WAL_ENQUEUE, &state->tree) != HOLT_OK) {
		throw IOException(HoltfsLastError("holt_tree_open_with_wal_commit"));
	}

	auto prefix = reinterpret_cast<const uint8_t *>(bind.prefix.data());
	auto start_after = OptionalData(bind.start_after);
	auto start_after_len = OptionalSize(bind.start_after);
	auto delimiter = DelimiterByte(bind);
	auto rc = bind.include_value ? holt_tree_scan_records(state->tree, prefix, bind.prefix.size(), delimiter,
	                                                      start_after, start_after_len, &state->iter)
	                             : holt_tree_scan_keys(state->tree, prefix, bind.prefix.size(), delimiter, start_after,
	                                                   start_after_len, &state->iter);
	if (rc != HOLT_OK) {
		throw IOException(HoltfsLastError(bind.include_value ? "holt_tree_scan_records" : "holt_tree_scan_keys"));
	}

	return std::move(state);
}

static void SetBlob(Vector &column, idx_t row, HoltBytes bytes) {
	if (!bytes.ptr) {
		column.SetValue(row, Value());
		return;
	}
	column.SetValue(row, Value::BLOB(bytes.ptr, static_cast<idx_t>(bytes.len)));
}

static void SetPath(Vector &column, idx_t row, HoltBytes bytes) {
	if (!bytes.ptr) {
		column.SetValue(row, Value());
		return;
	}
	column.SetValue(row, Value(string(reinterpret_cast<const char *>(bytes.ptr), bytes.len)));
}

static void HoltfsFilesFunction(ClientContext &, TableFunctionInput &input, DataChunk &output) {
	auto &bind = input.bind_data->CastNoConst<HoltfsFilesBindData>();
	auto &state = input.global_state->Cast<HoltfsFilesGlobalState>();
	idx_t count = 0;

	while (count < STANDARD_VECTOR_SIZE && (!bind.max_files || state.emitted < bind.max_files)) {
		HoltEntry entry;
		auto rc = holt_iter_next(state.iter, &entry);
		if (rc == HOLT_ITER_END) {
			break;
		}
		if (rc != HOLT_OK) {
			throw IOException(HoltfsLastError("holt_iter_next"));
		}

		if (entry.kind == HOLT_ENTRY_KEY) {
			output.data[0].SetValue(count, Value("key"));
			SetPath(output.data[1], count, entry.path);
			SetBlob(output.data[2], count, entry.value);
			output.data[3].SetValue(count, Value::UBIGINT(entry.version));
		} else if (entry.kind == HOLT_ENTRY_COMMON_PREFIX) {
			output.data[0].SetValue(count, Value("common_prefix"));
			SetPath(output.data[1], count, entry.path);
			output.data[2].SetValue(count, Value());
			output.data[3].SetValue(count, Value::UBIGINT(0));
		} else {
			holt_entry_free(&entry);
			throw IOException("holt_iter_next returned an unknown entry kind");
		}

		holt_entry_free(&entry);
		state.emitted++;
		count++;
	}

	output.SetCardinality(count);
}

static void RegisterHoltFiles(ExtensionLoader &loader) {
	TableFunction function("holt_files", {LogicalType::VARCHAR}, HoltfsFilesFunction, HoltfsFilesBind, HoltfsFilesInit);
	function.named_parameters["prefix"] = LogicalType::VARCHAR;
	function.named_parameters["delimiter"] = LogicalType::VARCHAR;
	function.named_parameters["start_after"] = LogicalType::VARCHAR;
	function.named_parameters["max_files"] = LogicalType::UBIGINT;
	function.named_parameters["include_value"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(function);
}

struct HoltfsIndexBindData : public TableFunctionData {
	string source_path;
	string index_path;
};

struct HoltfsIndexGlobalState : public GlobalTableFunctionState {
	bool done = false;
};

static unique_ptr<FunctionData> HoltfsIndexBind(ClientContext &, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 2) {
		throw BinderException("holtfs_index(source_path, index_path) expects two VARCHAR arguments");
	}

	auto result = make_uniq<HoltfsIndexBindData>();
	result->source_path = StringValue::Get(input.inputs[0]);
	result->index_path = StringValue::Get(input.inputs[1]);

	names.emplace_back("source_path");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("index_path");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("indexed_files");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("indexed_bytes");
	return_types.emplace_back(LogicalType::UBIGINT);

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> HoltfsIndexInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<HoltfsIndexGlobalState>();
}

static string MetadataValue(uint64_t size) {
	return "size=" + std::to_string(size) + ";kind=file";
}

static void IndexOneFile(FileSystem &fs, HoltTree *tree, const string &path, uint64_t &files, uint64_t &bytes) {
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	auto file_size = handle->GetFileSize();
	if (file_size < 0) {
		throw IOException("failed to stat file '" + path + "'");
	}

	auto key = fs.ConvertSeparators(path);
	auto size = static_cast<uint64_t>(file_size);
	auto value = MetadataValue(size);
	auto rc = holt_tree_put(tree, reinterpret_cast<const uint8_t *>(key.data()), key.size(),
	                        reinterpret_cast<const uint8_t *>(value.data()), value.size());
	if (rc != HOLT_OK) {
		throw IOException(HoltfsLastError("holt_tree_put"));
	}

	files++;
	bytes += size;
}

static void IndexPath(FileSystem &fs, HoltTree *tree, const string &source, uint64_t &files, uint64_t &bytes) {
	if (fs.FileExists(source)) {
		IndexOneFile(fs, tree, source, files, bytes);
		return;
	}
	if (!fs.DirectoryExists(source)) {
		throw IOException("holtfs_index source path is neither a file nor a directory: " + source);
	}

	auto listed = fs.ListFiles(source, [&](const string &name, bool is_directory) {
		auto child = fs.JoinPath(source, name);
		if (is_directory) {
			IndexPath(fs, tree, child, files, bytes);
		} else {
			IndexOneFile(fs, tree, child, files, bytes);
		}
	});
	if (!listed) {
		throw IOException("failed to list source directory '" + source + "'");
	}
}

static void HoltfsIndexFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind = input.bind_data->CastNoConst<HoltfsIndexBindData>();
	auto &state = input.global_state->Cast<HoltfsIndexGlobalState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	state.done = true;

	HoltTreeHandle handle;
	if (holt_tree_open_with_wal_commit(bind.index_path.c_str(), HOLT_WAL_WRITE, &handle.tree) != HOLT_OK) {
		throw IOException(HoltfsLastError("holt_tree_open_with_wal_commit"));
	}

	uint64_t files = 0;
	uint64_t bytes = 0;
	auto &fs = FileSystem::GetFileSystem(context);
	IndexPath(fs, handle.tree, bind.source_path, files, bytes);

	if (holt_tree_checkpoint(handle.tree) != HOLT_OK) {
		throw IOException(HoltfsLastError("holt_tree_checkpoint"));
	}

	output.data[0].SetValue(0, Value(bind.source_path));
	output.data[1].SetValue(0, Value(bind.index_path));
	output.data[2].SetValue(0, Value::UBIGINT(files));
	output.data[3].SetValue(0, Value::UBIGINT(bytes));
	output.SetCardinality(1);
}

static void RegisterHoltfsIndex(ExtensionLoader &loader) {
	TableFunction function("holtfs_index", {LogicalType::VARCHAR, LogicalType::VARCHAR}, HoltfsIndexFunction,
	                       HoltfsIndexBind, HoltfsIndexInit);
	loader.RegisterFunction(function);
}

static void LoadInternal(ExtensionLoader &loader) {
	ScalarFunction version_function("holtfs_version", {}, LogicalType::VARCHAR, HoltfsVersionFunction);
	loader.RegisterFunction(version_function);
	RegisterHoltfsIndex(loader);
	RegisterHoltFiles(loader);
}

} // namespace

void HoltfsExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string HoltfsExtension::Name() {
	return "holtfs";
}

std::string HoltfsExtension::Version() const {
	return HoltfsVersionString();
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(holtfs, loader) {
	duckdb::LoadInternal(loader);
}
}
