#define DUCKDB_EXTENSION_MAIN

#include "holtfs_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
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

static string HoltfsLastError(const char *op) {
	auto msg = holt_last_error_message();
	if (!msg || !msg[0]) {
		return string(op) + " failed";
	}
	return string(op) + " failed: " + msg;
}

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

static void LoadInternal(ExtensionLoader &loader) {
	ScalarFunction version_function("holtfs_version", {}, LogicalType::VARCHAR, HoltfsVersionFunction);
	loader.RegisterFunction(version_function);
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
