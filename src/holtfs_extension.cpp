#define DUCKDB_EXTENSION_MAIN

#include "holtfs_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "holt_ffi.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace duckdb {

namespace {

enum class IndexMode : uint8_t { PERSISTENT, MEMORY };

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

static string LowerAscii(string value) {
	std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) { return std::tolower(c); });
	return value;
}

static IndexMode ParseMode(const string &value) {
	auto normalized = LowerAscii(value);
	if (normalized == "persistent") {
		return IndexMode::PERSISTENT;
	}
	if (normalized == "memory") {
		return IndexMode::MEMORY;
	}
	throw BinderException("holtfs mode must be 'persistent' or 'memory'");
}

static const char *ModeName(IndexMode mode) {
	return mode == IndexMode::PERSISTENT ? "persistent" : "memory";
}

static void ParseRefreshMode(const string &value) {
	if (LowerAscii(value) != "replace") {
		throw BinderException("holtfs refresh must be 'replace'");
	}
}

struct HoltTreeHandle {
	explicit HoltTreeHandle(HoltTree *tree) : tree(tree) {
	}

	~HoltTreeHandle() {
		if (tree) {
			holt_tree_close(tree);
		}
	}

	HoltTreeHandle(const HoltTreeHandle &) = delete;
	HoltTreeHandle &operator=(const HoltTreeHandle &) = delete;

	HoltTree *tree;
};

using HoltTreeRef = std::shared_ptr<HoltTreeHandle>;

class MemoryIndexRegistry {
public:
	void Publish(const string &name, HoltTreeRef handle) {
		if (name.empty()) {
			throw BinderException("holtfs memory index requires name");
		}
		std::lock_guard<std::mutex> lock(mu);
		indexes[name] = handle;
	}

	HoltTreeRef Get(const string &name) {
		std::lock_guard<std::mutex> lock(mu);
		auto entry = indexes.find(name);
		if (entry == indexes.end()) {
			throw IOException("holtfs memory index not found: " + name);
		}
		return entry->second;
	}

private:
	std::mutex mu;
	std::unordered_map<string, HoltTreeRef> indexes;
};

static MemoryIndexRegistry &MemoryIndexes() {
	static MemoryIndexRegistry registry;
	return registry;
}

static string HoltfsLastError(const char *op) {
	auto msg = holt_last_error_message();
	if (!msg || !msg[0]) {
		return string(op) + " failed";
	}
	return string(op) + " failed: " + msg;
}

struct HoltfsFilesBindData : public TableFunctionData {
	string index_ref;
	IndexMode mode = IndexMode::PERSISTENT;
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
		if (kv.first == "mode") {
			result.mode = ParseMode(StringValue::Get(kv.second));
		} else if (kv.first == "prefix") {
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
		throw BinderException("holt_files(index_ref, ...) expects exactly one VARCHAR argument");
	}

	auto result = make_uniq<HoltfsFilesBindData>();
	result->index_ref = StringValue::Get(input.inputs[0]);
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
		if (owned_tree) {
			holt_tree_close(owned_tree);
		}
	}

	HoltTree *Tree() {
		return memory_tree ? memory_tree->tree : owned_tree;
	}

	HoltTree *owned_tree = nullptr;
	HoltTreeRef memory_tree;
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

	if (bind.mode == IndexMode::PERSISTENT) {
		if (holt_tree_open_with_wal_commit(bind.index_ref.c_str(), HOLT_WAL_ENQUEUE, &state->owned_tree) != HOLT_OK) {
			throw IOException(HoltfsLastError("holt_tree_open_with_wal_commit"));
		}
	} else {
		state->memory_tree = MemoryIndexes().Get(bind.index_ref);
	}

	auto prefix = reinterpret_cast<const uint8_t *>(bind.prefix.data());
	auto start_after = OptionalData(bind.start_after);
	auto start_after_len = OptionalSize(bind.start_after);
	auto delimiter = DelimiterByte(bind);
	auto tree = state->Tree();
	auto rc = bind.include_value ? holt_tree_scan_records(tree, prefix, bind.prefix.size(), delimiter, start_after,
	                                                      start_after_len, &state->iter)
	                             : holt_tree_scan_keys(tree, prefix, bind.prefix.size(), delimiter, start_after,
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
	function.named_parameters["mode"] = LogicalType::VARCHAR;
	function.named_parameters["prefix"] = LogicalType::VARCHAR;
	function.named_parameters["delimiter"] = LogicalType::VARCHAR;
	function.named_parameters["start_after"] = LogicalType::VARCHAR;
	function.named_parameters["max_files"] = LogicalType::UBIGINT;
	function.named_parameters["include_value"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(function);
}

struct HoltfsIndexBindData : public TableFunctionData {
	string source_path;
	IndexMode mode = IndexMode::PERSISTENT;
	string index_path;
	string name;
};

struct HoltfsIndexGlobalState : public GlobalTableFunctionState {
	bool done = false;
};

static void ReadIndexNamedParameters(TableFunctionBindInput &input, HoltfsIndexBindData &result) {
	for (auto &kv : input.named_parameters) {
		if (kv.second.IsNull()) {
			throw BinderException("holtfs named parameters cannot be NULL");
		}
		if (kv.first == "mode") {
			result.mode = ParseMode(StringValue::Get(kv.second));
		} else if (kv.first == "index_path") {
			result.index_path = StringValue::Get(kv.second);
		} else if (kv.first == "name") {
			result.name = StringValue::Get(kv.second);
		} else if (kv.first == "refresh") {
			ParseRefreshMode(StringValue::Get(kv.second));
		}
	}
}

static string IndexRef(const HoltfsIndexBindData &bind) {
	return bind.mode == IndexMode::PERSISTENT ? bind.index_path : bind.name;
}

static unique_ptr<FunctionData> HoltfsIndexBind(ClientContext &, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 1) {
		throw BinderException("holtfs_index(source_path, ...) expects exactly one VARCHAR argument");
	}

	auto result = make_uniq<HoltfsIndexBindData>();
	result->source_path = StringValue::Get(input.inputs[0]);
	ReadIndexNamedParameters(input, *result);

	if (result->mode == IndexMode::PERSISTENT) {
		if (result->index_path.empty()) {
			throw BinderException("holtfs persistent index requires index_path");
		}
		if (!result->name.empty()) {
			throw BinderException("holtfs persistent index does not accept name");
		}
	} else {
		if (result->name.empty()) {
			throw BinderException("holtfs memory index requires name");
		}
		if (!result->index_path.empty()) {
			throw BinderException("holtfs memory index does not accept index_path");
		}
	}

	names.emplace_back("source_path");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("mode");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("index_ref");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("refresh");
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

static string MetadataValue(uint64_t size, int64_t mtime_us) {
	return "size=" + std::to_string(size) + ";kind=file;mtime_us=" + std::to_string(mtime_us);
}

struct IndexedFile {
	string key;
	string value;
	uint64_t size;
};

static IndexedFile ReadFileMetadata(FileSystem &fs, const string &path) {
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	auto file_size = handle->GetFileSize();
	if (file_size < 0) {
		throw IOException("failed to stat file '" + path + "'");
	}

	IndexedFile file;
	file.key = fs.ConvertSeparators(path);
	file.size = static_cast<uint64_t>(file_size);
	auto mtime_us = Timestamp::GetEpochMicroSeconds(handle->Stats().last_modification_time);
	file.value = MetadataValue(file.size, mtime_us);
	return file;
}

static void IndexOneFile(FileSystem &fs, HoltTree *tree, const string &path, uint64_t &files, uint64_t &bytes) {
	auto file = ReadFileMetadata(fs, path);
	auto rc = holt_tree_put(tree, reinterpret_cast<const uint8_t *>(file.key.data()), file.key.size(),
	                        reinterpret_cast<const uint8_t *>(file.value.data()), file.value.size());
	if (rc != HOLT_OK) {
		throw IOException(HoltfsLastError("holt_tree_put"));
	}

	files++;
	bytes += file.size;
}

template <class FileCallback>
static bool WalkSourceFiles(FileSystem &fs, const string &source, FileCallback callback) {
	if (fs.FileExists(source)) {
		callback(source);
		return true;
	}
	if (!fs.DirectoryExists(source)) {
		return false;
	}

	auto listed = fs.ListFiles(source, [&](const string &name, bool is_directory) {
		auto child = fs.JoinPath(source, name);
		if (is_directory) {
			if (!WalkSourceFiles(fs, child, callback)) {
				throw IOException("source path disappeared while walking: " + child);
			}
		} else {
			callback(child);
		}
	});
	if (!listed) {
		throw IOException("failed to list source directory '" + source + "'");
	}
	return true;
}

static void IndexPath(FileSystem &fs, HoltTree *tree, const string &source, uint64_t &files, uint64_t &bytes) {
	auto exists = WalkSourceFiles(fs, source, [&](const string &path) { IndexOneFile(fs, tree, path, files, bytes); });
	if (!exists) {
		throw IOException("holtfs_index source path is neither a file nor a directory: " + source);
	}
}

static void ValidateIndexLocation(const string &source, const string &index_path) {
	if (source == index_path) {
		throw IOException("holtfs index_path must differ from source_path");
	}
	if (index_path.size() > source.size() && index_path.compare(0, source.size(), source) == 0 &&
	    (index_path[source.size()] == '/' || index_path[source.size()] == '\\')) {
		throw IOException("holtfs index_path must not be inside source_path");
	}
}

static void RemovePathIfExists(FileSystem &fs, const string &path) {
	if (fs.DirectoryExists(path)) {
		fs.RemoveDirectory(path);
	} else if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}
}

static string BuildTempIndexPath(const string &index_path) {
	auto now = std::chrono::steady_clock::now().time_since_epoch().count();
	return index_path + ".building." + std::to_string(now);
}

static void BuildMemoryIndex(FileSystem &fs, const HoltfsIndexBindData &bind, uint64_t &files, uint64_t &bytes) {
	HoltTree *tree = nullptr;
	if (holt_tree_open_memory(&tree) != HOLT_OK) {
		throw IOException(HoltfsLastError("holt_tree_open_memory"));
	}
	auto handle = std::make_shared<HoltTreeHandle>(tree);
	IndexPath(fs, handle->tree, bind.source_path, files, bytes);
	MemoryIndexes().Publish(bind.name, handle);
}

static void BuildPersistentIndex(FileSystem &fs, const HoltfsIndexBindData &bind, uint64_t &files, uint64_t &bytes) {
	ValidateIndexLocation(bind.source_path, bind.index_path);
	auto temp_path = BuildTempIndexPath(bind.index_path);
	RemovePathIfExists(fs, temp_path);

	HoltTree *tree = nullptr;
	if (holt_tree_open_with_wal_commit(temp_path.c_str(), HOLT_WAL_WRITE, &tree) != HOLT_OK) {
		throw IOException(HoltfsLastError("holt_tree_open_with_wal_commit"));
	}
	try {
		HoltTreeHandle handle(tree);
		IndexPath(fs, handle.tree, bind.source_path, files, bytes);

		if (holt_tree_checkpoint(handle.tree) != HOLT_OK) {
			throw IOException(HoltfsLastError("holt_tree_checkpoint"));
		}
	} catch (...) {
		RemovePathIfExists(fs, temp_path);
		throw;
	}

	RemovePathIfExists(fs, bind.index_path);
	fs.MoveFile(temp_path, bind.index_path);
}

static void HoltfsIndexFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind = input.bind_data->CastNoConst<HoltfsIndexBindData>();
	auto &state = input.global_state->Cast<HoltfsIndexGlobalState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	state.done = true;

	uint64_t files = 0;
	uint64_t bytes = 0;
	auto &fs = FileSystem::GetFileSystem(context);
	if (bind.mode == IndexMode::PERSISTENT) {
		BuildPersistentIndex(fs, bind, files, bytes);
	} else {
		BuildMemoryIndex(fs, bind, files, bytes);
	}

	output.data[0].SetValue(0, Value(bind.source_path));
	output.data[1].SetValue(0, Value(ModeName(bind.mode)));
	output.data[2].SetValue(0, Value(IndexRef(bind)));
	output.data[3].SetValue(0, Value("replace"));
	output.data[4].SetValue(0, Value::UBIGINT(files));
	output.data[5].SetValue(0, Value::UBIGINT(bytes));
	output.SetCardinality(1);
}

static void RegisterHoltfsIndex(ExtensionLoader &loader) {
	TableFunction function("holtfs_index", {LogicalType::VARCHAR}, HoltfsIndexFunction, HoltfsIndexBind,
	                       HoltfsIndexInit);
	function.named_parameters["mode"] = LogicalType::VARCHAR;
	function.named_parameters["index_path"] = LogicalType::VARCHAR;
	function.named_parameters["name"] = LogicalType::VARCHAR;
	function.named_parameters["refresh"] = LogicalType::VARCHAR;
	loader.RegisterFunction(function);
}

struct HoltfsValidateBindData : public TableFunctionData {
	string source_path;
	string index_ref;
	IndexMode mode = IndexMode::PERSISTENT;
};

struct HoltfsValidateGlobalState : public GlobalTableFunctionState {
	bool done = false;
};

struct ValidationStats {
	bool source_exists = false;
	uint64_t source_files = 0;
	uint64_t indexed_files = 0;
	uint64_t matched_files = 0;
	uint64_t changed_files = 0;
	uint64_t missing_files = 0;
	uint64_t deleted_files = 0;
};

static void ReadValidateNamedParameters(TableFunctionBindInput &input, HoltfsValidateBindData &result) {
	for (auto &kv : input.named_parameters) {
		if (kv.second.IsNull()) {
			throw BinderException("holtfs named parameters cannot be NULL");
		}
		if (kv.first == "mode") {
			result.mode = ParseMode(StringValue::Get(kv.second));
		}
	}
}

static unique_ptr<FunctionData> HoltfsValidateBind(ClientContext &, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 2) {
		throw BinderException("holtfs_validate(source_path, index_ref, ...) expects exactly two VARCHAR arguments");
	}

	auto result = make_uniq<HoltfsValidateBindData>();
	result->source_path = StringValue::Get(input.inputs[0]);
	result->index_ref = StringValue::Get(input.inputs[1]);
	ReadValidateNamedParameters(input, *result);

	names.emplace_back("source_path");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("mode");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("index_ref");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("source_exists");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("source_files");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("indexed_files");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("matched_files");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("changed_files");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("missing_files");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("deleted_files");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("is_current");
	return_types.emplace_back(LogicalType::BOOLEAN);

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> HoltfsValidateInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<HoltfsValidateGlobalState>();
}

static bool RecordValueEquals(const HoltRecord &record, const string &value) {
	return record.value.len == value.size() &&
	       (value.empty() || std::memcmp(record.value.ptr, value.data(), value.size()) == 0);
}

static void ValidateOneSourceFile(FileSystem &fs, HoltTree *tree, const string &path, ValidationStats &stats) {
	auto file = ReadFileMetadata(fs, path);
	HoltRecord record = {};
	auto rc = holt_tree_get(tree, reinterpret_cast<const uint8_t *>(file.key.data()), file.key.size(), &record);
	if (rc != HOLT_OK) {
		throw IOException(HoltfsLastError("holt_tree_get"));
	}

	stats.source_files++;
	if (!record.found) {
		stats.missing_files++;
		holt_record_free(&record);
		return;
	}
	if (RecordValueEquals(record, file.value)) {
		stats.matched_files++;
	} else {
		stats.changed_files++;
	}
	holt_record_free(&record);
}

static void ValidateSourcePath(FileSystem &fs, HoltTree *tree, const string &source, ValidationStats &stats) {
	stats.source_exists =
	    WalkSourceFiles(fs, source, [&](const string &path) { ValidateOneSourceFile(fs, tree, path, stats); });
}

static string BytesToString(HoltBytes bytes) {
	if (!bytes.ptr) {
		return string();
	}
	return string(reinterpret_cast<const char *>(bytes.ptr), bytes.len);
}

static bool IsUnderSourcePath(const string &key, const string &source) {
	if (key == source) {
		return true;
	}
	return key.size() > source.size() && key.compare(0, source.size(), source) == 0 &&
	       (key[source.size()] == '/' || key[source.size()] == '\\');
}

static void CountIndexedFiles(FileSystem &fs, HoltTree *tree, const string &source, ValidationStats &stats) {
	auto prefix = fs.ConvertSeparators(source);
	HoltIter *iter = nullptr;
	auto rc = holt_tree_scan_records(tree, reinterpret_cast<const uint8_t *>(prefix.data()), prefix.size(), -1, nullptr,
	                                 0, &iter);
	if (rc != HOLT_OK) {
		throw IOException(HoltfsLastError("holt_tree_scan_records"));
	}

	while (true) {
		HoltEntry entry;
		rc = holt_iter_next(iter, &entry);
		if (rc == HOLT_ITER_END) {
			break;
		}
		if (rc != HOLT_OK) {
			holt_iter_close(iter);
			throw IOException(HoltfsLastError("holt_iter_next"));
		}

		if (entry.kind == HOLT_ENTRY_KEY) {
			auto path = BytesToString(entry.path);
			if (IsUnderSourcePath(path, prefix)) {
				stats.indexed_files++;
				if (!fs.FileExists(path)) {
					stats.deleted_files++;
				}
			}
		}
		holt_entry_free(&entry);
	}
	holt_iter_close(iter);
}

static void HoltfsValidateFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind = input.bind_data->CastNoConst<HoltfsValidateBindData>();
	auto &state = input.global_state->Cast<HoltfsValidateGlobalState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	state.done = true;

	HoltTree *owned_tree = nullptr;
	HoltTreeRef memory_tree;
	if (bind.mode == IndexMode::PERSISTENT) {
		if (holt_tree_open_with_wal_commit(bind.index_ref.c_str(), HOLT_WAL_ENQUEUE, &owned_tree) != HOLT_OK) {
			throw IOException(HoltfsLastError("holt_tree_open_with_wal_commit"));
		}
	} else {
		memory_tree = MemoryIndexes().Get(bind.index_ref);
	}

	HoltTreeHandle owned_handle(owned_tree);
	auto tree = memory_tree ? memory_tree->tree : owned_handle.tree;
	auto &fs = FileSystem::GetFileSystem(context);
	ValidationStats stats;
	ValidateSourcePath(fs, tree, bind.source_path, stats);
	CountIndexedFiles(fs, tree, bind.source_path, stats);
	auto is_current = stats.source_exists && stats.changed_files == 0 && stats.missing_files == 0 &&
	                  stats.deleted_files == 0 && stats.source_files == stats.indexed_files;

	output.data[0].SetValue(0, Value(bind.source_path));
	output.data[1].SetValue(0, Value(ModeName(bind.mode)));
	output.data[2].SetValue(0, Value(bind.index_ref));
	output.data[3].SetValue(0, Value::BOOLEAN(stats.source_exists));
	output.data[4].SetValue(0, Value::UBIGINT(stats.source_files));
	output.data[5].SetValue(0, Value::UBIGINT(stats.indexed_files));
	output.data[6].SetValue(0, Value::UBIGINT(stats.matched_files));
	output.data[7].SetValue(0, Value::UBIGINT(stats.changed_files));
	output.data[8].SetValue(0, Value::UBIGINT(stats.missing_files));
	output.data[9].SetValue(0, Value::UBIGINT(stats.deleted_files));
	output.data[10].SetValue(0, Value::BOOLEAN(is_current));
	output.SetCardinality(1);
}

static void RegisterHoltfsValidate(ExtensionLoader &loader) {
	TableFunction function("holtfs_validate", {LogicalType::VARCHAR, LogicalType::VARCHAR}, HoltfsValidateFunction,
	                       HoltfsValidateBind, HoltfsValidateInit);
	function.named_parameters["mode"] = LogicalType::VARCHAR;
	loader.RegisterFunction(function);
}

static void LoadInternal(ExtensionLoader &loader) {
	ScalarFunction version_function("holtfs_version", {}, LogicalType::VARCHAR, HoltfsVersionFunction);
	loader.RegisterFunction(version_function);
	RegisterHoltfsIndex(loader);
	RegisterHoltFiles(loader);
	RegisterHoltfsValidate(loader);
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
