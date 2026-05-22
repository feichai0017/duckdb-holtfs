#define DUCKDB_EXTENSION_MAIN

#include "holtfs_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/common/enums/file_glob_options.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "holt_ffi.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

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

struct OpenIndexTree {
	OpenIndexTree() = default;

	OpenIndexTree(OpenIndexTree &&other) noexcept
	    : owned_tree(other.owned_tree), memory_tree(std::move(other.memory_tree)) {
		other.owned_tree = nullptr;
	}

	OpenIndexTree &operator=(OpenIndexTree &&other) noexcept {
		if (this != &other) {
			if (owned_tree) {
				holt_tree_close(owned_tree);
			}
			owned_tree = other.owned_tree;
			memory_tree = std::move(other.memory_tree);
			other.owned_tree = nullptr;
		}
		return *this;
	}

	~OpenIndexTree() {
		if (owned_tree) {
			holt_tree_close(owned_tree);
		}
	}

	OpenIndexTree(const OpenIndexTree &) = delete;
	OpenIndexTree &operator=(const OpenIndexTree &) = delete;

	HoltTree *Tree() const {
		return memory_tree ? memory_tree->tree : owned_tree;
	}

	HoltTree *owned_tree = nullptr;
	HoltTreeRef memory_tree;
};

static OpenIndexTree OpenIndexForRead(const string &index_ref, IndexMode mode) {
	OpenIndexTree index;
	if (mode == IndexMode::PERSISTENT) {
		if (holt_tree_open_with_wal_commit(index_ref.c_str(), HOLT_WAL_ENQUEUE, &index.owned_tree) != HOLT_OK) {
			throw IOException(HoltfsLastError("holt_tree_open_with_wal_commit"));
		}
	} else {
		index.memory_tree = MemoryIndexes().Get(index_ref);
	}
	return index;
}

static string InternalKeyPrefix() {
	return string("\0holtfs/", 8);
}

static string InternalKey(const char *name) {
	return InternalKeyPrefix() + name;
}

static bool IsInternalKey(const string &key) {
	auto prefix = InternalKeyPrefix();
	return key.size() >= prefix.size() && key.compare(0, prefix.size(), prefix) == 0;
}

static uint64_t NowMicros() {
	auto now = std::chrono::system_clock::now().time_since_epoch();
	return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(now).count());
}

static string BytesToString(HoltBytes bytes) {
	if (!bytes.ptr) {
		return string();
	}
	return string(reinterpret_cast<const char *>(bytes.ptr), bytes.len);
}

static void PutString(HoltTree *tree, const string &key, const string &value) {
	auto rc = holt_tree_put(tree, reinterpret_cast<const uint8_t *>(key.data()), key.size(),
	                        reinterpret_cast<const uint8_t *>(value.data()), value.size());
	if (rc != HOLT_OK) {
		throw IOException(HoltfsLastError("holt_tree_put"));
	}
}

static bool GetString(HoltTree *tree, const string &key, string &value) {
	HoltRecord record = {};
	auto rc = holt_tree_get(tree, reinterpret_cast<const uint8_t *>(key.data()), key.size(), &record);
	if (rc != HOLT_OK) {
		throw IOException(HoltfsLastError("holt_tree_get"));
	}
	if (!record.found) {
		holt_record_free(&record);
		return false;
	}
	value = BytesToString(record.value);
	holt_record_free(&record);
	return true;
}

static void PutU64(HoltTree *tree, const char *key, uint64_t value) {
	PutString(tree, InternalKey(key), std::to_string(value));
}

static bool GetU64(HoltTree *tree, const char *key, uint64_t &value) {
	string raw;
	if (!GetString(tree, InternalKey(key), raw)) {
		return false;
	}
	try {
		size_t parsed = 0;
		value = static_cast<uint64_t>(std::stoull(raw, &parsed));
		if (parsed != raw.size()) {
			throw IOException("holtfs manifest value is not a u64: " + string(key));
		}
	} catch (const std::exception &) {
		throw IOException("holtfs manifest value is not a u64: " + string(key));
	}
	return true;
}

struct IndexManifest {
	bool exists = false;
	string source_path;
	uint64_t built_at_us = 0;
	uint64_t refreshed_at_us = 0;
	uint64_t indexed_files = 0;
	uint64_t indexed_bytes = 0;
};

static IndexManifest ReadManifest(HoltTree *tree) {
	IndexManifest manifest;
	manifest.exists = GetString(tree, InternalKey("source_path"), manifest.source_path);
	if (!manifest.exists) {
		return manifest;
	}
	GetU64(tree, "built_at_us", manifest.built_at_us);
	GetU64(tree, "refreshed_at_us", manifest.refreshed_at_us);
	GetU64(tree, "indexed_files", manifest.indexed_files);
	GetU64(tree, "indexed_bytes", manifest.indexed_bytes);
	return manifest;
}

static void WriteManifest(HoltTree *tree, const string &source_path, uint64_t built_at_us, uint64_t refreshed_at_us,
                          uint64_t indexed_files, uint64_t indexed_bytes) {
	PutString(tree, InternalKey("format_version"), "1");
	PutString(tree, InternalKey("source_path"), source_path);
	PutU64(tree, "built_at_us", built_at_us);
	PutU64(tree, "refreshed_at_us", refreshed_at_us);
	PutU64(tree, "indexed_files", indexed_files);
	PutU64(tree, "indexed_bytes", indexed_bytes);
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

		auto path = BytesToString(entry.path);
		if (IsInternalKey(path)) {
			holt_entry_free(&entry);
			continue;
		}

		if (entry.kind == HOLT_ENTRY_KEY) {
			output.data[0].SetValue(count, Value("key"));
			output.data[1].SetValue(count, Value(path));
			SetBlob(output.data[2], count, entry.value);
			output.data[3].SetValue(count, Value::UBIGINT(entry.version));
		} else if (entry.kind == HOLT_ENTRY_COMMON_PREFIX) {
			output.data[0].SetValue(count, Value("common_prefix"));
			output.data[1].SetValue(count, Value(path));
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
static bool WalkLocalSourceFiles(FileSystem &fs, const string &source, FileCallback callback) {
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
			if (!WalkLocalSourceFiles(fs, child, callback)) {
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

template <class FileCallback>
static bool GlobSourceFiles(FileSystem &fs, const string &source, FileGlobOptions behavior, FileCallback callback) {
	auto files = fs.GlobFileList(source, FileGlobInput(behavior))->GetAllFiles();
	if (files.empty()) {
		return false;
	}
	std::sort(files.begin(), files.end());
	for (auto &file : files) {
		callback(file.path);
	}
	return true;
}

static string JoinPathComponent(FileSystem &fs, const string &root, const string &component) {
	if (root.empty()) {
		return component;
	}
	if (root.back() == '/' || root.back() == '\\') {
		return root + component;
	}
	return fs.JoinPath(root, component);
}

static string ParquetDatasetGlob(FileSystem &fs, const string &source) {
	return JoinPathComponent(fs, JoinPathComponent(fs, source, "**"), "*.parquet");
}

template <class FileCallback>
static bool ExpandSourceFiles(FileSystem &fs, const string &source, FileCallback callback) {
	if (FileSystem::HasGlob(source)) {
		return GlobSourceFiles(fs, source, FileGlobOptions::ALLOW_EMPTY, callback);
	}
	if (WalkLocalSourceFiles(fs, source, callback)) {
		return true;
	}
	return GlobSourceFiles(fs, ParquetDatasetGlob(fs, source), FileGlobOptions::ALLOW_EMPTY, callback);
}

static void IndexPath(FileSystem &fs, HoltTree *tree, const string &source, uint64_t &files, uint64_t &bytes) {
	auto exists =
	    ExpandSourceFiles(fs, source, [&](const string &path) { IndexOneFile(fs, tree, path, files, bytes); });
	if (!exists) {
		if (FileSystem::HasGlob(source)) {
			throw IOException("holtfs_index source glob matched no files: " + source);
		}
		throw IOException("holtfs_index source path is neither a file nor a directory or parquet dataset: " + source);
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
	auto now = NowMicros();
	WriteManifest(handle->tree, bind.source_path, now, now, files, bytes);
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
		auto now = NowMicros();
		WriteManifest(handle.tree, bind.source_path, now, now, files, bytes);

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
	output.data[3].SetValue(0, Value::UBIGINT(files));
	output.data[4].SetValue(0, Value::UBIGINT(bytes));
	output.SetCardinality(1);
}

static void RegisterHoltfsIndex(ExtensionLoader &loader) {
	TableFunction function("holtfs_index", {LogicalType::VARCHAR}, HoltfsIndexFunction, HoltfsIndexBind,
	                       HoltfsIndexInit);
	function.named_parameters["mode"] = LogicalType::VARCHAR;
	function.named_parameters["index_path"] = LogicalType::VARCHAR;
	function.named_parameters["name"] = LogicalType::VARCHAR;
	loader.RegisterFunction(function);
}

static bool IsUnderSourcePath(const string &key, const string &source) {
	if (key == source) {
		return true;
	}
	if (!source.empty() && (source.back() == '/' || source.back() == '\\')) {
		return key.size() >= source.size() && key.compare(0, source.size(), source) == 0;
	}
	return key.size() > source.size() && key.compare(0, source.size(), source) == 0 &&
	       (key[source.size()] == '/' || key[source.size()] == '\\');
}

static bool LooksLikeAbsolutePath(const string &path) {
	return !path.empty() && (path[0] == '/' || path.find("://") != string::npos || (path.size() > 1 && path[1] == ':'));
}

static string ResolveSubtreePath(FileSystem &fs, const string &source_path, const string &prefix) {
	if (prefix.empty()) {
		return source_path;
	}
	if (LooksLikeAbsolutePath(prefix)) {
		return prefix;
	}
	return fs.JoinPath(source_path, prefix);
}

struct CollectedFiles {
	vector<IndexedFile> files;
	std::unordered_set<string> keys;
	uint64_t bytes = 0;
	bool source_exists = false;
};

static CollectedFiles CollectSourceFiles(FileSystem &fs, const string &source) {
	CollectedFiles result;
	result.source_exists = ExpandSourceFiles(fs, source, [&](const string &path) {
		auto file = ReadFileMetadata(fs, path);
		result.bytes += file.size;
		result.keys.insert(file.key);
		result.files.push_back(std::move(file));
	});
	return result;
}

template <class RecordCallback>
static void ScanIndexRecordsFrom(HoltTree *tree, const string &prefix, const string &start_after,
                                 RecordCallback callback) {
	HoltIter *iter = nullptr;
	auto rc = holt_tree_scan_records(tree, reinterpret_cast<const uint8_t *>(prefix.data()), prefix.size(), -1,
	                                 OptionalData(start_after), OptionalSize(start_after), &iter);
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
			auto key = BytesToString(entry.path);
			auto value = BytesToString(entry.value);
			holt_entry_free(&entry);
			callback(key, value);
		} else {
			holt_entry_free(&entry);
		}
	}
	holt_iter_close(iter);
}

template <class RecordCallback>
static void ScanIndexRecords(HoltTree *tree, const string &prefix, RecordCallback callback) {
	ScanIndexRecordsFrom(tree, prefix, string(), callback);
}

static uint64_t MetadataSize(const string &value) {
	if (value.compare(0, 5, "size=") != 0) {
		return 0;
	}
	auto end = value.find(';', 5);
	auto raw = end == string::npos ? value.substr(5) : value.substr(5, end - 5);
	try {
		size_t parsed = 0;
		auto size = static_cast<uint64_t>(std::stoull(raw, &parsed));
		return parsed == raw.size() ? size : 0;
	} catch (const std::exception &) {
		return 0;
	}
}

struct IndexContentStats {
	uint64_t files = 0;
	uint64_t bytes = 0;
};

static IndexContentStats CountIndexContent(HoltTree *tree) {
	IndexContentStats stats;
	ScanIndexRecords(tree, string(), [&](const string &key, const string &value) {
		if (!IsInternalKey(key)) {
			stats.files++;
			stats.bytes += MetadataSize(value);
		}
	});
	return stats;
}

static vector<string> CollectIndexedKeys(HoltTree *tree, const string &prefix) {
	vector<string> keys;
	ScanIndexRecords(tree, prefix, [&](const string &key, const string &) {
		if (!IsInternalKey(key) && IsUnderSourcePath(key, prefix)) {
			keys.push_back(key);
		}
	});
	return keys;
}

struct OptionalNamedParameter {
	bool set = false;
	Value value;
};

struct HoltfsParquetScanBindData {
	string index_ref;
	IndexMode mode = IndexMode::PERSISTENT;
	string prefix;
	string start_after;
	idx_t max_files = 0;
	OptionalNamedParameter filename;
	OptionalNamedParameter hive_partitioning;
	OptionalNamedParameter union_by_name;
};

static bool EndsWith(const string &value, const string &suffix) {
	return value.size() >= suffix.size() && value.compare(value.size() - suffix.size(), suffix.size(), suffix) == 0;
}

static bool IsParquetDataFile(const string &path) {
	return EndsWith(LowerAscii(path), ".parquet");
}

static void ReadParquetScanNamedParameters(TableFunctionBindInput &input, HoltfsParquetScanBindData &result) {
	for (auto &kv : input.named_parameters) {
		if (kv.second.IsNull()) {
			throw BinderException("holt_parquet_scan named parameters cannot be NULL");
		}
		if (kv.first == "mode") {
			result.mode = ParseMode(StringValue::Get(kv.second));
		} else if (kv.first == "prefix") {
			result.prefix = StringValue::Get(kv.second);
		} else if (kv.first == "start_after") {
			result.start_after = StringValue::Get(kv.second);
		} else if (kv.first == "max_files") {
			auto value = UBigIntValue::Get(kv.second);
			if (value > std::numeric_limits<idx_t>::max()) {
				throw BinderException("holt_parquet_scan max_files exceeds idx_t");
			}
			result.max_files = static_cast<idx_t>(value);
		} else if (kv.first == "filename") {
			result.filename.set = true;
			result.filename.value = kv.second;
		} else if (kv.first == "hive_partitioning") {
			result.hive_partitioning.set = true;
			result.hive_partitioning.value = kv.second;
		} else if (kv.first == "union_by_name") {
			result.union_by_name.set = true;
			result.union_by_name.value = kv.second;
		}
	}
}

static vector<string> CollectParquetFiles(const HoltfsParquetScanBindData &bind) {
	auto index = OpenIndexForRead(bind.index_ref, bind.mode);
	vector<string> files;
	ScanIndexRecordsFrom(index.Tree(), bind.prefix, bind.start_after, [&](const string &key, const string &) {
		if (IsInternalKey(key) || !IsParquetDataFile(key)) {
			return;
		}
		files.push_back(key);
	});
	if (bind.max_files && files.size() > bind.max_files) {
		files.resize(bind.max_files);
	}
	return files;
}

static Value FileListValue(const vector<string> &files) {
	vector<Value> values;
	values.reserve(files.size());
	for (auto &file : files) {
		values.emplace_back(file);
	}
	return Value::LIST(LogicalType::VARCHAR, std::move(values));
}

static void AddNamedParameter(vector<unique_ptr<ParsedExpression>> &children, const string &name, const Value &value) {
	auto expression = make_uniq<ConstantExpression>(value);
	expression->SetAlias(name);
	children.push_back(std::move(expression));
}

static unique_ptr<TableRef> HoltParquetScanBindReplace(ClientContext &, TableFunctionBindInput &input) {
	if (input.inputs.size() != 1) {
		throw BinderException("holt_parquet_scan(index_ref, ...) expects exactly one VARCHAR argument");
	}

	HoltfsParquetScanBindData bind;
	bind.index_ref = StringValue::Get(input.inputs[0]);
	ReadParquetScanNamedParameters(input, bind);

	auto files = CollectParquetFiles(bind);
	if (files.empty()) {
		throw IOException("holt_parquet_scan found no Parquet files for prefix: " + bind.prefix);
	}

	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ConstantExpression>(FileListValue(files)));
	if (bind.filename.set) {
		AddNamedParameter(children, "filename", bind.filename.value);
	}
	if (bind.hive_partitioning.set) {
		AddNamedParameter(children, "hive_partitioning", bind.hive_partitioning.value);
	}
	if (bind.union_by_name.set) {
		AddNamedParameter(children, "union_by_name", bind.union_by_name.value);
	}

	auto table_function = make_uniq<TableFunctionRef>();
	table_function->function = make_uniq<FunctionExpression>("read_parquet", std::move(children));
	return std::move(table_function);
}

static void RegisterHoltParquetScan(ExtensionLoader &loader) {
	TableFunction function("holt_parquet_scan", {LogicalType::VARCHAR}, nullptr, nullptr);
	function.bind_replace = HoltParquetScanBindReplace;
	function.named_parameters["mode"] = LogicalType::VARCHAR;
	function.named_parameters["prefix"] = LogicalType::VARCHAR;
	function.named_parameters["start_after"] = LogicalType::VARCHAR;
	function.named_parameters["max_files"] = LogicalType::UBIGINT;
	function.named_parameters["filename"] = LogicalType::ANY;
	function.named_parameters["hive_partitioning"] = LogicalType::BOOLEAN;
	function.named_parameters["union_by_name"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(function);
}

struct HoltfsStatusBindData : public TableFunctionData {
	string index_ref;
	IndexMode mode = IndexMode::PERSISTENT;
	uint64_t max_age_seconds = 0;
};

struct HoltfsStatusGlobalState : public GlobalTableFunctionState {
	bool done = false;
};

static void ReadStatusNamedParameters(TableFunctionBindInput &input, HoltfsStatusBindData &result) {
	for (auto &kv : input.named_parameters) {
		if (kv.second.IsNull()) {
			throw BinderException("holtfs named parameters cannot be NULL");
		}
		if (kv.first == "mode") {
			result.mode = ParseMode(StringValue::Get(kv.second));
		} else if (kv.first == "max_age_seconds") {
			result.max_age_seconds = UBigIntValue::Get(kv.second);
		}
	}
}

static unique_ptr<FunctionData> HoltfsStatusBind(ClientContext &, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 1) {
		throw BinderException("holtfs_status(index_ref, ...) expects exactly one VARCHAR argument");
	}

	auto result = make_uniq<HoltfsStatusBindData>();
	result->index_ref = StringValue::Get(input.inputs[0]);
	ReadStatusNamedParameters(input, *result);

	names.emplace_back("index_ref");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("mode");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("has_manifest");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("source_path");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("source_exists");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("indexed_files");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("indexed_bytes");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("built_at_us");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("refreshed_at_us");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("age_seconds");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("is_stale");
	return_types.emplace_back(LogicalType::BOOLEAN);

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> HoltfsStatusInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<HoltfsStatusGlobalState>();
}

static bool SourceHasCheapExistenceCheck(const string &source_path) {
	return !FileSystem::HasGlob(source_path) && !FileSystem::IsRemoteFile(source_path);
}

static void HoltfsStatusFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind = input.bind_data->CastNoConst<HoltfsStatusBindData>();
	auto &state = input.global_state->Cast<HoltfsStatusGlobalState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	state.done = true;

	auto index = OpenIndexForRead(bind.index_ref, bind.mode);
	auto tree = index.Tree();
	auto manifest = ReadManifest(tree);
	auto &fs = FileSystem::GetFileSystem(context);
	auto source_exists =
	    manifest.exists && (!SourceHasCheapExistenceCheck(manifest.source_path) ||
	                        fs.FileExists(manifest.source_path) || fs.DirectoryExists(manifest.source_path));
	auto now = NowMicros();
	auto age_seconds = manifest.exists && manifest.refreshed_at_us <= now
	                       ? static_cast<double>(now - manifest.refreshed_at_us) / 1000000.0
	                       : 0.0;
	auto age_expired = bind.max_age_seconds > 0 && age_seconds > static_cast<double>(bind.max_age_seconds);
	auto is_stale = !manifest.exists || !source_exists || age_expired;

	output.data[0].SetValue(0, Value(bind.index_ref));
	output.data[1].SetValue(0, Value(ModeName(bind.mode)));
	output.data[2].SetValue(0, Value::BOOLEAN(manifest.exists));
	if (manifest.exists) {
		output.data[3].SetValue(0, Value(manifest.source_path));
		output.data[5].SetValue(0, Value::UBIGINT(manifest.indexed_files));
		output.data[6].SetValue(0, Value::UBIGINT(manifest.indexed_bytes));
		output.data[7].SetValue(0, Value::UBIGINT(manifest.built_at_us));
		output.data[8].SetValue(0, Value::UBIGINT(manifest.refreshed_at_us));
		output.data[9].SetValue(0, Value::DOUBLE(age_seconds));
	} else {
		output.data[3].SetValue(0, Value());
		output.data[5].SetValue(0, Value::UBIGINT(0));
		output.data[6].SetValue(0, Value::UBIGINT(0));
		output.data[7].SetValue(0, Value());
		output.data[8].SetValue(0, Value());
		output.data[9].SetValue(0, Value());
	}
	output.data[4].SetValue(0, Value::BOOLEAN(source_exists));
	output.data[10].SetValue(0, Value::BOOLEAN(is_stale));
	output.SetCardinality(1);
}

static void RegisterHoltfsStatus(ExtensionLoader &loader) {
	TableFunction function("holtfs_status", {LogicalType::VARCHAR}, HoltfsStatusFunction, HoltfsStatusBind,
	                       HoltfsStatusInit);
	function.named_parameters["mode"] = LogicalType::VARCHAR;
	function.named_parameters["max_age_seconds"] = LogicalType::UBIGINT;
	loader.RegisterFunction(function);
}

struct HoltfsRefreshBindData : public TableFunctionData {
	string source_path;
	string index_ref;
	IndexMode mode = IndexMode::PERSISTENT;
	string prefix;
};

struct HoltfsRefreshGlobalState : public GlobalTableFunctionState {
	bool done = false;
};

static void ReadRefreshNamedParameters(TableFunctionBindInput &input, HoltfsRefreshBindData &result) {
	for (auto &kv : input.named_parameters) {
		if (kv.second.IsNull()) {
			throw BinderException("holtfs named parameters cannot be NULL");
		}
		if (kv.first == "mode") {
			result.mode = ParseMode(StringValue::Get(kv.second));
		} else if (kv.first == "prefix") {
			result.prefix = StringValue::Get(kv.second);
		}
	}
}

static unique_ptr<FunctionData> HoltfsRefreshBind(ClientContext &, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 2) {
		throw BinderException("holtfs_refresh(source_path, index_ref, ...) expects exactly two VARCHAR arguments");
	}

	auto result = make_uniq<HoltfsRefreshBindData>();
	result->source_path = StringValue::Get(input.inputs[0]);
	result->index_ref = StringValue::Get(input.inputs[1]);
	ReadRefreshNamedParameters(input, *result);

	names.emplace_back("source_path");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("mode");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("index_ref");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("refresh_path");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("refreshed_files");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("refreshed_bytes");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("removed_keys");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("indexed_files");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("indexed_bytes");
	return_types.emplace_back(LogicalType::UBIGINT);

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> HoltfsRefreshInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<HoltfsRefreshGlobalState>();
}

static uint64_t DeleteStaleKeys(HoltTree *tree, const vector<string> &old_keys,
                                const std::unordered_set<string> &current_keys) {
	uint64_t removed = 0;
	for (auto &key : old_keys) {
		if (current_keys.find(key) != current_keys.end()) {
			continue;
		}
		uint8_t existed = 0;
		auto rc = holt_tree_delete(tree, reinterpret_cast<const uint8_t *>(key.data()), key.size(), &existed);
		if (rc != HOLT_OK) {
			throw IOException(HoltfsLastError("holt_tree_delete"));
		}
		removed += existed ? 1 : 0;
	}
	return removed;
}

static void CheckRefreshManifest(const IndexManifest &manifest, const string &source_path) {
	if (!manifest.exists) {
		throw IOException("holtfs index is missing its manifest; rebuild it with holtfs_index");
	}
	if (manifest.source_path != source_path) {
		throw IOException("holtfs source_path does not match the index manifest");
	}
}

static void RefreshPrefix(FileSystem &fs, HoltTree *tree, const HoltfsRefreshBindData &bind, const string &refresh_path,
                          uint64_t &refreshed_files, uint64_t &refreshed_bytes, uint64_t &removed_keys,
                          uint64_t &indexed_files, uint64_t &indexed_bytes) {
	auto manifest = ReadManifest(tree);
	CheckRefreshManifest(manifest, bind.source_path);

	if (!fs.FileExists(bind.source_path) && !fs.DirectoryExists(bind.source_path)) {
		throw IOException("holtfs_refresh source path is neither a file nor a directory: " + bind.source_path);
	}

	auto current = CollectSourceFiles(fs, refresh_path);
	auto key_prefix = fs.ConvertSeparators(refresh_path);
	auto old_keys = CollectIndexedKeys(tree, key_prefix);
	for (auto &file : current.files) {
		PutString(tree, file.key, file.value);
	}
	removed_keys = DeleteStaleKeys(tree, old_keys, current.keys);
	auto content = CountIndexContent(tree);
	auto now = NowMicros();
	WriteManifest(tree, bind.source_path, manifest.built_at_us, now, content.files, content.bytes);

	refreshed_files = current.files.size();
	refreshed_bytes = current.bytes;
	indexed_files = content.files;
	indexed_bytes = content.bytes;
}

static void HoltfsRefreshFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind = input.bind_data->CastNoConst<HoltfsRefreshBindData>();
	auto &state = input.global_state->Cast<HoltfsRefreshGlobalState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	state.done = true;

	auto &fs = FileSystem::GetFileSystem(context);
	if (!bind.prefix.empty() && FileSystem::HasGlob(bind.source_path)) {
		throw IOException("holtfs_refresh prefix refresh is not supported for glob source_path; rebuild the index");
	}
	auto refresh_path = ResolveSubtreePath(fs, bind.source_path, bind.prefix);
	uint64_t refreshed_files = 0;
	uint64_t refreshed_bytes = 0;
	uint64_t removed_keys = 0;
	uint64_t indexed_files = 0;
	uint64_t indexed_bytes = 0;

	if (bind.prefix.empty()) {
		HoltfsIndexBindData build;
		build.source_path = bind.source_path;
		build.mode = bind.mode;
		if (bind.mode == IndexMode::PERSISTENT) {
			build.index_path = bind.index_ref;
			BuildPersistentIndex(fs, build, indexed_files, indexed_bytes);
		} else {
			build.name = bind.index_ref;
			BuildMemoryIndex(fs, build, indexed_files, indexed_bytes);
		}
		refreshed_files = indexed_files;
		refreshed_bytes = indexed_bytes;
	} else if (bind.mode == IndexMode::PERSISTENT) {
		ValidateIndexLocation(bind.source_path, bind.index_ref);
		HoltTree *tree = nullptr;
		if (holt_tree_open_with_wal_commit(bind.index_ref.c_str(), HOLT_WAL_WRITE, &tree) != HOLT_OK) {
			throw IOException(HoltfsLastError("holt_tree_open_with_wal_commit"));
		}
		HoltTreeHandle handle(tree);
		RefreshPrefix(fs, handle.tree, bind, refresh_path, refreshed_files, refreshed_bytes, removed_keys,
		              indexed_files, indexed_bytes);
		if (holt_tree_checkpoint(handle.tree) != HOLT_OK) {
			throw IOException(HoltfsLastError("holt_tree_checkpoint"));
		}
	} else {
		auto handle = MemoryIndexes().Get(bind.index_ref);
		RefreshPrefix(fs, handle->tree, bind, refresh_path, refreshed_files, refreshed_bytes, removed_keys,
		              indexed_files, indexed_bytes);
	}

	output.data[0].SetValue(0, Value(bind.source_path));
	output.data[1].SetValue(0, Value(ModeName(bind.mode)));
	output.data[2].SetValue(0, Value(bind.index_ref));
	output.data[3].SetValue(0, Value(refresh_path));
	output.data[4].SetValue(0, Value::UBIGINT(refreshed_files));
	output.data[5].SetValue(0, Value::UBIGINT(refreshed_bytes));
	output.data[6].SetValue(0, Value::UBIGINT(removed_keys));
	output.data[7].SetValue(0, Value::UBIGINT(indexed_files));
	output.data[8].SetValue(0, Value::UBIGINT(indexed_bytes));
	output.SetCardinality(1);
}

static void RegisterHoltfsRefresh(ExtensionLoader &loader) {
	TableFunction function("holtfs_refresh", {LogicalType::VARCHAR, LogicalType::VARCHAR}, HoltfsRefreshFunction,
	                       HoltfsRefreshBind, HoltfsRefreshInit);
	function.named_parameters["mode"] = LogicalType::VARCHAR;
	function.named_parameters["prefix"] = LogicalType::VARCHAR;
	loader.RegisterFunction(function);

	TableFunction rebuild_function("holtfs_rebuild", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                               HoltfsRefreshFunction, HoltfsRefreshBind, HoltfsRefreshInit);
	rebuild_function.named_parameters["mode"] = LogicalType::VARCHAR;
	loader.RegisterFunction(rebuild_function);
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
	std::unordered_set<string> source_keys;
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
	stats.source_keys.insert(file.key);
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
	    ExpandSourceFiles(fs, source, [&](const string &path) { ValidateOneSourceFile(fs, tree, path, stats); });
}

static void CountIndexedFiles(FileSystem &fs, HoltTree *tree, const string &source, ValidationStats &stats) {
	auto glob_source = FileSystem::HasGlob(source);
	auto prefix = glob_source ? string() : fs.ConvertSeparators(source);
	ScanIndexRecords(tree, prefix, [&](const string &path, const string &) {
		if (!IsInternalKey(path) && (glob_source || IsUnderSourcePath(path, prefix))) {
			stats.indexed_files++;
			if (stats.source_keys.find(path) == stats.source_keys.end()) {
				stats.deleted_files++;
			}
		}
	});
}

static void HoltfsValidateFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind = input.bind_data->CastNoConst<HoltfsValidateBindData>();
	auto &state = input.global_state->Cast<HoltfsValidateGlobalState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	state.done = true;

	auto index = OpenIndexForRead(bind.index_ref, bind.mode);
	auto tree = index.Tree();
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
	RegisterHoltfsStatus(loader);
	RegisterHoltfsRefresh(loader);
	RegisterHoltFiles(loader);
	RegisterHoltParquetScan(loader);
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
