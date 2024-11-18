#pragma once

// Read data from a OSM pbf file. ReadFileStructure() must be called first. It
// reads the headers of all blobs in the file and then uses binary search to
// find the start of nodes, ways and relations in the file. The underlying
// assumption is that pbf files contain first all node blobs, then all way blobs
// and then all relations blobs. The reader will abort with high probability if
// this assumption does not hold.
//
// See src/bin/extract_admin.cpp for an example of how to use this. To get you
// the idea from the file above:
//   SomeStruct some_local_var;
//   OsmPbfReader reader(filename, n_threads);
//   reader.ReadFileStructure();
//   reader.ReadRelations([&some_local_var](const OSMTagHelper& tagh,
//                                          const OSMPBF::Relation& osm_rel,
//                                          std::mutex& mut) {
//    // Function doing some work.
//    ConsumeRelation(tagh, osm_rel, mut, &some_local_var);
//  });

#include <netinet/in.h>
#include <stdarg.h>
#include <stdio.h>
#include <zlib.h>

#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <regex>
#include <vector>

#include "absl/strings/str_format.h"
#include "base/thread_pool.h"
#include "logging/loguru.h"

namespace {
FILE* OpenFile(const std::string& filename) {
  FILE* fp = fopen(filename.c_str(), "rb");
  CHECK_S(fp != nullptr) << "can't open file <" << filename << ">";
  return fp;
}

void UnZip(const OSMPBF::Blob& blob, uint8_t outbuf[], int* rawsize) {
  z_stream zs;
  zs.next_in = (uint8_t*)blob.zlib_data().data();
  zs.avail_in = blob.zlib_data().size();
  zs.next_out = outbuf;
  zs.avail_out = blob.raw_size();
  zs.zalloc = Z_NULL;
  zs.zfree = Z_NULL;
  zs.opaque = Z_NULL;

  if (inflateInit(&zs) != Z_OK) {
    ABORT_S() << "inflateInit()";
  }
  if (inflate(&zs, Z_FINISH) != Z_STREAM_END) {
    ABORT_S() << "inflate()";
  }
  if (inflateEnd(&zs) != Z_OK) {
    ABORT_S() << "inflateEnd";
  }
  *rawsize = zs.total_out;
}
}  // namespace

class OsmPbfReader {
 public:
  enum BlobContentType {
    ContentNodes = 0,
    ContentWays,
    ContentRelations,
    ContentUnknown
  };
  struct Node {
    int64_t id;
    int64_t lat;
    int64_t lon;
  };
  // The mutex 'mut' passed to the functions below can be used to isolate one
  // thread from the wwork of the other threads.
  using RelationWorkerFunc = std::function<void(
      const OSMTagHelper& tagh, const OSMPBF::Relation& rel, std::mutex& mut)>;
  using WayWorkerFunc = std::function<void(
      const OSMTagHelper& tagh, const OSMPBF::Way& way, std::mutex& mut)>;
  // Currently, tags are not available for nodes.
  using NodeWorkerFunc = std::function<void(Node& node, std::mutex& mut)>;
  // Works on whole blobs. This is sometimes useful, for instance, when the
  // worker wants to use the sort order of the items in the blob.
  // Note that a blob contains only one type of items, i.e. nodes, ways or
  // relations.
  using BlobWorkerFunc = std::function<void(
      const OSMTagHelper& tagh, const OSMPBF::PrimitiveBlock& prim_block,
      std::mutex& mut)>;

  OsmPbfReader(std::string_view filename, int n_threads, bool verbose = false)
      : filename_(filename), n_threads_(n_threads), verbose_(verbose) {
    CHECK_GT_S(n_threads_, 0) << "need at least one thread";
    for (int i = 0; i < n_threads_; ++i) {
      file_handles_.push_back(OpenFile(filename_));
    }
  }

  // Read structure information (headers) from the pbf file and add a BlobMeta
  // for every OSMData blob in 'blob_meta'. Call before calling any of the other
  // Read* functions.
  // This might be a little bit slow on hard disks. It was tested only on SSDs.
  void ReadFileStructure() {
    FUNC_TIMER();
    CHECK_NE_S(file_handles_.at(0), nullptr);
    CHECK_S(blob_meta_.empty())
        << "ReadFileStructure() has already been called";
    char buffer[OSMPBF::max_blob_header_size];
    OSMPBF::BlobHeader blob_header;
    FILE* fp = file_handles_.at(0);

    while (!feof(fp)) {
      // Read length of the blob header.
      int32_t blob_size;
      if (fread(&blob_size, sizeof(blob_size), 1, fp) != 1) {
        break;  // End of file or error.
      }
      blob_size = ntohl(blob_size);
      CHECK_LE_S(blob_size, OSMPBF::max_blob_header_size)
          << "blob size too large";

      // Read blob header.
      CHECK_EQ_S(fread(buffer, blob_size, 1, fp), 1u)
          << "error reading blob header";
      CHECK_S(blob_header.ParseFromArray(buffer, blob_size))
          << "error parsing blob header";

      if (verbose_) {
        LOG_S(INFO) << absl::StrFormat(
            "Header (%d bytes): Blob type %s at %lld length %lld", blob_size,
            blob_header.type().c_str(), ftell(fp), blob_header.datasize());
      }

      // We're only interested in OSMData blobs.
      if (blob_header.type() == "OSMData") {
        int index = blob_meta_.size();
        blob_meta_.push_back({.index = index,
                              .file_pos = ftell(fp),
                              .block_length = blob_header.datasize()});
      }

      // Skip to the next blob header.
      fseek(fp, blob_header.datasize(), SEEK_CUR);
    }
    CHECK_S(feof(fp)) << " Error reading from <" << filename_ << ">";
    MarkContentTypes();
  }

  // Call 'worker_func' for each relation in the pbf file. Uses multiple
  // threads, so you might have to synchronize access to global data from your
  // worker_func.
  void ReadRelations(RelationWorkerFunc worker_func) {
    FUNC_TIMER();
    std::mutex mut;
    ThreadPool pool;
    for (BlobMeta& meta : blob_meta_) {
      if (meta.type == ContentRelations) {
        pool.AddWork([this, &worker_func, &meta, &mut](int thread_idx) {
          this->HandleRelationBlob(thread_idx, worker_func, &meta, mut);
        });
      }
    }
    pool.Start(n_threads_);
    pool.WaitAllFinished();
    CheckSortOrder(ContentRelations);
  }

  // Call 'worker_func' for each way in the pbf file. Uses multiple
  // threads, so you might have to synchronize access to global data from your
  // worker_func.
  void ReadWays(WayWorkerFunc worker_func) {
    FUNC_TIMER();
    std::mutex mut;
    ThreadPool pool;
    for (BlobMeta& meta : blob_meta_) {
      if (meta.type == ContentWays) {
        pool.AddWork([this, &worker_func, &meta, &mut](int thread_idx) {
          this->HandleWayBlob(thread_idx, worker_func, &meta, mut);
        });
      }
    }
    pool.Start(n_threads_);
    pool.WaitAllFinished();
    CheckSortOrder(ContentWays);
  }

  // Call 'worker_func' for each way in the pbf file. Uses multiple
  // threads, so you might have to synchronize access to global data from your
  // worker_func.
  void ReadNodes(NodeWorkerFunc worker_func) {
    FUNC_TIMER();
    std::mutex mut;
    ThreadPool pool;
    for (BlobMeta& meta : blob_meta_) {
      if (meta.type == ContentNodes) {
        pool.AddWork([this, &worker_func, &meta, &mut](int thread_idx) {
          this->HandleNodeBlob(thread_idx, worker_func, &meta, mut);
        });
      }
    }
    pool.Start(n_threads_);
    pool.WaitAllFinished();
    CheckSortOrder(ContentNodes);
  }

  // Call 'worker_func' for each blob of type 'type' in the pbf file.
  // Uses multiple threads, so you might have to synchronize access to global
  // data from your worker_func.
  void ReadBlobs(BlobContentType type, BlobWorkerFunc worker_func) {
    FuncTimer timer(
        absl::StrFormat("ReadBlobs(type=%s)", BlobContentTypeToStr(type)),
        __FILE__, __LINE__);
    std::mutex mut;
    ThreadPool pool;
    for (BlobMeta& meta : blob_meta_) {
      if (meta.type == type) {
        pool.AddWork([this, &worker_func, &meta, &mut](int thread_idx) {
          this->HandleBlob(thread_idx, worker_func, &meta, mut);
        });
      }
    }
    pool.Start(n_threads_);
    pool.WaitAllFinished();
    CheckSortOrder(ContentNodes);
  }

  int64_t CountEntries(BlobContentType type) const {
    int64_t count = 0;
    for (const BlobMeta& meta : blob_meta_) {
      if (meta.type == type) {
        count += meta.num_entries;
      }
    }
    return count;
  }

  static const char* BlobContentTypeToStr(enum BlobContentType t) {
    switch (t) {
      case ContentNodes:
        return "ContentNodes";
      case ContentWays:
        return "ContentWays";
      case ContentRelations:
        return "ContentRelations";
      case ContentUnknown:
      default:
        return "ContentUnknown";
    }
  };

 private:
  struct BlobMeta {
    BlobContentType type = ContentUnknown;
    // File information of data blob.
    std::int64_t index = 0;         // Position in vector blob_meta_.
    std::int64_t file_pos = 0;      // Starting position of blob.
    std::int64_t block_length = 0;  // Length of blob.

    // Stats - will be filled when the blob is read the first time.
    int64_t min_id = -1;
    int64_t max_id = -1;
    int64_t num_entries = -1;
    bool content_stats_done = false;
  };

  static void ComputeContentStats(const OSMPBF::PrimitiveBlock& prim_block,
                                  BlobMeta* meta) {
    CHECK_S(!meta->content_stats_done);
    meta->min_id = std::numeric_limits<int64_t>::max();
    meta->max_id = std::numeric_limits<int64_t>::min();
    meta->num_entries = 0;
    for (const OSMPBF::PrimitiveGroup& pg : prim_block.primitivegroup()) {
      CHECK_S(pg.nodes().empty()) << "Not implemented";
      if (pg.dense().id().size() > 0) {
        meta->num_entries += pg.dense().id().size();
        CHECK_S(meta->type == ContentNodes || meta->type == ContentUnknown)
            << meta->file_pos;
        meta->type = ContentNodes;
        std::int64_t running_id = 0;
        for (int i = 0; i < pg.dense().id().size(); ++i) {
          running_id += static_cast<std::int64_t>(pg.dense().id(i));
          meta->min_id = std::min(meta->min_id, running_id);
          meta->max_id = std::max(meta->max_id, running_id);
        }
      }
      if (pg.ways().size() > 0) {
        meta->num_entries += pg.ways().size();
        CHECK_S(meta->type == ContentWays || meta->type == ContentUnknown)
            << meta->file_pos;
        meta->type = ContentWays;
        for (const OSMPBF::Way& way : pg.ways()) {
          meta->min_id = std::min(meta->min_id, way.id());
          meta->max_id = std::max(meta->max_id, way.id());
        }
      }
      if (pg.relations().size() > 0) {
        meta->num_entries += pg.relations().size();
        CHECK_S(meta->type == ContentRelations || meta->type == ContentUnknown)
            << meta->file_pos;
        meta->type = ContentRelations;
        for (const OSMPBF::Relation& relation : pg.relations()) {
          meta->min_id = std::min(meta->min_id, relation.id());
          meta->max_id = std::max(meta->max_id, relation.id());
        }
      }
    }
    CHECK_GT_S(meta->num_entries, 0)
        << "PrimitiveGroup that doesn't contain anything useful at pos "
        << meta->file_pos;
  }

  // Check that the ids in blobs of content type from are sorted in
  // non-overlapping, ascending order.
  void CheckSortOrder(BlobContentType type) {
    if (verbose_) {
      LOG_S(INFO) << "Check id sort order for blobs of type " << type;
    }
    std::int64_t minv = -1;
    for (size_t i = content_start[type]; i < content_start[type + 1]; ++i) {
      const BlobMeta& meta = blob_meta_.at(i);
      CHECK_EQ_S(meta.type, type);
      CHECK_S(meta.num_entries > 0);
      CHECK_S(meta.min_id > minv && meta.max_id >= meta.min_id);
      minv = meta.max_id;
    }
  }

  static void ReadBlob(FILE* fp, BlobMeta* blob_meta,
                       OSMPBF::PrimitiveBlock* prim_block) {
    prim_block->Clear();
    if (fseek(fp, blob_meta->file_pos, SEEK_SET) != 0) {
      ABORT_S() << absl::StrFormat("fseek failed at pos %lld",
                                   blob_meta->file_pos);
    }
    char* buf = new char[blob_meta->block_length];
    if (fread(buf, blob_meta->block_length, 1, fp) != 1) {
      ABORT_S() << absl::StrFormat("unable to read data from file at pos %lld",
                                   blob_meta->file_pos);
    }
    OSMPBF::Blob blob;
    if (!blob.ParseFromArray(buf, blob_meta->block_length)) {
      ABORT_S() << "unable to parse blob";
    }
    delete[] buf;

    uint8_t* rawbuf = new uint8_t[OSMPBF::max_uncompressed_blob_size];
    int rawsize = 0;
    if (blob.has_zlib_data()) {
      UnZip(blob, rawbuf, &rawsize);
    } else {
      ABORT_S() << "blob without zlib data";
    }

    if (!prim_block->ParseFromArray(rawbuf, rawsize)) {
      ABORT_S() << "unable to parse primitive block";
    }
    delete[] rawbuf;
    if (!blob_meta->content_stats_done) {
      ComputeContentStats(*prim_block, blob_meta);
      blob_meta->content_stats_done = true;
    }
  }

  // Get the content type of the blob at position 'pos' in blob_meta_.
  // This is an expensive operation (may read from the file), therefore the
  // result is permanently stored in blob_meta_ and used on subsequent calls
  // that query the same position.
  BlobContentType GetBlobContentType(int64_t pos) {
    BlobMeta& meta = blob_meta_.at(pos);
    if (meta.type == ContentUnknown) {
      OSMPBF::PrimitiveBlock prim_block;
      // This also sets 'meta.type'.
      ReadBlob(file_handles_.at(0), &meta, &prim_block);
    }
    if (verbose_) {
      LOG_S(INFO) << "Content Type pos=" << pos << " is " << meta.type;
    }
    CHECK_NE_S(meta.type, ContentUnknown);
    return meta.type;
  }

  // Find the first blob at position >= start_idx that is not of type 'type'.
  // Note: The underlying assumption is that the blobs in the file are
  // strictly ordered by type - first nodes, then ways, then relations.
  int64_t FindContentStart(const BlobContentType type,
                           const int64_t start_idx) {
    CHECK_S(!blob_meta_.empty());
    // Invariant:
    //   * L-1 is equal type (or is -1).
    //   * R is another type (or is value 'size', i.e. beyond end).
    // When L = R then we found the start of not <type>.
    int64_t L = start_idx;
    int64_t R = blob_meta_.size();

    while (L < R) {
      int64_t mid = (L + R) / 2;
      if (GetBlobContentType(mid) == type) {
        L = mid + 1;
      } else
        R = mid;
    }
    if (verbose_) {
      LOG_S(INFO) << "First other type found at " << L;
    }
    CHECK_GT_S(L, start_idx) << "does not start with " << type;
    return L;
  }

  void MarkContentTypes() {
    content_start[ContentNodes] = 0;
    content_start[ContentWays] = FindContentStart(ContentNodes, 0);
    content_start[ContentRelations] =
        FindContentStart(ContentWays, content_start[ContentWays]);
    content_start[ContentUnknown] = blob_meta_.size();
    for (size_t i = 0; i < blob_meta_.size(); ++i) {
      if (i < content_start[ContentWays]) {
        blob_meta_.at(i).type = ContentNodes;
      } else if (i < content_start[ContentRelations]) {
        blob_meta_.at(i).type = ContentWays;
      } else {
        blob_meta_.at(i).type = ContentRelations;
      }
    }
    LOG_S(INFO) << absl::StrFormat("%5u Blobs in file %s", blob_meta_.size(),
                                   filename_);
    LOG_S(INFO) << absl::StrFormat("%5u..%5u node blobs",
                                   content_start[ContentNodes],
                                   content_start[ContentWays] - 1);
    LOG_S(INFO) << absl::StrFormat("%5u..%5u way blobs",
                                   content_start[ContentWays],
                                   content_start[ContentRelations] - 1);
    LOG_S(INFO) << absl::StrFormat("%5u..%5u relation blobs",
                                   content_start[ContentRelations],
                                   content_start[ContentUnknown] - 1);
  }

  void HandleRelationBlob(int thread_idx, RelationWorkerFunc worker_func,
                          BlobMeta* meta, std::mutex& mut) {
    CHECK_EQ_S(meta->type, ContentRelations);
    OSMPBF::PrimitiveBlock primblock;
    ReadBlob(file_handles_.at(thread_idx), meta, &primblock);
    OSMTagHelper tagh(primblock.stringtable());

    for (const OSMPBF::PrimitiveGroup& pg : primblock.primitivegroup()) {
      CHECK_S(pg.relations().size() > 0);
      if (verbose_) {
        LOG_S(INFO) << "Read PrimitiveGroup with " << pg.relations().size()
                    << " relations";
      }
      for (const OSMPBF::Relation& rel : pg.relations()) {
        worker_func(tagh, rel, mut);
      }
    }
  }

  void HandleWayBlob(int thread_idx, WayWorkerFunc worker_func, BlobMeta* meta,
                     std::mutex& mut) {
    CHECK_EQ_S(meta->type, ContentWays);
    OSMPBF::PrimitiveBlock primblock;
    ReadBlob(file_handles_.at(thread_idx), meta, &primblock);
    OSMTagHelper tagh(primblock.stringtable());

    for (const OSMPBF::PrimitiveGroup& pg : primblock.primitivegroup()) {
      CHECK_S(pg.ways().size() > 0);
      if (verbose_) {
        LOG_S(INFO) << "Read PrimitiveGroup with " << pg.ways().size()
                    << " ways";
      }
      for (const OSMPBF::Way& way : pg.ways()) {
        worker_func(tagh, way, mut);
      }
    }
  }

  void HandleNodeBlob(int thread_idx, NodeWorkerFunc worker_func,
                      BlobMeta* meta, std::mutex& mut) {
    CHECK_EQ_S(meta->type, ContentNodes);
    OSMPBF::PrimitiveBlock primblock;
    ReadBlob(file_handles_.at(thread_idx), meta, &primblock);

    for (const OSMPBF::PrimitiveGroup& pg : primblock.primitivegroup()) {
      CHECK_S(pg.dense().id_size() > 0);
      if (verbose_) {
        LOG_S(INFO) << "Read PrimitiveGroup with " << pg.dense().id_size()
                    << " nodes";
      }
      Node node = {.id = 0, .lat = 0, .lon = 0};
      for (int i = 0; i < pg.dense().id_size(); ++i) {
        node.id += pg.dense().id(i);
        node.lat += pg.dense().lat(i);
        node.lon += pg.dense().lon(i);
        worker_func(node, mut);
      }
    }
  }

  void HandleBlob(int thread_idx, BlobWorkerFunc worker_func, BlobMeta* meta,
                  std::mutex& mut) {
    OSMPBF::PrimitiveBlock primblock;
    ReadBlob(file_handles_.at(thread_idx), meta, &primblock);
    OSMTagHelper tagh(primblock.stringtable());
    worker_func(tagh, primblock, mut);
  }

  const std::string filename_;
  const int n_threads_;
  const bool verbose_;
  // Per thread file handle.
  std::vector<FILE*> file_handles_;
  std::vector<BlobMeta> blob_meta_;
  size_t content_start[ContentUnknown + 1];
};
