#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <string>

#include "base/mmap_base.h"
#include "base/util.h"
#include "graph/graph_def.h"
#include "graph/graph_def_utils.h"
#include "graph/rgraph.h"

#if 0
void AddGroupedOSMIds(int fd, const std::vector<uint64_t>& ids,
                      MMGroupedOSMIds* mmgids) {
  mmgids->num_ids = ids.size();

  // Create the groups vector and save the delta encrypted ids in 'buff'.
  uint32_t num_groups =
      (mmgids->num_ids + kOSMIdsGroupSize - 1) / kOSMIdsGroupSize;
  std::vector<MMGroupedOSMIds::IdGroup> groups(num_groups);
  WriteBuff buff;
  for (uint32_t gidx = 0; gidx < groups.size(); ++gidx) {
    MMGroupedOSMIds::IdGroup& g = groups.at(gidx);
    uint32_t first_pos = gidx * kOSMIdsGroupSize;
    uint64_t prev_id = ids.at(first_pos);
    CHECK_LT_S(prev_id, 1ull << kOSMIdsIdBits);
    CHECK_LT_S(buff.used(), 1ull << kOSMIdsBlobOffsetBits);
    g.first_id = prev_id;
    g.blob_offset = buff.used();
    for (uint32_t pos = first_pos + 1;
         pos < std::min(first_pos + kOSMIdsGroupSize, ids.size()); ++pos) {
      // Push delta.
      uint64_t id = ids.at(pos);
      PositiveDeltaEncodeUInt64(prev_id, id, &buff);
      prev_id = id;
    }
  }

  // Write groups vector.
  // AppendVectorData("id-groups", fd, groups, &(mmgids->mmgroups));
  mmgids->mmgroups.InitDataBlob("id-groups", fd, groups);
  // Write blob containing the deltas
  CHECK_EQ_S(mmgids->id_blob_start(), GetFileSize(fd));
  AppendData("id-groups:blob", fd, buff.base_ptr(), buff.used());
}
#endif

constexpr uint64_t kNumOSMIds = 100;
void AddClusters(int fd, const MMFileHeader& mmheader,
                 std::vector<MMCluster>* clusters) {
  for (uint32_t idx = 0; idx < mmheader.clusters.num; ++idx) {
    LOG_S(INFO) << "Add cluster " << idx;
    MMCluster* cluster = &clusters->at(idx);
    {
      std::vector<MMNode> nodes;
      nodes.push_back(
          {.edge_start_pos = idx + 0, .border_node = 0, .dead_end = 0});
      nodes.push_back(
          {.edge_start_pos = idx + 1, .border_node = 0, .dead_end = 1});
      // AppendVectorData("nodes", fd, nodes, &cluster->nodes);
      cluster->nodes.InitDataBlob("nodes", fd, nodes);
    }

    {
      std::vector<uint64_t> ids(kNumOSMIds, 0);
      for (size_t i = 0; i < kNumOSMIds; ++i) {
        ids.at(i) = i * 5;
      }
      cluster->grouped_osm_node_ids.InitGroupDataBlob("node-id-groups", fd,
                                                      ids);
    }

    std::vector<uint64_t> way_idx_to_streetname_idx;
    for (size_t i = 0; i < 1000; ++i) {
      way_idx_to_streetname_idx.push_back(i % 32);
    }
    cluster->way_idx_to_streetname_idx.InitDataBlob(
        "way_idx_to_streetname_idx", fd, way_idx_to_streetname_idx);
    CHECK_EQ_S(cluster->way_idx_to_streetname_idx.bit_width__, 5);
  }
}

constexpr uint64_t kMagic = 7715514337782280064ull;
constexpr uint64_t kVersion = 1;

void WriteMMFile(const std::string& path) {
  int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) AbortOnError("open");

  MMFileHeader mmheader;
  mmheader.magic = kMagic;
  mmheader.version = kVersion;
  mmheader.file_size = 0;
  mmheader.cluster_bounding_rects = {.num = 0, .offset = 0};
  mmheader.clusters = {.num = 0, .offset = 0};

  AppendData("file-header", fd, (const uint8_t*)&mmheader, sizeof(mmheader));

  uint32_t num_clusters = 2;

  // Write the zeroed bounding_rects vector to the file. Will write it again in
  // the end.
  std::vector<MMBoundingRect> bounding_rects(num_clusters, {0});
  // AppendVectorData("bounding rects", fd, bounding_rects,
  //                  &mmheader.cluster_bounding_rects);
  mmheader.cluster_bounding_rects.InitDataBlob("bounding rects", fd,
                                               bounding_rects);

  // Write the zeroed clusters vector to the file. Will write it again in
  // the end.
  std::vector<MMCluster> clusters(num_clusters, {0});
  clusters.at(0).id = 0;
  clusters.at(1).id = 1;
  // AppendVectorData("clusters", fd, clusters, &mmheader.clusters);
  mmheader.clusters.InitDataBlob("clusters", fd, clusters);

  AddClusters(fd, mmheader, &clusters);

  // Rewrite the header data and the two vectors belonging to the header.
  mmheader.file_size = GetFileSize(fd);
  WriteDataTo(fd, 0, (const uint8_t*)&mmheader, sizeof(mmheader));
  WriteDataTo(fd, mmheader.cluster_bounding_rects.offset,
              (const uint8_t*)bounding_rects.data(),
              VectorDataSizeInBytes(bounding_rects));
  WriteDataTo(fd, mmheader.clusters.offset, (const uint8_t*)clusters.data(),
              VectorDataSizeInBytes(clusters));
  CHECK_EQ_S(mmheader.file_size, GetFileSize(fd));

  if (::fsync(fd) != 0) AbortOnError("fsync");
  if (::close(fd) != 0) AbortOnError("close");
}

void VerifyMMFile(const std::string& path) {
  LOG_S(INFO) << "verify contents of mmap file " << path;
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) AbortOnError("open");
  struct stat sb;
  if (fstat(fd, &sb) == -1) AbortOnError("fstat");
  void* mmap_ptr = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
  ::close(fd);
  fd = -1;

  const uint8_t* base_ptr = (uint8_t*)mmap_ptr;

  // Here we go, through 'h' we should be able to access all data.
  MMFileHeader* h = (MMFileHeader*)base_ptr;
  LOG_S(INFO) << "Header file magic:" << h->magic;
  CHECK_EQ_S(kMagic, h->magic);
  CHECK_EQ_S(kVersion, h->version);
  CHECK_EQ_S(sb.st_size, h->file_size);
  CHECK_EQ_S(h->cluster_bounding_rects.num, 2);
  CHECK_EQ_S(h->clusters.num, 2);

  for (const auto& rect : h->cluster_bounding_rects.span(base_ptr)) {
    CHECK_EQ_S(rect.lat0, 0);
    CHECK_EQ_S(rect.lon0, 0);
    CHECK_EQ_S(rect.lat1, 0);
    CHECK_EQ_S(rect.lon1, 0);
  }

  for (size_t i = 0; i < h->clusters.num; ++i) {
    const MMCluster& c = h->clusters.at(base_ptr, i);
    CHECK_EQ_S(c.id, i);
    CHECK_EQ_S(c.nodes.num, 2);
    CHECK_EQ_S(c.nodes.at(base_ptr, 0).edge_start_pos, i + 0);
    CHECK_EQ_S(c.nodes.at(base_ptr, 0).border_node, 0);
    CHECK_EQ_S(c.nodes.at(base_ptr, 0).dead_end, 0);
    CHECK_EQ_S(c.nodes.at(base_ptr, 1).edge_start_pos, i + 1);
    CHECK_EQ_S(c.nodes.at(base_ptr, 1).border_node, 0);
    CHECK_EQ_S(c.nodes.at(base_ptr, 1).dead_end, 1);

    for (uint32_t i = 0; i < kNumOSMIds; ++i) {
      // LOG_S(INFO) << "node id " << i << " is "
      //             << c.grouped_osm_node_ids.get_id(base_ptr, i);
      CHECK_EQ_S(c.grouped_osm_node_ids.get_id(base_ptr, i), 5 * i);
    }

    for (size_t i = 0; i < c.way_idx_to_streetname_idx.num__; ++i) {
      CHECK_EQ_S(c.way_idx_to_streetname_idx.at(base_ptr, i), i % 32) << i;
    }
  }

  munmap(mmap_ptr, sb.st_size);
}

void TestMMap() {
  const char path[] = "/tmp/osmnavi_mmap_test.raw";
  WriteMMFile(path);
  VerifyMMFile(path);
}

void TestMMCompressedUIntVec() {
  FUNC_TIMER();

#if 0
  for (uint8_t bit_width = 1; bit_width <= 64; ++bit_width) {
    LOG_S(INFO) << "bit_width=" << (int)bit_width;
    constexpr size_t kDataSize = 10000;
    uint64_t data[kDataSize] = {};
    MMCompressedUIntVec mmv;
    mmv.num = kDataSize;
    mmv.bit_width = bit_width;
    mmv.offset = 0;
    CHECK_LE_S(mmv.NumDataBytes(), kDataSize);

    for (size_t i = 0; i < mmv.num; i++) {
      uint64_t val = (mmv.bit_width == 64 ? i : i % (1ull << mmv.bit_width));
      mmv.Set((uint8_t*)data, i, val);
      CHECK_EQ_S(mmv.at((uint8_t*)data, i), val) << i;
      /*
      for (uint64_t val = 0; val < 128; ++val) {
        mmv.Set((uint8_t*)data, i, val);
        LOG_S(INFO) << "i:     " << i;
        LOG_S(INFO) << "val:   " << val;
        LOG_S(INFO) << "arr[0]:" << std::bitset<64>(data[0]);
        LOG_S(INFO) << "arr[1]:" << std::bitset<64>(data[1]);
        CHECK_EQ_S(mmv.at((uint8_t*)data, i), val) << i;
      }
      mmv.Set((uint8_t*)data, i, 0);
      */
    }
    for (size_t i = 0; i < mmv.num; i++) {
      uint64_t val = (mmv.bit_width == 64 ? i : i % (1ull << mmv.bit_width));
      CHECK_EQ_S(mmv.at((uint8_t*)data, i), val) << i;
    }
  }
#endif
}

int main(int argc, char* argv[]) {
  InitLogging(argc, argv);
  if (argc != 1) {
    ABORT_S() << absl::StrFormat("usage: %s", argv[0]);
  }

  TestMMap();
  TestMMCompressedUIntVec();

  LOG_S(INFO)
      << "\n\033[1;32m*****************************\nTesting successfully "
         "finished\n*****************************\033[0m";
  return 0;
}
