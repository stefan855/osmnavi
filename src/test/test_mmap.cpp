#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <limits>
#include <string>

#include "base/mmap_base.h"
#include "base/util.h"
#include "graph/graph_def.h"
#include "graph/graph_def_utils.h"
#include "graph/mmgraph.h"

namespace {
int OpenTempFile() {
  // Lives a temporary life in ranmdisk /proc/self/fd/.
  int fd = memfd_create("test_mmap.cpp", MFD_CLOEXEC);  // In ramdisk
  if (fd < 0) FileAbortOnError("memfd_create");
  return fd;
}

void CloseTempFile(int fd) { ::close(fd); }

void* MMapTempFile(int fd) {
  void* ptr = mmap(NULL, GetFileSize(fd), PROT_READ, MAP_PRIVATE, fd, 0);
  if (ptr == MAP_FAILED) {
    perror("mmap");
    CloseTempFile(fd);
    ABORT_S();
  }
  return ptr;
}
}  // namespace

void TestMMCompressedUIntVec() {
  FUNC_TIMER();
  struct MM {
    MMCompressedUIntVec cv;
  };
  std::vector<uint32_t> inp;
  for (size_t i = 0; i < 1000; ++i) {
    inp.push_back(i % 128);
  }
  int fd = OpenTempFile();

  // Build file.
  {
    MM mm;
    AppendData("mm-struct", fd, (const uint8_t*)&mm, sizeof(mm));
    mm.cv.WriteDataBlob("compressed vector", fd, inp);
    CHECK_EQ_S(inp.size(), mm.cv.size());
    CHECK_EQ_S(7, mm.cv.bit_width());
    WriteDataTo(fd, 0, (const uint8_t*)&mm, sizeof(mm));
  }

  // MMap and compare.
  {
    const uint8_t* base_ptr = (const uint8_t*)MMapTempFile(fd);
    const MM* mm = (MM*)base_ptr;
    CHECK_EQ_S(inp.size(), mm->cv.size());
    for (size_t i = 0; i < inp.size(); ++i) {
      CHECK_EQ_S(inp.at(i), mm->cv.at(base_ptr, i));
    }
    munmap((void*)base_ptr, GetFileSize(fd));
  }

  CloseTempFile(fd);
}

void TestMMTurnCostsTable() {
  FUNC_TIMER();
  struct MM {
    MMTurnCostsTable tct;
  };
  std::vector<TurnCostData> tcdata(4);
  tcdata.at(0).turn_costs = {1, 0};
  tcdata.at(1).turn_costs = {255};
  tcdata.at(2).turn_costs = {200, 100, 59, 25, 12};
  tcdata.at(3).turn_costs = {1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
                             1, 2, 3, 4, 5, 6, 7, 8, 9, 0};

  int fd = OpenTempFile();

  // Build file.
  std::vector<uint32_t> idx_to_pos;
  {
    MM mm;
    AppendData("mm-struct", fd, (const uint8_t*)&mm, sizeof(mm));
    idx_to_pos = mm.tct.WriteDataBlob("turn_costs table", fd, tcdata);
    CHECK_EQ_S(idx_to_pos.size(), tcdata.size());
    WriteDataTo(fd, 0, (const uint8_t*)&mm, sizeof(mm));
  }

  // MMap and compare.
  {
    const uint8_t* base_ptr = (const uint8_t*)MMapTempFile(fd);
    const MM* mm = (MM*)base_ptr;
    for (size_t i = 0; i < tcdata.size(); ++i) {
      std::span<const uint8_t> v = mm->tct.at(base_ptr, idx_to_pos.at(i));
      CHECK_EQ_S(v.size(), tcdata.at(i).turn_costs.size());
      for (size_t p = 0; p < v.size(); ++p) {
        CHECK_EQ_S(v[p], tcdata.at(i).turn_costs.at(p));
      }
    }
    munmap((void*)base_ptr, GetFileSize(fd));
  }

  CloseTempFile(fd);
}

void TestMMStringsTable() {
  FUNC_TIMER();
  struct MM {
    MMStringsTable st;
  };
  const std::vector<std::string> strings = {"123", "", "012345"};
  int fd = OpenTempFile();

  // Build file.
  std::vector<uint32_t> idx_to_pos;
  {
    MM mm;
    AppendData("mm-struct", fd, (const uint8_t*)&mm, sizeof(mm));
    idx_to_pos = mm.st.WriteDataBlob("strings table", fd, strings);
    CHECK_EQ_S(idx_to_pos.size(), strings.size());
    WriteDataTo(fd, 0, (const uint8_t*)&mm, sizeof(mm));
  }

  // MMap and compare.
  {
    const uint8_t* base_ptr = (const uint8_t*)MMapTempFile(fd);
    const MM* mm = (MM*)base_ptr;
    for (size_t i = 0; i < strings.size(); ++i) {
      CHECK_EQ_S(strings.at(i), mm->st.at(base_ptr, idx_to_pos.at(i)));
    }
    munmap((void*)base_ptr, GetFileSize(fd));
  }

  CloseTempFile(fd);
}

void TestMMGroupedOSMIds() {
  FUNC_TIMER();
  struct MM {
    MMGroupedOSMIds gi;
  };
  std::vector<int64_t> ids;
  // A few special cases with large deltas.
  ids.push_back(0);
  ids.push_back(std::numeric_limits<int64_t>::max());
  ids.push_back(std::numeric_limits<int64_t>::min());
  ids.push_back(std::numeric_limits<int64_t>::max());
  ids.push_back(0);
  ids.push_back(std::numeric_limits<int64_t>::min());
  ids.push_back(0);
  uint64_t rand_state = 123456789999;
  for (size_t i = 0; i < 1000; ++i) {
    rand_state = PseudoRandom64(rand_state);
    ids.push_back(((int64_t)rand_state));
  }

  int fd = OpenTempFile();

  // Build file.
  {
    MM mm;
    AppendData("mm-struct", fd, (const uint8_t*)&mm, sizeof(mm));
    mm.gi.WriteDataBlob("grouped osm ids", fd, ids);
    CHECK_EQ_S(ids.size(), mm.gi.size());
    WriteDataTo(fd, 0, (const uint8_t*)&mm, sizeof(mm));
  }

  // MMap and compare.
  {
    const uint8_t* base_ptr = (const uint8_t*)MMapTempFile(fd);
    const MM* mm = (MM*)base_ptr;
    CHECK_EQ_S(ids.size(), mm->gi.size());
    for (size_t i = 0; i < ids.size(); ++i) {
      CHECK_EQ_S(ids.at(i), mm->gi.at(base_ptr, i));
    }
    munmap((void*)base_ptr, GetFileSize(fd));
  }

  CloseTempFile(fd);
}

void TestMMNodeBits() {
  FUNC_TIMER();

  uint64_t u = 0;
  MM_NODE_RW(u).set_edge_start_pos(123);
  CHECK_EQ_S(MM_NODE(u).edge_start_pos(), 123);

  MMNodeBits x{0};
  CHECK_S(!x.border_node());
  CHECK_S(!x.dead_end());
  CHECK_S(!x.off_cluster_node());
  CHECK_EQ_S(x.edge_start_pos(), 0);

  x.set_border_node(true);
  CHECK_S(x.border_node());
  CHECK_S(!x.dead_end());
  CHECK_S(!x.off_cluster_node());
  CHECK_EQ_S(x.edge_start_pos(), 0);

  x.set_dead_end(true);
  CHECK_S(x.border_node());
  CHECK_S(x.dead_end());
  CHECK_S(!x.off_cluster_node());
  CHECK_EQ_S(x.edge_start_pos(), 0);

  x.set_off_cluster_node(true);
  CHECK_S(x.border_node());
  CHECK_S(x.dead_end());
  CHECK_S(x.off_cluster_node());
  CHECK_EQ_S(x.edge_start_pos(), 0);

  x.set_edge_start_pos(1234567);
  CHECK_S(x.border_node());
  CHECK_S(x.dead_end());
  CHECK_S(x.off_cluster_node());
  CHECK_EQ_S(x.edge_start_pos(), 1234567);
}

void TestMMEdgeBits() {
  FUNC_TIMER();

  uint64_t u = 0;
  MM_EDGE_RW(u).set_target_idx(123);
  CHECK_EQ_S(MM_EDGE(u).target_idx(), 123);

  MMEdgeBits x{0};

  CHECK_S(!x.dead_end());
  CHECK_S(!x.bridge());
  CHECK_S(!x.restricted());
  CHECK_S(!x.contra_way());
  CHECK_S(!x.cluster_border_edge());
  CHECK_S(!x.complex_turn_restriction_trigger());
  CHECK_EQ_S(x.target_idx(), 0);

  x.set_dead_end(true);
  CHECK_S(x.dead_end());
  CHECK_S(!x.bridge());
  CHECK_S(!x.restricted());
  CHECK_S(!x.contra_way());
  CHECK_S(!x.cluster_border_edge());
  CHECK_S(!x.complex_turn_restriction_trigger());
  CHECK_EQ_S(x.target_idx(), 0);

  x.set_bridge(true);
  CHECK_S(x.dead_end());
  CHECK_S(x.bridge());
  CHECK_S(!x.restricted());
  CHECK_S(!x.contra_way());
  CHECK_S(!x.cluster_border_edge());
  CHECK_S(!x.complex_turn_restriction_trigger());
  CHECK_EQ_S(x.target_idx(), 0);

  x.set_restricted(true);
  CHECK_S(x.dead_end());
  CHECK_S(x.bridge());
  CHECK_S(x.restricted());
  CHECK_S(!x.contra_way());
  CHECK_S(!x.cluster_border_edge());
  CHECK_S(!x.complex_turn_restriction_trigger());
  CHECK_EQ_S(x.target_idx(), 0);

  x.set_contra_way(true);
  CHECK_S(x.dead_end());
  CHECK_S(x.bridge());
  CHECK_S(x.restricted());
  CHECK_S(x.contra_way());
  CHECK_S(!x.cluster_border_edge());
  CHECK_S(!x.complex_turn_restriction_trigger());
  CHECK_EQ_S(x.target_idx(), 0);

  x.set_cluster_border_edge(true);
  CHECK_S(x.dead_end());
  CHECK_S(x.bridge());
  CHECK_S(x.restricted());
  CHECK_S(x.contra_way());
  CHECK_S(x.cluster_border_edge());
  CHECK_S(!x.complex_turn_restriction_trigger());
  CHECK_EQ_S(x.target_idx(), 0);

  x.set_complex_turn_restriction_trigger(true);
  CHECK_S(x.dead_end());
  CHECK_S(x.bridge());
  CHECK_S(x.restricted());
  CHECK_S(x.contra_way());
  CHECK_S(x.cluster_border_edge());
  CHECK_S(x.complex_turn_restriction_trigger());
  CHECK_EQ_S(x.target_idx(), 0);

  x.set_target_idx(12345678);
  CHECK_S(x.dead_end());
  CHECK_S(x.bridge());
  CHECK_S(x.restricted());
  CHECK_S(x.contra_way());
  CHECK_S(x.cluster_border_edge());
  CHECK_S(x.complex_turn_restriction_trigger());
  CHECK_EQ_S(x.target_idx(), 12345678);
}

int main(int argc, char* argv[]) {
  InitLogging(argc, argv);
  if (argc != 1) {
    ABORT_S() << absl::StrFormat("usage: %s", argv[0]);
  }

  /*
  TestMMap();
  TestMMCompressedUIntVec();
  */


  TestMMCompressedUIntVec();
  TestMMTurnCostsTable();
  TestMMStringsTable();
  TestMMGroupedOSMIds();

  TestMMNodeBits();
  TestMMEdgeBits();

  LOG_S(INFO)
      << "\n\033[1;32m*****************************\nTesting successfully "
         "finished\n*****************************\033[0m";
  return 0;
}
