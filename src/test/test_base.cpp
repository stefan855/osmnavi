#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

#include <cmath>

#include "base/deg_coord.h"
#include "base/top_n.h"
#include "base/util.h"

void TestTopNGreater() {
  FUNC_TIMER();
  TopN<int, 5, /*keep_greater*/true> topn;
  std::vector<int> items = {1, 6, 3, 9, 3, 7, 1, 5, 9, 34, 2, 8};
  for (int val : items) {
    topn.Add(val);
  }
  CHECK_EQ_S(topn.size(), 5);
  CHECK_S(!topn.empty());
  // Use .at() instead of [] once C++26 is fully done.
  CHECK_EQ_S(topn.span()[0], 34);
  CHECK_EQ_S(topn.span()[1], 9);
  CHECK_EQ_S(topn.span()[2], 9);
  CHECK_EQ_S(topn.span()[3], 8);
  CHECK_EQ_S(topn.span()[4], 7);
  CHECK_EQ_S(topn.top(), 34);
}

void TestTopNSmaller() {
  FUNC_TIMER();
  TopN<int, 5, /*keep_greater*/false> topn;
  std::vector<int> items = {1, 6, 3, 9, 3, 7, 1, 5, 9, 34, 2, 8};
  for (int val : items) {
    topn.Add(val);
  }
  CHECK_EQ_S(topn.size(), 5);
  CHECK_S(!topn.empty());
  // Use .at() instead of [] once C++26 is fully done.
  CHECK_EQ_S(topn.span()[0], 1);
  CHECK_EQ_S(topn.span()[1], 1);
  CHECK_EQ_S(topn.span()[2], 2);
  CHECK_EQ_S(topn.span()[3], 3);
  CHECK_EQ_S(topn.span()[4], 3);
  CHECK_EQ_S(topn.top(), 1);
}

void TestDegE6() {
  FUNC_TIMER();
  DegE6 a(static_cast<int>(5));
  DegE6 b(static_cast<int32_t>(5));
  DegE6 c(static_cast<int64_t>(5));
  DegE6 d(5.0f);
  DegE6 e(5.0);

  CHECK_EQ_S(a.v(), 5);;
  CHECK_EQ_S(b.v(), 5);
  CHECK_EQ_S(c.v(), 5);
  CHECK_EQ_S(d.v(), 5 * DegE6::MulFactor());
  CHECK_EQ_S(e.v(), 5 * DegE6::MulFactor());
  CHECK_EQ_S(d.AsFloat(), 5.0f);
  CHECK_EQ_S(e.AsDouble(), 5.0);
}

int main(int argc, char* argv[]) {
  InitLogging(argc, argv);
  if (argc != 1) {
    ABORT_S() << absl::StrFormat("usage: %s", argv[0]);
  }

  TestTopNGreater();
  TestTopNSmaller();
  TestDegE6();

  LOG_S(INFO)
      << "\n\033[1;32m*****************************\nTesting successfully "
         "finished\n*****************************\033[0m";
  return 0;
}
