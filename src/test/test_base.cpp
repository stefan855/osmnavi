#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

#include <cmath>

#include "base/top_n.h"
#include "base/util.h"

void TestTopNGreater() {
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

int main(int argc, char* argv[]) {
  InitLogging(argc, argv);
  if (argc != 1) {
    ABORT_S() << absl::StrFormat("usage: %s", argv[0]);
  }

  TestTopNGreater();
  TestTopNSmaller();

  LOG_S(INFO)
      << "\n\033[1;32m*****************************\nTesting successfully "
         "finished\n*****************************\033[0m";
  return 0;
}
