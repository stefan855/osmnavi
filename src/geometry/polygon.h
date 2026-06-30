#pragma once
#include <stdio.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string_view>

#include "absl/strings/numbers.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include "logging/loguru.h"

struct Polygon {
  struct Point {
    // TODO DegE6
    int32_t x;
    int32_t y;
  };
  std::vector<Point> coords;
};

inline Polygon LoadPolygon(const std::string& filename) {
  Polygon poly;
  std::ifstream file(filename);
  if (!file.is_open()) {
    perror(filename.c_str());
    exit(EXIT_FAILURE);
  }

  for (std::string line; std::getline(file, line);) {
    std::vector<std::string_view> row = absl::StrSplit(line, ',');
    CHECK_EQ_S(row.size(), 4u);

    DegE6 lat, lon;
    if (row.at(2).contains('.') && row.at(3).contains('.')) {
      double num;
      CHECK_S(absl::SimpleAtod(row.at(2), &num)) << line;
      lat = DegE6(num);
      CHECK_S(absl::SimpleAtod(row.at(3), &num)) << line;
      lon = DegE6(num);
    } else {
      int32_t num;
      CHECK_S(absl::SimpleAtoi(row.at(2), &num)) << row.at(2);
      lat = DegE6(num);
      CHECK_S(absl::SimpleAtoi(row.at(3), &num)) << row.at(3);
      lon = DegE6(num);
    }

    if (poly.coords.empty()) {
      CHECK_S(row.at(0) == "poly-start") << filename << ":" << row.at(0);
    } else {
      CHECK_S(row.at(0) == "pt") << filename << ":" << row.at(0);
    }
    poly.coords.push_back({.x = lon.v(), .y = lat.v()});
  }
  return poly;
}
