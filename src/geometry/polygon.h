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

    int32_t lat, lon;
    absl::SimpleAtoi(row.at(2), &lat);
    absl::SimpleAtoi(row.at(3), &lon);
    if (poly.coords.empty()) {
      CHECK_S(row.at(0) == "poly-start") << filename << ":" << row.at(0);
    } else {
      CHECK_S(row.at(0) == "pt") << filename << ":" << row.at(0);
    }
    poly.coords.push_back({.x = lon, .y = lat});
  }
  return poly;
}
