#pragma once
#include <stdio.h>

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <string_view>
#include <unordered_map>

#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include "base/country_code.h"
#include "base/util.h"
#include "geometry/polygon.h"
#include "logging/loguru.h"

// Given a set of polygons with associated countries, find the country that
// contains a point. Note, this works with enclaves/exclaves.
// Follows the algorithm in https://en.wikipedia.org/wiki/Point_in_polygon.
// Adds optimizations to speed the algorithm up, mainly by bucketing line
// segments by height and sorting them by (y0,x0,y1,x1) within each bucket.
class FastCountryPolygons {
 public:
  struct Line {
    int32_t x0;
    int32_t y0;
    int32_t x1;
    int32_t y1;
    // Up to two contries that "own" this line as border.
    uint16_t country_num_1;
    uint16_t country_num_2;
  };

  FastCountryPolygons()
      : max_height_({100, 1000, 10000, 100000, 1000000,
                     std::numeric_limits<uint32_t>::max()}),
        limited_lines_(max_height_.size()) {}

  void LoadPolygonsFromWildcard(std::string_view wildcard) {
    FuncTimer timer(
        absl::StrFormat("Load multiple polygon files from %s", wildcard));
    for (const std::filesystem::path path : GetFilesWithWildcard(wildcard)) {
      std::vector<std::string> v =
          absl::StrSplit(path.filename().string(), '_');
      CHECK_EQ_S(v.at(0).size(), 2u) << path;
      AddPolygon(LoadPolygon(path), TwoLetterCountryCodeToNum(v.at(0)));
    }
    for (size_t i = 0; i < limited_lines_.size(); ++i) {
      LOG_S(INFO) << absl::StrFormat("FastCountryPolygon maxheight:%u lines:%d",
                                     max_height_.at(i),
                                     limited_lines_.at(i).size());
    }
  }

  void AddPolygon(const Polygon& p, uint16_t country_num) {
    for (size_t i = 0; i < p.coords.size() - 1; ++i) {
      const Polygon::Point& p1 = p.coords.at(i);
      const Polygon::Point& p2 = p.coords.at(i + 1);
      AddLine(p1.x, p1.y, p2.x, p2.y, country_num);
    }
  }

  void AddLine(int32_t x0, int32_t y0, int32_t x1, int32_t y1,
               uint16_t country_num) {
    CHECK_GT_S(country_num, 0);
    // horizontal lines never count as intersections with a horizontal ray.
    if (y0 == y1) return;

    if (y0 > y1) {
      std::swap(x0, x1);
      std::swap(y0, y1);
    }
    // POSTCOND(y0 < y1);

    int64_t height = y1 - y0;
    CHECK_GT_S(height, 0);
    CHECK_LE_S(height, std::numeric_limits<uint32_t>::max());
    size_t pos = 0;
    while (height > max_height_.at(pos)) {
      pos++;
    }
    CHECK_LT_S(pos, max_height_.size());
    limited_lines_.at(pos).push_back({.x0 = x0,
                                      .y0 = y0,
                                      .x1 = x1,
                                      .y1 = y1,
                                      .country_num_1 = country_num,
                                      .country_num_2 = 0});
  }

  void PrepareData() {
    FuncTimer timer("FastCountryPolygons::PrepareData()");
    for (size_t i = 0; i < limited_lines_.size(); ++i) {
      std::vector<Line>& lines = limited_lines_.at(i);
      // Sort lines by increasing (y0,x0,y1,x1).
      std::sort(lines.begin(), lines.end(), [](const Line& a, const Line& b) {
        if (a.y0 < b.y0) {
          return true;
        } else if (a.y0 == b.y0) {
          if (a.x0 < b.x0) {
            return true;
          } else if (a.x0 == b.x0) {
            if (a.y1 < b.y1) {
              return true;
            } else if (a.y1 == b.y1) {
              return a.x1 < b.x1;
            }
          }
        }
        return false;
      });
      MergeDupLines(&lines);
      SetRealMaxHeight(i);
    }
  }

  void CountRayIntersections(
      const int32_t p_x, const int32_t p_y,
      std::unordered_map<uint16_t, int32_t>* counts_per_country) const {
    for (size_t pos = 0; pos < max_height_.size(); ++pos) {
      const std::vector<Line>& lines = limited_lines_.at(pos);
      //      auto it = lines.begin();
      //      if (pos < max_height_.size()) {
      auto it = std::lower_bound(
          lines.begin(), lines.end(), p_y - max_height_.at(pos),
          [](const Line& line, int32_t value) { return line.y0 < value; });
      //      }
      for (; it != lines.end(); ++it) {
        const Line& line = *it;
        // The list is sorted by increasing y0, so we can stop here.
        // We already stop when line.y0 == p_y: This line is never
        // counted because the other end y1 is larger.
        if (line.y0 >= p_y) break;
        if (Intersects(p_x, p_y, line)) {
          (*counts_per_country)[line.country_num_1] += 1;
          if (line.country_num_2 != 0) {
            (*counts_per_country)[line.country_num_2] += 1;
          }
        }
      }
    }
  }

  uint16_t GetCountryNum(const int32_t p_x, const int32_t p_y,
                         const int64_t debug_id = 0) const {
    std::unordered_map<uint16_t, int32_t> counts_per_country;
    CountRayIntersections(p_x, p_y, &counts_per_country);
    int num_odds = 0;
    uint16_t found_country_num = 0;
    for (auto [country_num, count] : counts_per_country) {
      if (count & 1) {
        num_odds++;
        if (country_num > found_country_num) {
          found_country_num = country_num;
        }
      }
    }
    return found_country_num;
  }

  const std::vector<std::vector<Line>>& get_line_vectors() const {
    return limited_lines_;
  }

 private:
  static void MergeDupLines(std::vector<Line>* lines) {
    // Read all elements from read_pos 0..size-1.
    // If the element at read_pos can be merged with the element at
    // write_pos-1, then do so and increment only read_pos.
    // If not, then write the element at read_pos to the element at write_pos
    // and increment both read_pos and write_pos.
    if (lines->size() < 2) return;
    size_t write_pos;
    size_t read_pos;
    for (read_pos = 1, write_pos = 1; read_pos < lines->size(); ++read_pos) {
      const Line& rp = lines->at(read_pos);
      Line& prev_wp = lines->at(write_pos - 1);
      if (prev_wp.country_num_2 == 0 && rp.x1 == prev_wp.x1 &&
          rp.y1 == prev_wp.y1 && rp.x0 == prev_wp.x0 && rp.y0 == prev_wp.y0) {
        // Merge with element before write_pos.
        prev_wp.country_num_2 = rp.country_num_1;
      } else {
        lines->at(write_pos) = rp;
        ++write_pos;
      }
    }
    LOG_S(INFO) << absl::StrFormat("MergeDupLines %u->%u", read_pos, write_pos);
    lines->resize(write_pos);
  }

  void SetRealMaxHeight(size_t i) {
    int64_t max_height = 0;
    for (const auto& line : limited_lines_.at(i)) {
      if (line.y1 - line.y0 > max_height) {
        max_height = static_cast<int64_t>(line.y1) - line.y0;
      }
    }
    CHECK_LE_S(max_height, max_height_.at(i));
    LOG_S(INFO) << "Reduce max height from " << max_height_.at(i) << " to "
                << max_height;

    max_height_.at(i) = max_height;
  }

  static inline int Intersects(const int32_t ray_x, const int32_t ray_y,
                               const Line& line) {
    // Completely above or below ray?
    // Note: We know that line.y0 < line.y1.

    if (ray_y <= line.y0 || ray_y > line.y1) {
      return 0;
    }
    // Post condition: y1 <= ray_t and y1 >= ray_y.

    // Completely on the left side of the ray-startpoint?
    if (line.x0 < ray_x && line.x1 < ray_x) {
      return 0;
    }

    // Simple Intersection. Start and end point are past ray_x, so the
    // intersection is at >=ray_x.
    if (line.x0 >= ray_x && line.x1 >= ray_x) {
      return 1;
    }

    // For given ray_y, find out 'xcross' where the line crosses at ray_y.
    // The result can be used to determine if the crossing is right or left of
    // the point (ray_x,ray_y).
    // Note: y0 != y1, because conditions above.
    // The formula implemented is
    //   const float m_inverse = (x1 - x1) / (y1 - y1);
    //   const float xcross = m_inverse * (ray_y - y1) + x1;
    // Since the values are integers, the order of computation is changed
    // compared to when using doubles.
    CHECK_NE_S(line.y0, line.y1);
    const int64_t xcross = static_cast<int64_t>(line.x0) +
                           (static_cast<int64_t>(line.x1 - line.x0) *
                            static_cast<int64_t>(ray_y - line.y0)) /
                               static_cast<int64_t>(line.y1 - line.y0);

    return (xcross >= ray_x) ? 1 : 0;
  }

  int CountIntersections(const int32_t p_x, const int32_t p_y) const {
    std::unordered_map<uint16_t, int32_t> counts_per_country;
    CountRayIntersections(p_x, p_y, &counts_per_country);
    int total = 0;
    for (auto [country_num, count] : counts_per_country) {
      total += count;
    }
    return total;
  }

  std::vector<uint32_t> max_height_;
  std::vector<std::vector<Line>> limited_lines_;

  friend void TestFastPolygonContains();
};
