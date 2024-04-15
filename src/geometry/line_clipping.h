#pragma once
// Based on
// https://en.wikipedia.org/wiki/Cohen-Sutherland_algorithm

struct TwoPoint {
  double x0;
  double y0;
  double x1;
  double y1;
};

namespace {
constexpr uint8_t SideLeft = 1;
constexpr uint8_t SideRight = 2;
constexpr uint8_t SideBottom = 4;
constexpr uint8_t SideTop = 8;

// Compute region code of point (x,y) relative to 'rect'.
inline uint8_t RegionCode(double x, double y, const TwoPoint& rect) {
  uint8_t code = 0;
  if (x < rect.x0) {
    code = SideLeft;
  } else if (x > rect.x1) {
    code = SideRight;
  }
  if (y < rect.y0) {
    code |= SideBottom;
  } else if (y > rect.y1) {
    code |= SideTop;
  }
  return code;
}
}  // namespace

// Clips line 'line' at the border of rectangle 'rect'. Follows the
// Cohen-Sutherland algorithm as described in
// https://en.wikipedia.org/wiki/Cohen-Sutherland_algorithm. Returns true if the
// line crosses 'rect' and the clipped line in 'line'. Returns false if the line
// is not crossing  'rect'.
inline bool ClipLineCohenSutherland(const TwoPoint& rect, TwoPoint* line) {
  uint8_t code0 = RegionCode(line->x0, line->y0, rect);
  uint8_t code1 = RegionCode(line->x1, line->y1, rect);
  while (true) {
    if (code0 & code1) {
      // Both points share a side, no intersection.
      return false;
    } else if (!(code0 | code1)) {
      // Both are inside.
      return true;
    } else {
      double x;
      double y;
      // Compute slope.
      const double m = (line->y1 - line->y0) / (line->x1 - line->x0);
      const uint8_t high_code = code0 > code1 ? code0 : code1;
      // Compute intersection on one side.
      if (high_code & SideTop) {
        x = line->x0 + (rect.y1 - line->y0) / m;
        y = rect.y1;
      } else if (high_code & SideBottom) {
        x = line->x0 + (rect.y0 - line->y0) / m;
        y = rect.y0;
      } else if (high_code & SideRight) {
        x = rect.x1;
        y = line->y0 + (rect.x1 - line->x0) * m;
      } else {
        CHECK_S(high_code & SideLeft);
        x = rect.x0;
        y = line->y0 + (rect.x0 - line->x0) * m;
      }
      // Clip and recompute code.
      if (high_code == code0) {
        line->x0 = x;
        line->y0 = y;
        code0 = RegionCode(x, y, rect);
      } else {
        line->x1 = x;
        line->y1 = y;
        code1 = RegionCode(x, y, rect);
      }
    }
  }
}
