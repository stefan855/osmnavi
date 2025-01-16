#pragma once

template <typename T>
struct Point {
  T x;
  T y;
};

template <typename T>
struct Rectangle {
  Point<T> minp;
  Point<T> maxp;

  // Initialise Rectangle.
  void Set(Point<T> p1, Point<T> p2) {
    minp.x = std::min(p1.x, p2.x);
    minp.y = std::min(p1.y, p2.y);
    maxp.x = std::max(p1.x, p2.x);
    maxp.y = std::max(p1.y, p2.y);
  }

  // Initialise Rectangle.
  void Set(Point<T> p) { Set(p, p); }

  // Add a point to the bounding rectangle.
  void ExtendBound(Point<T> p) {
    minp.x = std::min<T>(minp.x, p.x);
    minp.y = std::min<T>(minp.y, p.y);
    maxp.x = std::max<T>(maxp.x, p.x);
    maxp.y = std::max<T>(maxp.y, p.y);
  }

  bool Contains(Point<T> p) const {
    return p.x >= minp.x && p.x <= maxp.x && p.y >= minp.y && p.y <= maxp.y;
  }
};
