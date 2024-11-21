#pragma once

#include <string_view>

#include "absl/strings/str_cat.h"

// A key in a OSM key-value pair is a combination of key parts below, as in
// 'maxspeed:forward'. Note that other keys exist, but we're not interested in
// them.
constexpr const std::string_view kKeyParts[] = {
    "access",      "advisory",      "backward",  "bicycle",     "bicycle_road",
    "both",        "both_ways",     "bridge",    "bus",         "change",
    "conditional", "cycleway",      "foot",      "footway",     "forward",
    "hgv",         "highway",       "horse",     "incline",     "junction",
    "lane",        "lane_markings", "lanes",     "lanes_inner", "layer",
    "left",        "lit",           "maxspeed",  "moped",       "motor_vehicle",
    "motorcar",    "motorcycle",    "motorroad", "name",        "oneway",
    "practical",   "psv",           "right",     "service",     "sidewalk",
    "smoothness",  "source",        "surface",   "toll",        "tracktype",
    "traffic",     "tunnel",        "turn",      "type",        "variable",
    "vehicle",     "width",         "zone",
};

namespace {
// Get the position of 'key_part' in 'kKeyParts'. Fails if key_part does not
// exist. If called during constexpr computation then it will fail the
// compilation.
constexpr int GetMandatoryKeyPartBit(std::string_view key_part) {
  for (size_t pos = 0; pos < sizeof(kKeyParts) / sizeof(std::string_view);
       ++pos) {
    if (key_part == kKeyParts[pos]) return pos;
  }
  assert(false);
}

}  // namespace

// These constants are set at compile time!
constexpr uint8_t KEY_BIT_ACCESS = GetMandatoryKeyPartBit("access");
constexpr uint8_t KEY_BIT_ADVISORY = GetMandatoryKeyPartBit("advisory");
constexpr uint8_t KEY_BIT_BACKWARD = GetMandatoryKeyPartBit("backward");
constexpr uint8_t KEY_BIT_BICYCLE = GetMandatoryKeyPartBit("bicycle");
constexpr uint8_t KEY_BIT_BICYCLE_ROAD = GetMandatoryKeyPartBit("bicycle_road");
constexpr uint8_t KEY_BIT_BOTH = GetMandatoryKeyPartBit("both");
constexpr uint8_t KEY_BIT_BOTH_WAYS = GetMandatoryKeyPartBit("both_ways");
constexpr uint8_t KEY_BIT_BRIDGE = GetMandatoryKeyPartBit("bridge");
constexpr uint8_t KEY_BIT_BUS = GetMandatoryKeyPartBit("bus");
constexpr uint8_t KEY_BIT_CHANGE = GetMandatoryKeyPartBit("change");
constexpr uint8_t KEY_BIT_CONDITIONAL = GetMandatoryKeyPartBit("conditional");
constexpr uint8_t KEY_BIT_CYCLEWAY = GetMandatoryKeyPartBit("cycleway");
constexpr uint8_t KEY_BIT_FOOT = GetMandatoryKeyPartBit("foot");
constexpr uint8_t KEY_BIT_FOOTWAY = GetMandatoryKeyPartBit("footway");
constexpr uint8_t KEY_BIT_FORWARD = GetMandatoryKeyPartBit("forward");
constexpr uint8_t KEY_BIT_HGV = GetMandatoryKeyPartBit("hgv");
constexpr uint8_t KEY_BIT_HIGHWAY = GetMandatoryKeyPartBit("highway");
constexpr uint8_t KEY_BIT_HORSE = GetMandatoryKeyPartBit("horse");
constexpr uint8_t KEY_BIT_INCLINE = GetMandatoryKeyPartBit("incline");
constexpr uint8_t KEY_BIT_JUNCTION = GetMandatoryKeyPartBit("junction");
constexpr uint8_t KEY_BIT_LANE = GetMandatoryKeyPartBit("lane");
constexpr uint8_t KEY_BIT_LANE_MARKINGS =
    GetMandatoryKeyPartBit("lane_markings");
constexpr uint8_t KEY_BIT_LANES = GetMandatoryKeyPartBit("lanes");
// "lanes" if it is not at the start of the key.
constexpr uint8_t KEY_BIT_LANES_INNER = GetMandatoryKeyPartBit("lanes_inner");
constexpr uint8_t KEY_BIT_LAYER = GetMandatoryKeyPartBit("layer");
constexpr uint8_t KEY_BIT_LEFT = GetMandatoryKeyPartBit("left");
constexpr uint8_t KEY_BIT_LIT = GetMandatoryKeyPartBit("lit");
constexpr uint8_t KEY_BIT_MAXSPEED = GetMandatoryKeyPartBit("maxspeed");
constexpr uint8_t KEY_BIT_MOPED = GetMandatoryKeyPartBit("moped");
constexpr uint8_t KEY_BIT_MOTOR_VEHICLE =
    GetMandatoryKeyPartBit("motor_vehicle");
constexpr uint8_t KEY_BIT_MOTORCAR = GetMandatoryKeyPartBit("motorcar");
constexpr uint8_t KEY_BIT_MOTORCYCLE = GetMandatoryKeyPartBit("motorcycle");
constexpr uint8_t KEY_BIT_MOTORROAD = GetMandatoryKeyPartBit("motorroad");
constexpr uint8_t KEY_BIT_NAME = GetMandatoryKeyPartBit("name");
constexpr uint8_t KEY_BIT_ONEWAY = GetMandatoryKeyPartBit("oneway");
constexpr uint8_t KEY_BIT_PRACTICAL = GetMandatoryKeyPartBit("practical");
constexpr uint8_t KEY_BIT_PSV = GetMandatoryKeyPartBit("psv");
constexpr uint8_t KEY_BIT_RIGHT = GetMandatoryKeyPartBit("right");
constexpr uint8_t KEY_BIT_SERVICE = GetMandatoryKeyPartBit("service");
constexpr uint8_t KEY_BIT_SIDEWALK = GetMandatoryKeyPartBit("sidewalk");
constexpr uint8_t KEY_BIT_SMOOTHNESS = GetMandatoryKeyPartBit("smoothness");
constexpr uint8_t KEY_BIT_SOURCE = GetMandatoryKeyPartBit("source");
constexpr uint8_t KEY_BIT_SURFACE = GetMandatoryKeyPartBit("surface");
constexpr uint8_t KEY_BIT_TOLL = GetMandatoryKeyPartBit("toll");
constexpr uint8_t KEY_BIT_TRACKTYPE = GetMandatoryKeyPartBit("tracktype");
constexpr uint8_t KEY_BIT_TRAFFIC = GetMandatoryKeyPartBit("traffic");
constexpr uint8_t KEY_BIT_TUNNEL = GetMandatoryKeyPartBit("tunnel");
constexpr uint8_t KEY_BIT_TURN = GetMandatoryKeyPartBit("turn");
constexpr uint8_t KEY_BIT_TYPE = GetMandatoryKeyPartBit("type");
constexpr uint8_t KEY_BIT_VARIABLE = GetMandatoryKeyPartBit("variable");
constexpr uint8_t KEY_BIT_VEHICLE = GetMandatoryKeyPartBit("vehicle");
constexpr uint8_t KEY_BIT_WIDTH = GetMandatoryKeyPartBit("width");
constexpr uint8_t KEY_BIT_ZONE = GetMandatoryKeyPartBit("zone");
constexpr uint8_t KEY_BIT_MAX = KEY_BIT_ZONE + 1;

constexpr uint64_t GetBitMask(uint8_t bit) { return 1ull << bit; }
constexpr uint64_t GetBitMask(uint8_t bit, uint8_t bit2) {
  return GetBitMask(bit) + GetBitMask(bit2);
}
constexpr uint64_t GetBitMask(uint8_t bit, uint8_t bit2, uint8_t bit3) {
  return GetBitMask(bit) + GetBitMask(bit2) + GetBitMask(bit3);
}
constexpr uint64_t GetBitMask(uint8_t bit, uint8_t bit2, uint8_t bit3,
                              uint8_t bit4) {
  return GetBitMask(bit) + GetBitMask(bit2) + GetBitMask(bit3) +
         GetBitMask(bit4);
}
constexpr bool BitIsContained(uint8_t bit, uint64_t bitset) {
  return (bitset & GetBitMask(bit)) != 0;
}
constexpr bool BitsetContainedIn(uint64_t bitset, uint64_t large_set) {
  return (bitset & large_set) == bitset;
}
constexpr bool BitsetsOverlap(uint64_t bitset1, uint64_t bitset2) {
  return (bitset1 & bitset2) != 0;
}

constexpr uint64_t BitsetUnion(uint64_t bitset1, uint64_t bitset2) {
  return (bitset1 | bitset2);
}

constexpr uint64_t BITSET_VEHICLES =
    GetBitMask(KEY_BIT_MOTORCAR) | GetBitMask(KEY_BIT_MOTORCYCLE) |
    GetBitMask(KEY_BIT_MOPED) | GetBitMask(KEY_BIT_HORSE) |
    GetBitMask(KEY_BIT_FOOT) | GetBitMask(KEY_BIT_BUS) |
    GetBitMask(KEY_BIT_HGV) | GetBitMask(KEY_BIT_BICYCLE);

constexpr uint64_t BITSET_MODIFIERS =
    GetBitMask(KEY_BIT_FORWARD) | GetBitMask(KEY_BIT_BACKWARD) |
    GetBitMask(KEY_BIT_LEFT) | GetBitMask(KEY_BIT_RIGHT) |
    GetBitMask(KEY_BIT_BOTH) | GetBitMask(KEY_BIT_BOTH_WAYS);

constexpr uint64_t BITSET_LANES_INNER = GetBitMask(KEY_BIT_LANES_INNER);

inline std::string KeyPartBitsToString(uint64_t bits) {
  std::string res;
  for (uint8_t bit = 0; bit < sizeof(kKeyParts) / sizeof(std::string_view);
       ++bit) {
    if (BitIsContained(bit, bits)) {
      if (res.empty()) {
        res = kKeyParts[bit];
      } else {
        absl::StrAppend(&res, ":", kKeyParts[bit]);
      }
    }
  }
  return res;
}

constexpr int GetKeyPartBitFast(std::string_view k) {
  if (k.size() < 3) return -1;
  switch (k[0]) {
    case 'a':
      if (k == "access") return KEY_BIT_ACCESS;
      if (k == "advisory") return KEY_BIT_ADVISORY;
      break;

    case 'b':
      if (k[1] < 'r') {
        if (k == "backward") return KEY_BIT_BACKWARD;
        if (k == "bicycle") return KEY_BIT_BICYCLE;
        if (k == "bicycle_road") return KEY_BIT_BICYCLE_ROAD;
        if (k == "both") return KEY_BIT_BOTH;
        if (k == "both_ways") return KEY_BIT_BOTH_WAYS;
      } else {
        if (k == "bridge") return KEY_BIT_BRIDGE;
        if (k == "bus") return KEY_BIT_BUS;
      }
      break;

    case 'c':
      if (k == "change") return KEY_BIT_CHANGE;
      if (k == "conditional") return KEY_BIT_CONDITIONAL;
      if (k == "cycleway") return KEY_BIT_CYCLEWAY;
      break;

    case 'f':
      if (k == "foot") return KEY_BIT_FOOT;
      if (k == "footway") return KEY_BIT_FOOTWAY;
      if (k == "forward") return KEY_BIT_FORWARD;
      break;

    case 'h':
      if (k == "hgv") return KEY_BIT_HGV;
      if (k == "highway") return KEY_BIT_HIGHWAY;
      if (k == "horse") return KEY_BIT_HORSE;
      break;

    case 'i':
      if (k == "incline") return KEY_BIT_INCLINE;
      break;

    case 'j':
      if (k == "junction") return KEY_BIT_JUNCTION;
      break;

    case 'l':
      if (k[1] < 'e') {
        if (k == "lane") return KEY_BIT_LANE;
        if (k == "lane_markings") return KEY_BIT_LANE_MARKINGS;
        if (k == "lanes") return KEY_BIT_LANES;
        if (k == "lanes_inner") return KEY_BIT_LANES_INNER;
        if (k == "layer") return KEY_BIT_LAYER;
      } else {
        if (k == "left") return KEY_BIT_LEFT;
        if (k == "lit") return KEY_BIT_LIT;
      }
      break;

    case 'm':
      if (k[1] == 'a') {
        if (k == "maxspeed") return KEY_BIT_MAXSPEED;
      } else if (k == "moped") {
        return KEY_BIT_MOPED;
      } else if (k[2] == 't') {
        if (k == "motor_vehicle") return KEY_BIT_MOTOR_VEHICLE;
        if (k == "motorcar") return KEY_BIT_MOTORCAR;
        if (k == "motorcycle") return KEY_BIT_MOTORCYCLE;
        if (k == "motorroad") return KEY_BIT_MOTORROAD;
      }
      break;

    case 'n':
      if (k == "name") return KEY_BIT_NAME;
      break;

    case 'o':
      if (k == "oneway") return KEY_BIT_ONEWAY;
      break;

    case 'p':
      if (k == "practical") return KEY_BIT_PRACTICAL;
      if (k == "psv") return KEY_BIT_PSV;
      break;

    case 'r':
      if (k == "right") return KEY_BIT_RIGHT;
      break;

    case 's':
      if (k[1] < 'o') {
        if (k == "service") return KEY_BIT_SERVICE;
        if (k == "sidewalk") return KEY_BIT_SIDEWALK;
        if (k == "smoothness") return KEY_BIT_SMOOTHNESS;
      } else {
        if (k == "source") return KEY_BIT_SOURCE;
        if (k == "surface") return KEY_BIT_SURFACE;
      }
      break;

    case 't':
      if (k[1] == 'o') {
        if (k == "toll") return KEY_BIT_TOLL;
      } else if (k[1] == 'r') {
        if (k == "tracktype") return KEY_BIT_TRACKTYPE;
        if (k == "traffic") return KEY_BIT_TRAFFIC;
      } else if (k[1] == 'u') {
        if (k == "tunnel") return KEY_BIT_TUNNEL;
        if (k == "turn") return KEY_BIT_TURN;
      } else if (k[1] == 'y') {
        if (k == "type") return KEY_BIT_TYPE;
      }
      break;

    case 'v':
      if (k == "variable") return KEY_BIT_VARIABLE;
      if (k == "vehicle") return KEY_BIT_VEHICLE;
      break;

    case 'w':
      if (k == "width") return KEY_BIT_WIDTH;
      break;

    case 'z':
      if (k == "zone") return KEY_BIT_ZONE;
      break;

    default:
      break;
  }
  return -1;
}

constexpr int GetKeyPartBit(std::string_view key_part) {
  for (size_t pos = 0; pos < sizeof(kKeyParts) / sizeof(std::string_view);
       ++pos) {
    if (key_part == kKeyParts[pos]) return pos;
  }
  return -1;
}
