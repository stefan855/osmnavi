#pragma once

#include <string_view>

#include "absl/strings/str_cat.h"

// A key in a OSM key-value pair is a combination of key parts below, as in
// 'maxspeed:forward'. Note that other keys exist, but we're not interested in
// them.
constexpr const std::string_view kKeyParts[] = {
    "access",
    "advisory",
    "backward",
    "barrier",
    "bicycle",
    "bicycle_road",
    "both",
    "both_ways",
    "bridge",
    "bus",
    "change",
    "conditional",
    "crossing",
    "cycleway",
    "direction",
    "foot",
    "footway",
    "forward",
    "hgv",
    "highway",
    "horse",
    "incline",
    "junction",
    "lane",
    "lane_markings",
    "lanes",
    "lanes_inner",
    "layer",
    "left",
    "lit",
    "locked",
    "markings",
    "maxspeed",
    "moped",
    "motor_vehicle",
    "motorcar",
    "motorcycle",
    "motorroad",
    "name",
    "oneway",
    "practical",
    "priority_road",
    "psv",
    "public_transport",
    "railway",
    "right",
    "service",
    "sidewalk",
    "signals",
    "smoothness",
    "source",
    "stop",
    "surface",
    "toll",
    "tracktype",
    "traffic",
    "traffic_calming",
    "traffic_sign",
    "traffic_signals",
    "tunnel",
    "turn",
    "type",
    "variable",
    "vehicle",
    "width",
    "zone",
};

namespace {
// Get the position of 'key_part' in 'kKeyParts'. Fails if key_part does not
// exist. If called during constexpr computation then it will fail the
// compilation.
constexpr int GetMandatoryKeyBit(std::string_view key_part) {
  for (size_t pos = 0; pos < sizeof(kKeyParts) / sizeof(std::string_view);
       ++pos) {
    if (key_part == kKeyParts[pos]) return pos;
  }
  assert(false);
}

}  // namespace

// These constants are set at compile time!
constexpr uint8_t KEY_BIT_ACCESS = GetMandatoryKeyBit("access");
constexpr uint8_t KEY_BIT_ADVISORY = GetMandatoryKeyBit("advisory");
constexpr uint8_t KEY_BIT_BACKWARD = GetMandatoryKeyBit("backward");
constexpr uint8_t KEY_BIT_BARRIER = GetMandatoryKeyBit("barrier");
constexpr uint8_t KEY_BIT_BICYCLE = GetMandatoryKeyBit("bicycle");
constexpr uint8_t KEY_BIT_BICYCLE_ROAD = GetMandatoryKeyBit("bicycle_road");
constexpr uint8_t KEY_BIT_BOTH = GetMandatoryKeyBit("both");
constexpr uint8_t KEY_BIT_BOTH_WAYS = GetMandatoryKeyBit("both_ways");
constexpr uint8_t KEY_BIT_BRIDGE = GetMandatoryKeyBit("bridge");
constexpr uint8_t KEY_BIT_BUS = GetMandatoryKeyBit("bus");
constexpr uint8_t KEY_BIT_CHANGE = GetMandatoryKeyBit("change");
constexpr uint8_t KEY_BIT_CONDITIONAL = GetMandatoryKeyBit("conditional");
constexpr uint8_t KEY_BIT_CROSSING = GetMandatoryKeyBit("crossing");
constexpr uint8_t KEY_BIT_CYCLEWAY = GetMandatoryKeyBit("cycleway");
constexpr uint8_t KEY_BIT_DIRECTION = GetMandatoryKeyBit("direction");
constexpr uint8_t KEY_BIT_FOOT = GetMandatoryKeyBit("foot");
constexpr uint8_t KEY_BIT_FOOTWAY = GetMandatoryKeyBit("footway");
constexpr uint8_t KEY_BIT_FORWARD = GetMandatoryKeyBit("forward");
constexpr uint8_t KEY_BIT_HGV = GetMandatoryKeyBit("hgv");
constexpr uint8_t KEY_BIT_HIGHWAY = GetMandatoryKeyBit("highway");
constexpr uint8_t KEY_BIT_HORSE = GetMandatoryKeyBit("horse");
constexpr uint8_t KEY_BIT_INCLINE = GetMandatoryKeyBit("incline");
constexpr uint8_t KEY_BIT_JUNCTION = GetMandatoryKeyBit("junction");
constexpr uint8_t KEY_BIT_LANE = GetMandatoryKeyBit("lane");
constexpr uint8_t KEY_BIT_LANE_MARKINGS = GetMandatoryKeyBit("lane_markings");
constexpr uint8_t KEY_BIT_LANES = GetMandatoryKeyBit("lanes");
// "lanes" if it is not at the start of the key.
constexpr uint8_t KEY_BIT_LANES_INNER = GetMandatoryKeyBit("lanes_inner");
constexpr uint8_t KEY_BIT_LAYER = GetMandatoryKeyBit("layer");
constexpr uint8_t KEY_BIT_LEFT = GetMandatoryKeyBit("left");
constexpr uint8_t KEY_BIT_LIT = GetMandatoryKeyBit("lit");
constexpr uint8_t KEY_BIT_LOCKED = GetMandatoryKeyBit("locked");
constexpr uint8_t KEY_BIT_MARKINGS = GetMandatoryKeyBit("markings");
constexpr uint8_t KEY_BIT_MAXSPEED = GetMandatoryKeyBit("maxspeed");
constexpr uint8_t KEY_BIT_MOPED = GetMandatoryKeyBit("moped");
constexpr uint8_t KEY_BIT_MOTOR_VEHICLE = GetMandatoryKeyBit("motor_vehicle");
constexpr uint8_t KEY_BIT_MOTORCAR = GetMandatoryKeyBit("motorcar");
constexpr uint8_t KEY_BIT_MOTORCYCLE = GetMandatoryKeyBit("motorcycle");
constexpr uint8_t KEY_BIT_MOTORROAD = GetMandatoryKeyBit("motorroad");
constexpr uint8_t KEY_BIT_NAME = GetMandatoryKeyBit("name");
constexpr uint8_t KEY_BIT_ONEWAY = GetMandatoryKeyBit("oneway");
constexpr uint8_t KEY_BIT_PRACTICAL = GetMandatoryKeyBit("practical");
constexpr uint8_t KEY_BIT_PRIORITY_ROAD = GetMandatoryKeyBit("priority_road");
constexpr uint8_t KEY_BIT_PSV = GetMandatoryKeyBit("psv");
constexpr uint8_t KEY_BIT_PUBLIC_TRANSPORT =
    GetMandatoryKeyBit("public_transport");
constexpr uint8_t KEY_BIT_RAILWAY = GetMandatoryKeyBit("railway");
constexpr uint8_t KEY_BIT_RIGHT = GetMandatoryKeyBit("right");
constexpr uint8_t KEY_BIT_SERVICE = GetMandatoryKeyBit("service");
constexpr uint8_t KEY_BIT_SIDEWALK = GetMandatoryKeyBit("sidewalk");
constexpr uint8_t KEY_BIT_SIGNALS = GetMandatoryKeyBit("signals");
constexpr uint8_t KEY_BIT_SMOOTHNESS = GetMandatoryKeyBit("smoothness");
constexpr uint8_t KEY_BIT_SOURCE = GetMandatoryKeyBit("source");
constexpr uint8_t KEY_BIT_STOP = GetMandatoryKeyBit("stop");
constexpr uint8_t KEY_BIT_SURFACE = GetMandatoryKeyBit("surface");
constexpr uint8_t KEY_BIT_TOLL = GetMandatoryKeyBit("toll");
constexpr uint8_t KEY_BIT_TRACKTYPE = GetMandatoryKeyBit("tracktype");
constexpr uint8_t KEY_BIT_TRAFFIC = GetMandatoryKeyBit("traffic");
constexpr uint8_t KEY_BIT_TRAFFIC_CALMING =
    GetMandatoryKeyBit("traffic_calming");
constexpr uint8_t KEY_BIT_TRAFFIC_SIGN = GetMandatoryKeyBit("traffic_sign");
constexpr uint8_t KEY_BIT_TRAFFIC_SIGNALS =
    GetMandatoryKeyBit("traffic_signals");
constexpr uint8_t KEY_BIT_TUNNEL = GetMandatoryKeyBit("tunnel");
constexpr uint8_t KEY_BIT_TURN = GetMandatoryKeyBit("turn");
constexpr uint8_t KEY_BIT_TYPE = GetMandatoryKeyBit("type");
constexpr uint8_t KEY_BIT_VARIABLE = GetMandatoryKeyBit("variable");
constexpr uint8_t KEY_BIT_VEHICLE = GetMandatoryKeyBit("vehicle");
constexpr uint8_t KEY_BIT_WIDTH = GetMandatoryKeyBit("width");
constexpr uint8_t KEY_BIT_ZONE = GetMandatoryKeyBit("zone");
constexpr uint8_t KEY_BIT_MAX = KEY_BIT_ZONE + 1;

// We're not using std::bitset<> because it allows assignment and comparison to
// (unsigned) integers, which is an invitation for nasty errors.
class KeySet final {
 public:
  constexpr KeySet() : bs_(0) {}
  constexpr KeySet(const KeySet& other) : bs_(other.bs_) {}
  // Protect from errors such as "key_set = KEY_BIT_ZONE" or
  // KeySet(KEY_BIT_ZONE).
  constexpr KeySet(__uint128_t bs) = delete;
  // Constructor that accepts a braced list, such as
  //   KeySet({KEY_BIT_WIDTH, KEY_BIT_ZONE})
  constexpr KeySet(std::initializer_list<uint8_t> bits) : bs_(0) {
    for (uint8_t bit : bits) {
      set(bit);
    }
  }

  constexpr void set(uint32_t bit) { bs_ |= ((__uint128_t)1) << bit; }
  constexpr void clear() { bs_ = 0; }
  constexpr bool test(uint32_t bit) const {
    return (bs_ & (((__uint128_t)1) << bit)) != 0;
  }
  constexpr bool any() const { return bs_ != 0; }
  constexpr bool none() const { return bs_ == 0; }
  constexpr KeySet operator&(const KeySet& other) const {
    KeySet ks;
    ks.bs_ = (bs_ & other.bs_);
    return ks;
  }
  constexpr KeySet operator|(const KeySet& other) const {
    KeySet ks;
    ks.bs_ = (bs_ | other.bs_);
    return ks;
  }
  constexpr KeySet operator~() const {
    KeySet ks;
    ks.bs_ = ~bs_;
    return ks;
  }
  constexpr bool operator==(const KeySet& other) const {
    return bs_ == other.bs_;
  }
  constexpr KeySet& operator&=(const KeySet& other) {
    bs_ &= other.bs_;
    return *this;
  }
  constexpr KeySet& operator|=(const KeySet& other) {
    bs_ |= other.bs_;
    return *this;
  }

 private:
  static_assert(KEY_BIT_MAX <= 128);
  // If we ever run out of bits we will use something different, for instance an
  // array of uint64_t.
  __uint128_t bs_;
};

constexpr bool BitsetContainedIn(KeySet bitset, KeySet large_set) {
  return (bitset & large_set) == bitset;
}
constexpr bool BitsetsOverlap(KeySet bitset1, KeySet bitset2) {
  return (bitset1 & bitset2).any();
}

constexpr KeySet BitsetUnion(KeySet bitset1, KeySet bitset2) {
  return (bitset1 | bitset2);
}

inline std::string KeySetToString(KeySet ks) {
  std::string res = "{";
  for (uint8_t bit = 0; bit < KEY_BIT_MAX; ++bit) {
    if (ks.test(bit)) {
      absl::StrAppend(&res, res.size() > 1 ? "," : "", (int)bit);
    }
  }
  absl::StrAppend(&res, "}");
  return res;
}

inline std::string KeySetToSymbolicString(KeySet ks) {
  std::string res = "{";
  for (uint8_t bit = 0; bit < KEY_BIT_MAX; ++bit) {
    if (ks.test(bit)) {
      absl::StrAppend(&res, res.size() > 1 ? "," : "", kKeyParts[bit]);
    }
  }
  absl::StrAppend(&res, "}");
  return res;
}

constexpr KeySet BITSET_VEHICLES({KEY_BIT_MOTORCAR, KEY_BIT_MOTORCYCLE,
                                  KEY_BIT_MOPED, KEY_BIT_HORSE, KEY_BIT_FOOT,
                                  KEY_BIT_BUS, KEY_BIT_HGV, KEY_BIT_BICYCLE});

constexpr KeySet BITSET_MODIFIERS({KEY_BIT_FORWARD, KEY_BIT_BACKWARD,
                                   KEY_BIT_LEFT, KEY_BIT_RIGHT, KEY_BIT_BOTH,
                                   KEY_BIT_BOTH_WAYS});

constexpr KeySet BITSET_LANES_INNER({KEY_BIT_LANES_INNER});

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
        if (k == "barrier") return KEY_BIT_BARRIER;
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
      if (k == "crossing") return KEY_BIT_CROSSING;
      if (k == "cycleway") return KEY_BIT_CYCLEWAY;
      break;

    case 'd':
      if (k == "direction") return KEY_BIT_DIRECTION;
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
        // This doesn't exist in real data, it is created in ParseTags()
        // when a "lanes" tag is seen at non-first position.
        if (k == "lanes_inner") return KEY_BIT_LANES_INNER;
        if (k == "layer") return KEY_BIT_LAYER;
      } else {
        if (k == "left") return KEY_BIT_LEFT;
        if (k == "lit") return KEY_BIT_LIT;
        if (k == "locked") return KEY_BIT_LOCKED;
      }
      break;

    case 'm':
      if (k[1] == 'a') {
        if (k == "markings") return KEY_BIT_MARKINGS;
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
      if (k == "priority_road") return KEY_BIT_PRIORITY_ROAD;
      if (k == "psv") return KEY_BIT_PSV;
      if (k == "public_transport") return KEY_BIT_PUBLIC_TRANSPORT;
      break;

    case 'r':
      if (k == "right") return KEY_BIT_RIGHT;
      if (k == "railway") return KEY_BIT_RAILWAY;
      break;

    case 's':
      if (k[1] < 'o') {
        if (k == "service") return KEY_BIT_SERVICE;
        if (k == "sidewalk") return KEY_BIT_SIDEWALK;
        if (k == "signals") return KEY_BIT_SIGNALS;
        if (k == "smoothness") return KEY_BIT_SMOOTHNESS;
      } else {
        if (k == "source") return KEY_BIT_SOURCE;
        if (k == "stop") return KEY_BIT_STOP;
        if (k == "surface") return KEY_BIT_SURFACE;
      }
      break;

    case 't':
      if (k[1] == 'o') {
        if (k == "toll") return KEY_BIT_TOLL;
      } else if (k[1] == 'r') {
        if (k == "tracktype") return KEY_BIT_TRACKTYPE;
        if (k == "traffic") return KEY_BIT_TRAFFIC;
        if (k == "traffic_calming") return KEY_BIT_TRAFFIC_CALMING;
        if (k == "traffic_sign") return KEY_BIT_TRAFFIC_SIGN;
        if (k == "traffic_signals") return KEY_BIT_TRAFFIC_SIGNALS;
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
