#pragma once

#include <fstream>
#include <string_view>

#include "absl/strings/str_split.h"
#include "base/country_code.h"
#include "base/util.h"
#include "logging/loguru.h"

enum HIGHWAY_LABEL : uint8_t {
  HW_MOTORWAY = 0,
  HW_MOTORWAY_JUNCTION,
  HW_MOTORWAY_LINK,
  HW_TRUNK,
  HW_TRUNK_LINK,
  HW_PRIMARY,
  HW_PRIMARY_LINK,
  HW_SECONDARY,
  HW_SECONDARY_LINK,
  HW_TERTIARY,
  HW_TERTIARY_LINK,
  HW_TURNING_CIRCLE,  // Wendeplatz
  HW_RESIDENTIAL,
  HW_UNCLASSIFIED,
  HW_LIVING_STREET,
  HW_SERVICE,
  HW_BUSWAY,
  HW_BUS_GUIDEWAY,
  HW_CYCLEWAY,
  HW_FOOTWAY,
  HW_TRACK,
  HW_PEDESTRIAN,
  HW_PATH,
  HW_STEPS,
  HW_ESCAPE,  // NOROUTE
  HW_ROAD,    // NOROUTE
  HW_BRIDLEWAY,
  HW_MAX
  // handle: footway=sidewalk, junction=*
};

namespace {
std::vector<std::string_view> HighWayLabelStringVector() {
  std::vector<std::string_view> v((size_t)HW_MAX);
  v[(size_t)HW_MOTORWAY] = "motorway";
  v[(size_t)HW_MOTORWAY_JUNCTION] = "motorway_junction";
  v[(size_t)HW_MOTORWAY_LINK] = "motorway_link";
  v[(size_t)HW_TRUNK] = "trunk";
  v[(size_t)HW_TRUNK_LINK] = "trunk_link";
  v[(size_t)HW_PRIMARY] = "primary";
  v[(size_t)HW_PRIMARY_LINK] = "primary_link";
  v[(size_t)HW_SECONDARY] = "secondary";
  v[(size_t)HW_SECONDARY_LINK] = "secondary_link";
  v[(size_t)HW_TERTIARY] = "tertiary";
  v[(size_t)HW_TERTIARY_LINK] = "tertiary_link";
  v[(size_t)HW_TURNING_CIRCLE] = "turning_circle";
  v[(size_t)HW_RESIDENTIAL] = "residential";
  v[(size_t)HW_UNCLASSIFIED] = "unclassified";
  v[(size_t)HW_LIVING_STREET] = "living_street";
  v[(size_t)HW_SERVICE] = "service";
  v[(size_t)HW_BUSWAY] = "busway";
  v[(size_t)HW_BUS_GUIDEWAY] = "bus_guideway";
  v[(size_t)HW_CYCLEWAY] = "cycleway";
  v[(size_t)HW_FOOTWAY] = "footway";
  v[(size_t)HW_TRACK] = "track";
  v[(size_t)HW_PEDESTRIAN] = "pedestrian";
  v[(size_t)HW_PATH] = "path";
  v[(size_t)HW_STEPS] = "steps";
  v[(size_t)HW_ESCAPE] = "escape";
  v[(size_t)HW_ROAD] = "road";
  v[(size_t)HW_BRIDLEWAY] = "bridleway";
  return v;
}
}  // namespace

inline std::string_view HighwayLabelToString(HIGHWAY_LABEL hw) {
  static const std::vector<std::string_view> v = HighWayLabelStringVector();
  return v.at((size_t)hw);
}

inline HIGHWAY_LABEL HighwayLabelToEnum(std::string_view hw_str) {
  if (hw_str.empty()) {
    return HW_MAX;
  }
  for (HIGHWAY_LABEL hw = HW_MOTORWAY; hw < HW_MAX;
       hw = (HIGHWAY_LABEL)(hw + 1)) {
    if (hw_str == HighwayLabelToString(hw)) {
      return hw;
    }
  }
  return HW_MAX;
}

inline std::vector<HIGHWAY_LABEL> HighwayLabelsByPrefix(
    std::string_view prefix) {
  std::vector<HIGHWAY_LABEL> hws;
  for (HIGHWAY_LABEL hw = HW_MOTORWAY; hw < HW_MAX;
       hw = (HIGHWAY_LABEL)(hw + 1)) {
    if (absl::StartsWith(HighwayLabelToString(hw), prefix)) {
      hws.push_back(hw);
    }
  }
  return hws;
}

enum DIRECTION : uint8_t {
  DIR_BOTH = 0,
  DIR_FORWARD,
  DIR_BACKWARD,
  DIR_MAX,
};

namespace {
std::vector<std::string_view> DirectionStringVector() {
  std::vector<std::string_view> v((size_t)DIR_MAX);
  v[(size_t)DIR_BOTH] = "both";
  v[(size_t)DIR_FORWARD] = "forward";
  v[(size_t)DIR_BACKWARD] = "backward";
  return v;
}
}  // namespace

inline std::string_view DirectionToString(DIRECTION dir) {
  static const std::vector<std::string_view> v = DirectionStringVector();
  return v.at((size_t)dir);
}

inline DIRECTION DirectionToEnum(std::string_view dir_str) {
  if (dir_str.empty()) {
    return DIR_MAX;
  }
  for (DIRECTION dir = DIR_BOTH; dir < DIR_MAX; dir = (DIRECTION)(dir + 1)) {
    if (dir_str == DirectionToString(dir)) {
      return dir;
    }
  }
  return DIR_MAX;
}

inline bool IsDirForward(DIRECTION dir) {
  return dir == DIR_FORWARD || dir == DIR_BOTH;
}

inline bool IsDirBackward(DIRECTION dir) {
  return dir == DIR_BACKWARD || dir == DIR_BOTH;
}

enum VEHICLE : uint8_t {
  VH_MOTOR_VEHICLE = 0,
  VH_BICYCLE = 1,
  VH_FOOT = 2,
  VH_MOPED = 3,
  VH_HORSE = 4,
  VH_MOTORCYCLE = 5,
  VH_PSV = 6,
  VH_BUS = 7,
  VH_HGV = 8,
  VH_MAX = 9,
  // COACH?
  // HAZMAT?
  // TAXI?
  // AGRICULTURAL?
  // FORESTRY?
};

namespace {
std::vector<std::string_view> VehicleStringVector() {
  std::vector<std::string_view> v((size_t)VH_MAX);
  v[(size_t)VH_MOTOR_VEHICLE] = "motor_vehicle";
  v[(size_t)VH_BICYCLE] = "bicycle";
  v[(size_t)VH_FOOT] = "foot";
  v[(size_t)VH_MOPED] = "moped";
  v[(size_t)VH_HORSE] = "horse";
  v[(size_t)VH_MOTORCYCLE] = "motorcycle";
  v[(size_t)VH_PSV] = "psv";
  v[(size_t)VH_BUS] = "bus";
  v[(size_t)VH_HGV] = "hgv";
  return v;
}
}  // namespace

inline std::string_view VehicleToString(VEHICLE vh) {
  static const std::vector<std::string_view> v = VehicleStringVector();
  return v.at((size_t)vh);
}

inline VEHICLE VehicleToEnum(std::string_view vh_str) {
  if (vh_str.empty()) {
    return VH_MAX;
  }
  for (VEHICLE vh = VH_MOTOR_VEHICLE; vh < VH_MAX; vh = (VEHICLE)(vh + 1)) {
    if (vh_str == VehicleToString(vh)) {
      return vh;
    }
  }
  return VH_MAX;
}

inline VEHICLE PrefixedVehicleToEnum(std::string_view vh_str) {
  if (!ConsumePrefixIf("vh_", &vh_str)) {
    return VH_MAX;
  }
  return VehicleToEnum(vh_str);
}

inline bool VehicleIsMotorized(VEHICLE vh) {
  switch (vh) {
    case VH_MOTOR_VEHICLE:
    case VH_MOTORCYCLE:
    case VH_PSV:
    case VH_BUS:
    case VH_HGV:
      return true;
    default:
      return false;
  }
}

enum ACCESS : uint16_t {
  ACC_NO = 0,
  ACC_PRIVATE,
  ACC_CUSTOMERS,
  ACC_DELIVERY,     // mostly for vehicles transporting goods.
  ACC_DESTINATION,  // == no transit
  ACC_PERMISSIVE,
  ACC_YES,
  ACC_DESIGNATED,
  ACC_MAX,
};

namespace {
std::vector<std::string_view> AccessStringVector() {
  std::vector<std::string_view> v((size_t)ACC_MAX);
  v[(size_t)ACC_NO] = "no";
  v[(size_t)ACC_PRIVATE] = "private";
  v[(size_t)ACC_CUSTOMERS] = "customers";
  v[(size_t)ACC_DELIVERY] = "delivery";
  v[(size_t)ACC_DESTINATION] = "destination";
  v[(size_t)ACC_PERMISSIVE] = "permissive";
  v[(size_t)ACC_YES] = "yes";
  v[(size_t)ACC_DESIGNATED] = "designated";
  return v;
}
}  // namespace

inline std::string_view AccessToString(ACCESS acc) {
  static const std::vector<std::string_view> v = AccessStringVector();
  return v.at((size_t)acc);
}

inline ACCESS AccessToEnum(std::string_view acc_str) {
  if (acc_str.empty()) {
    return ACC_MAX;
  }
  for (ACCESS acc = ACC_NO; acc < ACC_MAX; acc = (ACCESS)(acc + 1)) {
    if (acc_str == AccessToString(acc)) {
      return acc;
    }
  }
  return ACC_MAX;
}

inline ACCESS PrefixedAccessToEnum(std::string_view acc_str) {
  if (!ConsumePrefixIf("acc_", &acc_str)) {
    return ACC_MAX;
  }
  return AccessToEnum(acc_str);
}

enum ENVIRONMENT_TYPE : uint8_t {
  ET_ANY = 0,
  ET_URBAN,
  ET_RURAL,
  ET_MAX,
};

inline std::string_view EnvironmentTypeToString(ENVIRONMENT_TYPE et) {
  if (et == ET_ANY) {
    return "any";
  } else if (et == ET_URBAN) {
    return "urban";
  } else if (et == ET_RURAL) {
    return "rural";
  }
  return "n/a";
}

enum IS_MOTORROAD : uint8_t {
  IM_NO = 0,
  IM_YES,
  IM_MAX,
};

enum TURNS {
  TURN_NONE = 0,
  TURN_THROUGH = 1,
  TURN_LEFT = 2,
  TURN_SLIGHT_LEFT = 3,
  TURN_SHARP_LEFT = 3,
  TURN_RIGHT = 5,
  TURN_SLIGHT_RIGHT = 6,
  TURN_SHARP_RIGHT = 7,
  TURN_MERGE_TO_LEFT = 8,
  TURN_MERGE_TO_RIGHT = 9,
  TURN_REVERSE = 10,
  TURN_MAX,
};

enum CHANGE {
  CHANGE_YES = 0,
  CHANGE_NO = 1,
  CHANGE_NOT_LEFT = 2,
  CHANGE_NOT_RIGHT = 3,
  CHANGE_MAX,
};

enum SURFACE {
  SURFACE_UNKNOWN = 0,
  SURFACE_PAVED = 1,
  SURFACE_ASPHALT = 2,
  SURFACE_CHIPSEAL = 3,
  SURFACE_CONCRETE = 4,
  SURFACE_CONCRETE_LANES = 5,
  // many more ....
  SURFACE_MAX,
};

enum SMOOTHNESS {
  SMOOTHNESS_EXCELLENT = 0,
  // many more ....
  SMOOTHNESS_MAX,
};

constexpr uint16_t INFINITE_MAXSPEED = 1023;  // 2^10 - 1
constexpr uint16_t WALK_MAXSPEED = 7;

struct alignas(2) RoutingAttrs {
  ACCESS access : 4;  // yes, no, private, permissive, destination, ...
  uint16_t maxspeed : 10;
  uint16_t lit : 1;
  uint16_t toll : 1;
  uint16_t surface : 6;
  uint16_t width_dm : 8;    // 0 = unknown, otherwise width in decimeters.
  uint16_t left_side : 1;   // is on left side, e.g. a sidewalk or a cycleway.
  uint16_t right_side : 1;  // is on right side. Both left_side and right_side
                            // can be 1!
  // uint16_t smoothness : 3;
};

inline void ClearRoutingAttrs(RoutingAttrs* ra) {
  std::memset(ra, 0, sizeof(RoutingAttrs));
}

// Parse a numeric maxspeed and return true if successful.
// Expected format is "<num>[ ][mph|knots][km/h]". <num> consist of up to 5
// digits and nothing else, or is "none", which means no restriction or "walk".
// The returned value is in km/h, rounded to the closest whole number.
inline bool ParseNumericMaxspeed(std::string_view val,
                                 std::uint16_t* maxspeed) {
  if (val.empty()) {
    return false;
  }
  if (val == "none") {
    // TODO: This value is too high for routing. There should be something like
    // PRACTICAL_INFINITE_MAXSPEED or a limit for maxspeed in the config.
    *maxspeed = INFINITE_MAXSPEED;
    return true;
  } else if (val == "walk") {
    // TODO: handle walking speed better.
    *maxspeed = WALK_MAXSPEED;
    return true;
  }
  std::uint32_t parsed_speed = 0;
  std::size_t pos = 0;
  while (pos < val.size() && std::isdigit(val.at(pos))) {
    parsed_speed = parsed_speed * 10 + (val.at(pos) - '0');
    pos++;
  }
  if (pos > 0 && pos < 6) {
    if (pos == val.size()) {
      // We consumed all data, we're done.
      *maxspeed = parsed_speed;
      return true;
    }
    if (val.at(pos) == ' ') {
      const std::string_view unit = val.substr(pos + 1);
      if (unit == "mph") {
        *maxspeed = static_cast<uint16_t>(parsed_speed * 1.609344 + 0.5);
        return true;
      } else if (unit == "knots") {
        *maxspeed = static_cast<uint16_t>(parsed_speed * 1.852 + 0.5);
        return true;
      } else if (unit == "km/h") {
        *maxspeed = static_cast<uint16_t>(parsed_speed);
        return true;
      }
    }
  }
  return false;
}

inline std::string RoutingAttrsDebugString(RoutingAttrs ra) {
  return absl::StrFormat("dir:%s acc:%s maxsp:%u",
                         AccessToString(ra.access).substr(0, 4), ra.maxspeed);
}
