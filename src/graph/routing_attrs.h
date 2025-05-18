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
  HW_TURNING_CIRCLE,  // German: Wendeplatz.
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
  return VECTOR_AT(v, (size_t)hw);
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

// Direction of a way, deduced from the oneway and related tags.
// DIR_FORWARD and DIR_BACKWARD are used as array indices in the code and should
// be kept stable. The invalid direction attribute is indicated by DIR_MAX.
enum DIRECTION : uint8_t {
  DIR_FORWARD = 0,
  DIR_BACKWARD = 1,
  DIR_BOTH = 2,
  DIR_MAX = 3,
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
  return VECTOR_AT(v, (size_t)dir);
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
  VH_MOTORCAR = 0,
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
  v[(size_t)VH_MOTORCAR] = "motorcar";
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
  return VECTOR_AT(v, (size_t)vh);
}

inline VEHICLE VehicleToEnum(std::string_view vh_str) {
  if (vh_str.empty()) {
    return VH_MAX;
  }
  for (VEHICLE vh = VH_MOTORCAR; vh < VH_MAX; vh = (VEHICLE)(vh + 1)) {
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
    case VH_MOTORCAR:
    case VH_MOTORCYCLE:
    case VH_PSV:
    case VH_BUS:
    case VH_HGV:
      return true;
    default:
      return false;
  }
}

enum ACCESS : uint8_t {
  ACC_NO = 0,
  ACC_PRIVATE,
  ACC_CUSTOMERS,
  ACC_DELIVERY,     // mostly for vehicles transporting goods.
  ACC_DESTINATION,  // == no transit
  ACC_DISMOUNT,     // for bicycles, mopeds, horses, ...
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
  v[(size_t)ACC_DISMOUNT] = "dismount";
  v[(size_t)ACC_PERMISSIVE] = "permissive";
  v[(size_t)ACC_YES] = "yes";
  v[(size_t)ACC_DESIGNATED] = "designated";
  return v;
}
}  // namespace

inline std::string_view AccessToString(ACCESS acc) {
  static const std::vector<std::string_view> v = AccessStringVector();
  return VECTOR_AT(v, (size_t)acc);
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

enum SURFACE : uint16_t {
  // https://wiki.openstreetmap.org/wiki/Key:surface
  // Paved:
  SURFACE_UNKNOWN = 0,
  SURFACE_PAVED,
  SURFACE_ASPHALT,
  SURFACE_CHIPSEAL,  // Similar to asphalt, but not as good.
  SURFACE_CONCRETE,
  SURFACE_CONCRETE_LANES,  // Two concreate lanes for two wheels.
  SURFACE_CONCRETE_PLATES,
  SURFACE_PAVING_STONES,  // Pflastersteine.
  SURFACE_PAVING_STONES_LANES,
  SURFACE_GRASS_PAVER,         // German: Rasengittersteine.
  SURFACE_SETT,                // German: Behauenes Steinpflaster.
  SURFACE_UNHEWN_COBBLESTONE,  // German: Rohes Kopfsteinpflaster.
  SURFACE_COBBLESTONE,         // Better use sett or unhewn_cobblestone
  SURFACE_BRICKS,              // German: Tonziegel.
  SURFACE_METAL,
  SURFACE_METAL_GRID,
  SURFACE_WOOD,
  SURFACE_STEPPING_STONES,  // German: Trittsteine.
  SURFACE_RUBBER,
  SURFACE_TILES,  // German: Fliesen/Kacheln.
  // UNPAVED:
  SURFACE_UNPAVED,      // General. German: Ungepflastert.
  SURFACE_COMPACTED,    // Best quality below paved street
  SURFACE_FINE_GRAVEL,  // German: Kies
  SURFACE_GRAVEL,       // German: Schotter.
  SURFACE_SHELLS,
  SURFACE_ROCK,
  SURFACE_PEBBLESTONE,  // German: Loser Kies
  SURFACE_GROUND,       // German: Naturbelassen
  SURFACE_DIRT,
  SURFACE_EARTH,
  SURFACE_GRASS,  // Only for pedestrians.
  SURFACE_MUD,
  SURFACE_SAND,
  SURFACE_WOODCHIPS,  // German: Hackschnitzel.
  SURFACE_SNOW,
  SURFACE_ICE,
  SURFACE_SALT,
  // SPECIAL
  SURFACE_CLAY,  // German: Asche.
  SURFACE_TARTAN,
  SURFACE_ARTIFICIAL_TURF,  // German: Kunstrasen.
  SURFACE_ACRYLIC,
  SURFACE_CARPET,
  SURFACE_PLASTIC,
  SURFACE_MAX,
};

namespace {
std::vector<std::string_view> SurfaceStringVector() {
  std::vector<std::string_view> v((size_t)SURFACE_MAX);
  v[(size_t)SURFACE_UNKNOWN] = "n/a";
  v[(size_t)SURFACE_PAVED] = "paved";
  v[(size_t)SURFACE_ASPHALT] = "asphalt";
  v[(size_t)SURFACE_CHIPSEAL] = "chipseal";
  v[(size_t)SURFACE_CONCRETE] = "concrete";
  v[(size_t)SURFACE_CONCRETE_LANES] = "concrete:lanes";
  v[(size_t)SURFACE_CONCRETE_PLATES] = "concrete:plates";
  v[(size_t)SURFACE_PAVING_STONES] = "paving_stones";
  v[(size_t)SURFACE_PAVING_STONES_LANES] = "paving_stones:lanes";
  v[(size_t)SURFACE_GRASS_PAVER] = "grass_paver";
  v[(size_t)SURFACE_SETT] = "sett";
  v[(size_t)SURFACE_UNHEWN_COBBLESTONE] = "unhewn_cobblestone";
  v[(size_t)SURFACE_COBBLESTONE] = "cobblestone";
  v[(size_t)SURFACE_BRICKS] = "bricks";
  v[(size_t)SURFACE_METAL] = "metal";
  v[(size_t)SURFACE_METAL_GRID] = "metal_grid";
  v[(size_t)SURFACE_WOOD] = "wood";
  v[(size_t)SURFACE_STEPPING_STONES] = "stepping_stones";
  v[(size_t)SURFACE_RUBBER] = "rubber";
  v[(size_t)SURFACE_TILES] = "tiles";
  v[(size_t)SURFACE_UNPAVED] = "unpaved";
  v[(size_t)SURFACE_COMPACTED] = "compacted";
  v[(size_t)SURFACE_FINE_GRAVEL] = "fine_gravel";
  v[(size_t)SURFACE_GRAVEL] = "gravel";
  v[(size_t)SURFACE_SHELLS] = "shells";
  v[(size_t)SURFACE_ROCK] = "rock";
  v[(size_t)SURFACE_PEBBLESTONE] = "pebblestone";
  v[(size_t)SURFACE_GROUND] = "ground";
  v[(size_t)SURFACE_DIRT] = "dirt";
  v[(size_t)SURFACE_EARTH] = "earth";
  v[(size_t)SURFACE_GRASS] = "grass";
  v[(size_t)SURFACE_MUD] = "mud";
  v[(size_t)SURFACE_SAND] = "sand";
  v[(size_t)SURFACE_WOODCHIPS] = "woodchips";
  v[(size_t)SURFACE_SNOW] = "snow";
  v[(size_t)SURFACE_ICE] = "ice";
  v[(size_t)SURFACE_SALT] = "salt";
  v[(size_t)SURFACE_CLAY] = "clay";
  v[(size_t)SURFACE_TARTAN] = "tartan";
  v[(size_t)SURFACE_ARTIFICIAL_TURF] = "artificial_turf";
  v[(size_t)SURFACE_ACRYLIC] = "acrylic";
  v[(size_t)SURFACE_CARPET] = "carpet";
  v[(size_t)SURFACE_PLASTIC] = "plastic";
  return v;
}
}  // namespace

inline std::string_view SurfaceToString(SURFACE surface) {
  static const std::vector<std::string_view> v = SurfaceStringVector();
  return VECTOR_AT(v, (size_t)surface);
}

inline SURFACE SurfaceToEnum(std::string_view str) {
  if (str.empty()) {
    return SURFACE_MAX;
  }
  for (SURFACE surface = SURFACE_UNKNOWN; surface < SURFACE_MAX;
       surface = (SURFACE)(surface + 1)) {
    if (str == SurfaceToString(surface)) {
      // LOG_S(INFO) << "found surface " << str;
      return surface;
    }
  }
  return SURFACE_MAX;
}

enum SMOOTHNESS : uint16_t {
  // https://wiki.openstreetmap.org/wiki/Key:smoothness
  SMOOTHNESS_UNKNOWN = 0,
  SMOOTHNESS_EXCELLENT,
  SMOOTHNESS_GOOD,          // Last good for racing bike.
  SMOOTHNESS_INTERMEDIATE,  // Last good for city bike, wheel chair, sports car.
  SMOOTHNESS_BAD,           // last good for trekking bike, normal car.
  SMOOTHNESS_VERY_BAD,      // Last good for high clearance cars.
  SMOOTHNESS_HORRIBLE,      // Last good for heavy-duty off road vehicles.
  SMOOTHNESS_VERY_HORRIBLE,  // Last good for tractor mountain bike, tank.
  SMOOTHNESS_IMPASSABLE,     // no wheeled vehicles.
  SMOOTHNESS_MAX,
};

namespace {
std::vector<std::string_view> SmoothnessStringVector() {
  std::vector<std::string_view> v((size_t)SMOOTHNESS_MAX);
  v[(size_t)SMOOTHNESS_UNKNOWN] = "n/a";
  v[(size_t)SMOOTHNESS_EXCELLENT] = "excellent";
  v[(size_t)SMOOTHNESS_GOOD] = "good";
  v[(size_t)SMOOTHNESS_INTERMEDIATE] = "intermediate";
  v[(size_t)SMOOTHNESS_BAD] = "bad";
  v[(size_t)SMOOTHNESS_VERY_BAD] = "very_bad";
  v[(size_t)SMOOTHNESS_HORRIBLE] = "horrible";
  v[(size_t)SMOOTHNESS_VERY_HORRIBLE] = "very_horrible";
  v[(size_t)SMOOTHNESS_IMPASSABLE] = "impassable";
  return v;
}
}  // namespace

inline std::string_view SmoothnessToString(SMOOTHNESS smoothness) {
  static const std::vector<std::string_view> v = SmoothnessStringVector();
  return VECTOR_AT(v, (size_t)smoothness);
}

inline SMOOTHNESS SmoothnessToEnum(std::string_view str) {
  if (str.empty()) {
    return SMOOTHNESS_MAX;
  }
  for (SMOOTHNESS sn = SMOOTHNESS_UNKNOWN; sn < SMOOTHNESS_MAX;
       sn = (SMOOTHNESS)(sn + 1)) {
    if (str == SmoothnessToString(sn)) {
      return sn;
    }
  }
  return SMOOTHNESS_MAX;
}

enum TRACKTYPE : uint16_t {
  // https://wiki.openstreetmap.org/wiki/Key:tracktype
  TRACKTYPE_UNKNOWN = 0,
  TRACKTYPE_GRADE1,  // Sold, paved with asphalt, concrete or so.
  TRACKTYPE_GRADE2,  // Mostly solid, such as gravel, compacted or fine_gravel.
  TRACKTYPE_GRADE3,  // Mix of solid and soft material.
  TRACKTYPE_GRADE4,  // Mostly soft.
  TRACKTYPE_GRADE5,  // Soft, no hard materials.
  TRACKTYPE_MAX,
};

namespace {
std::vector<std::string_view> TracktypeStringVector() {
  std::vector<std::string_view> v((size_t)TRACKTYPE_MAX);
  v[(size_t)TRACKTYPE_UNKNOWN] = "n/a";
  v[(size_t)TRACKTYPE_GRADE1] = "grade1";
  v[(size_t)TRACKTYPE_GRADE2] = "grade2";
  v[(size_t)TRACKTYPE_GRADE3] = "grade3";
  v[(size_t)TRACKTYPE_GRADE4] = "grade4";
  v[(size_t)TRACKTYPE_GRADE5] = "grade5";
  return v;
}
}  // namespace

inline std::string_view TracktypeToString(TRACKTYPE tracktype) {
  static const std::vector<std::string_view> v = TracktypeStringVector();
  return VECTOR_AT(v, (size_t)tracktype);
}

inline TRACKTYPE TracktypeToEnum(std::string_view tracktype_str) {
  if (tracktype_str.empty()) {
    return TRACKTYPE_MAX;
  }
  for (TRACKTYPE tt = TRACKTYPE_UNKNOWN; tt < TRACKTYPE_MAX;
       tt = (TRACKTYPE)(tt + 1)) {
    if (tracktype_str == TracktypeToString(tt)) {
      return tt;
    }
  }
  return TRACKTYPE_MAX;
}

constexpr uint16_t INFINITE_MAXSPEED = 1023;  // 2^10 - 1
constexpr uint16_t WALK_MAXSPEED = 7;

struct alignas(2) RoutingAttrs {
  uint8_t dir : 1;  // 1 if the direction of the road is allowed, 0 if not. 
  ACCESS access : 4;  // no, private, ...
  uint16_t maxspeed : 10;
  uint16_t lit : 1;
  uint16_t toll : 1;
  SURFACE surface : 6;
  TRACKTYPE tracktype : 3;
  SMOOTHNESS smoothness : 4;
  uint16_t left_side : 1;   // is on left side, e.g. a sidewalk or a cycleway.
  uint16_t right_side : 1;  // is on right side. Both left_side and right_side
                            // can be 1!
  uint16_t width_dm : 8;    // 0 = unknown, otherwise width in decimeters.
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
  return absl::StrFormat(
      "dir:%u acc:%s maxsp:%u surface:%s tracktype:%s smoothness:%s",
      ra.dir,
      AccessToString(ra.access).substr(0, 4), ra.maxspeed,
      ra.surface < SURFACE_MAX ? SurfaceToString(ra.surface)
                               : absl::StrCat(ra.surface),
      ra.tracktype < TRACKTYPE_MAX ? TracktypeToString(ra.tracktype)
                                   : absl::StrCat(ra.tracktype),
      ra.smoothness < SMOOTHNESS_MAX ? SmoothnessToString(ra.smoothness)
                                     : absl::StrCat(ra.smoothness));
}
