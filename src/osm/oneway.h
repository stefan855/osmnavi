#pragma once

#include "absl/strings/str_format.h"
#include "base/util.h"
#include "graph/routing_attrs.h"
#include "osm/key_bits.h"
#include "osm/osm_helpers.h"
#include "osm/parsed_tag.h"

namespace {

inline DIRECTION DefaultDirection(const OSMTagHelper& tagh,
                                  const HIGHWAY_LABEL hw,
                                  const std::vector<ParsedTag>& ptags) {
  if (hw == HW_MOTORWAY || hw == HW_MOTORWAY_JUNCTION ||
      hw == HW_MOTORWAY_LINK) {
    return DIR_FORWARD;
  }

  // For roundabouts (and circular), the default direction is forward. This
  // should be used if no explicit oneway tag is present. See
  // https://wiki.openstreetmap.org/wiki/DE:Tag:junction=roundabout See
  // https://wiki.openstreetmap.org/wiki/DE:Tag:junction=circular
  for (const ParsedTag& pt : ptags) {
    if (pt.bits == GetBitMask(KEY_BIT_JUNCTION) &&
        (tagh.ToString(pt.val_st_idx) == "roundabout" ||
         tagh.ToString(pt.val_st_idx) == "circular")) {
      return DIR_FORWARD;
    }
  }
  return DIR_BOTH;
}

// Special case cycleway[:[right|left]]=opposite*.
//
// See https://wiki.openstreetmap.org/wiki/DE:Key:cycleway
// cycleway[:[right|left]]=opposite* is obsolete but still exists. Do not handle
// "cycleway:*:oneway=", because this might be for a separate way.
inline bool HasCyclewayOppositeTag(const OSMTagHelper& tagh,
                                   const std::vector<ParsedTag>& ptags) {
  constexpr uint64_t cycleway_bits = GetBitMask(KEY_BIT_CYCLEWAY) |
                                     GetBitMask(KEY_BIT_LEFT) |
                                     GetBitMask(KEY_BIT_RIGHT);
  for (const ParsedTag& pt : ptags) {
    if (pt.first == KEY_BIT_CYCLEWAY &&
        BitsetContainedIn(pt.bits, cycleway_bits)) {
      if (absl::StartsWith(tagh.ToString(pt.val_st_idx), "opposite")) {
        return true;
      }
    }
  }
  return false;
}

inline DIRECTION ExtractOnewayValue(std::string_view oneway_val) {
  if (oneway_val == "yes") {
    return DIR_FORWARD;
  } else if (oneway_val == "-1") {
    return DIR_BACKWARD;
  } else if (oneway_val == "no") {
    return DIR_BOTH;
  }
  // For instance "reversible" or "alternating", which are not handled.
  return DIR_MAX;
}

}  // namespace

// Find the DIRECTION of this way for cars. DIR_MAX represents no possible
// direction. Note that accessibility is not considered.
inline DIRECTION CarRoadDirection(const OSMTagHelper& tagh,
                                  const HIGHWAY_LABEL hw, int64_t way_id,
                                  const std::vector<ParsedTag>& ptags) {
  constexpr uint64_t selector_bits =
      GetBitMask(KEY_BIT_ONEWAY) | GetBitMask(KEY_BIT_VEHICLE) |
      GetBitMask(KEY_BIT_MOTOR_VEHICLE) | GetBitMask(KEY_BIT_MOTORCAR);

  DIRECTION dir = DefaultDirection(tagh, hw, ptags);
  for (const ParsedTag& pt : ptags) {
    if (pt.first == KEY_BIT_ONEWAY) {
      if (BitsetContainedIn(pt.bits, selector_bits)) {
        dir = ExtractOnewayValue(tagh.ToString(pt.val_st_idx));
      }
    }
  }
  return dir;
}

// Find out if this way is oneway or bothways for bicycles.
// Does not find out if bicycles have access, i.e. "oneway=no
// access:bicycle=no" yields that both directions are possible.
//
// Complicated cases:
// 461917749  cycleway:left:oneway=no cycleway:right=no
//
//
// 9428840:   oneway:bicycle=no ## oneway:psv=no ## oneway=yes
// 684582393: cycleway:left=opposite_lane ## highway=residential ##
//            maxspeed=30 ## oneway:bicycle=no ## oneway=yes ##
// 77910473:  busway:left=lane ## cycleway:left=share_busway ##
//            cycleway:right:lane=advisory ## cycleway:right=lane ##
//            highway=residential ## lanes:bus:backward=1 ## lanes:forward=1
//            ## lanes=2 ## maxspeed=50 ## oneway:bicycle=no ##
//            oneway:bus=no ## oneway=yes ## surface=asphalt ##
// 28047081:  bicycle:forward=no ## highway=track ## oneway:bicycle=-1 ##
//            tracktype=grade2 ##
// 519927818: cycleway=opposite ## highway=residential ## maxspeed=30 ##
//            oneway:bicycle=no ## oneway=-1 ## sidewalk:both=separate
//            ## surface=asphalt ##
// 147699876: highway=residential ## lanes=1 ## maxspeed=30 ##
//            oneway:bicycle=-1 ## oneway=yes ## sidewalk=both ##
//            surface=asphalt ##
// 32318831:  bicycle=yes ## cycleway:left:oneway=-1 ##
//            cycleway:left=opposite ## cycleway:right=no ##
//            highway=residential ## maxspeed=30 ## oneway=yes ##
//            sidewalk=both ## surface=asphalt ##
// Find the DIRECTION of this way for cars. DIR_MAX represents no possible
// direction. Note that accessibility is not considered.
inline DIRECTION BicycleRoadDirection(const OSMTagHelper& tagh,
                                      const HIGHWAY_LABEL hw, int64_t way_id,
                                      const std::vector<ParsedTag>& ptags) {
  constexpr uint64_t selector_bits = GetBitMask(KEY_BIT_ONEWAY) |
                                     GetBitMask(KEY_BIT_VEHICLE) |
                                     GetBitMask(KEY_BIT_BICYCLE);

  DIRECTION dir = DefaultDirection(tagh, hw, ptags);
  for (const ParsedTag& pt : ptags) {
    if (pt.first == KEY_BIT_ONEWAY) {
      if (BitsetContainedIn(pt.bits, selector_bits)) {
        dir = ExtractOnewayValue(tagh.ToString(pt.val_st_idx));
      }
    }
  }
  if (dir != DIR_BOTH && HasCyclewayOppositeTag(tagh, ptags)) {
    if (dir == DIR_MAX) {
      return DIR_BACKWARD;
    }
    return DIR_BOTH;
  }
  return dir;
}
