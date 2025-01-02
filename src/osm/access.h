#pragma once

#include "absl/strings/str_format.h"
#include "base/util.h"
#include "graph/routing_attrs.h"
#include "osm/key_bits.h"
#include "osm/osm_helpers.h"
#include "osm/parsed_tag.h"

namespace {

inline ACCESS ExtendedAccessToEnum(std::string_view val, bool bicycle) {
  ACCESS acc = AccessToEnum(val);
  if (acc == ACC_MAX && bicycle && val == "use_sidepath") {
    // Special case, bicycle should use separate bicycle path.
    acc = ACC_NO;
  }
  return acc;
}

inline ACCESS InterpretAccessValue(std::string_view val, bool lanes,
                                   bool bicycle) {
  if (!lanes) {
    return ExtendedAccessToEnum(val, bicycle);
  }
  ACCESS acc = ACC_MAX;
  for (std::string_view sub : absl::StrSplit(val, '|')) {
    ACCESS sub_acc = ExtendedAccessToEnum(sub, bicycle);
    // Only replace if value is "better".
    if (sub_acc != ACC_MAX && (acc == ACC_MAX || sub_acc > acc)) {
      acc = sub_acc;
    }
  }
  return acc;
}

// Set access in forward and backward direction based on tags.
// 'weak' indicates that only access that already is better than 'no' should be
// modified. For instance, a way with "highway=footway access=permissive" should
// not be set permissive for cars.
inline void SetAccess(const ParsedTag& pt, bool weak, std::string_view value,
                      RoutingAttrs* ra_forw, RoutingAttrs* ra_backw) {
  ACCESS acc =
      InterpretAccessValue(value, BitIsContained(KEY_BIT_LANES_INNER, pt.bits),
                           pt.first == KEY_BIT_BICYCLE);
  if (acc == ACC_MAX) {
    // In general, if we don't know the access value, then it has to be
    // interpreted as a 'no'.
    // For instance, 'agricultural' or 'forestry' will go here.
    acc = ACC_NO;
  }

  const bool forward = BitIsContained(KEY_BIT_FORWARD, pt.bits);
  const bool backward = BitIsContained(KEY_BIT_BACKWARD, pt.bits);
  // If forward/backward are missing, then set both.
  if (forward || !backward) {
    if (!weak || ra_forw->access != ACC_NO) {
      ra_forw->access = acc;
    }
  }
  if (backward || !forward) {
    if (!weak || ra_backw->access != ACC_NO) {
      ra_backw->access = acc;
    }
  }
}
}  // namespace

inline void CarAccess(const OSMTagHelper& tagh, std::int64_t way_id,
                      const std::vector<ParsedTag>& ptags,
                      RoutingAttrs* ra_forw, RoutingAttrs* ra_backw) {
  // Special (hard) cases:
  // 1) access:lanes:backward=motorcar;motorcycle|hgv (TODO!)
  // 2) lanes:motor_vehicle=yes|no|yes
  constexpr uint64_t selector_bits =
      GetBitMask(KEY_BIT_ACCESS) | GetBitMask(KEY_BIT_VEHICLE) |
      GetBitMask(KEY_BIT_MOTOR_VEHICLE) | GetBitMask(KEY_BIT_MOTORCAR);
  // ":both_ways" is not used here. It means a lane that is for both directions,
  // not both ":forward" and ":backward" for the road. See
  // https://wiki.openstreetmap.org/wiki/Forward_%26_backward,_left_%26_right
  constexpr uint64_t modifier_bits = GetBitMask(KEY_BIT_FORWARD) |
                                     GetBitMask(KEY_BIT_BACKWARD) |
                                     GetBitMask(KEY_BIT_LANES_INNER);

  for (const ParsedTag& pt : ptags) {
    if ((pt.bits & selector_bits) == 0) {
      continue;
    }

    switch (pt.bits & ~modifier_bits) {
      case GetBitMask(KEY_BIT_ACCESS):
      case GetBitMask(KEY_BIT_ACCESS, KEY_BIT_VEHICLE):
      case GetBitMask(KEY_BIT_ACCESS, KEY_BIT_MOTOR_VEHICLE):
      case GetBitMask(KEY_BIT_ACCESS, KEY_BIT_MOTORCAR):
      // Instead of access:motorcar=... one can say motorcar=...
      case GetBitMask(KEY_BIT_VEHICLE):
      case GetBitMask(KEY_BIT_MOTOR_VEHICLE):
      case GetBitMask(KEY_BIT_MOTORCAR): {
        const bool weak =
            (pt.bits & ~modifier_bits) == GetBitMask(KEY_BIT_ACCESS);
        SetAccess(pt, weak, tagh.ToString(pt.val_st_idx), ra_forw, ra_backw);
        break;
      }
      default:
        break;
    }
  }
}

inline void BicycleAccess(const OSMTagHelper& tagh, std::int64_t way_id,
                          const std::vector<ParsedTag>& ptags,
                          RoutingAttrs* ra_forw, RoutingAttrs* ra_backw) {
  constexpr uint64_t selector_bits = GetBitMask(KEY_BIT_ACCESS) |
                                     GetBitMask(KEY_BIT_VEHICLE) |
                                     GetBitMask(KEY_BIT_BICYCLE);
  constexpr uint64_t modifier_bits =
      GetBitMask(KEY_BIT_FORWARD) | GetBitMask(KEY_BIT_BACKWARD) |
      GetBitMask(KEY_BIT_BOTH_WAYS) | GetBitMask(KEY_BIT_LANES_INNER);

  for (const ParsedTag& pt : ptags) {
    if ((pt.bits & selector_bits) == 0) {
      continue;
    }

    switch (pt.bits & ~modifier_bits) {
      case GetBitMask(KEY_BIT_ACCESS):
      case GetBitMask(KEY_BIT_ACCESS, KEY_BIT_VEHICLE):
      case GetBitMask(KEY_BIT_ACCESS, KEY_BIT_BICYCLE):
      // Instead of access:bicycle=... one can say bicycle=...
      case GetBitMask(KEY_BIT_VEHICLE):
      case GetBitMask(KEY_BIT_BICYCLE): {
        const bool weak =
            (pt.bits & ~modifier_bits) == GetBitMask(KEY_BIT_ACCESS);
        SetAccess(pt, weak, tagh.ToString(pt.val_st_idx), ra_forw, ra_backw);
        break;
      }
      default:
        break;
    }
  }
}
