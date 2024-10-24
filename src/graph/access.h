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

inline void SetAccess(const ParsedTag& pt, std::string_view value,
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
  if (BitIsContained(KEY_BIT_FORWARD, pt.bits)) {
    ra_forw->access = acc;
  } else if (BitIsContained(KEY_BIT_BACKWARD, pt.bits)) {
    ra_backw->access = acc;
  } else {
    // forward/backward are missing, either nothing or both_ways is
    // specified, so set both.
    ra_forw->access = acc;
    ra_backw->access = acc;
  }
}
}  // namespace

inline void CarAccess(const OSMTagHelper& tagh, std::int64_t way_id,
                      const std::vector<ParsedTag>& ptags,
                      RoutingAttrs* ra_forw, RoutingAttrs* ra_backw) {
  // Special (hard) cases:
  // 1) access:lanes:backward=motorcar;motorcycle|hgv (TODO!)
  // 2) lanes:motor_vehicle=yes|no|yes
  // 3) access:both_ways=no ::
  //    access:lanes:backward=yes|no ::
  //    access:lanes:forward=yes|no ::
  constexpr uint64_t selector_bits =
      GetBitMask(KEY_BIT_ACCESS) | GetBitMask(KEY_BIT_VEHICLE) |
      GetBitMask(KEY_BIT_MOTOR_VEHICLE) | GetBitMask(KEY_BIT_MOTORCAR);
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
      case GetBitMask(KEY_BIT_ACCESS, KEY_BIT_MOTOR_VEHICLE):
      case GetBitMask(KEY_BIT_ACCESS, KEY_BIT_MOTORCAR):
      // Instead of access:motorcar=... one can say motorcar=...
      case GetBitMask(KEY_BIT_VEHICLE):
      case GetBitMask(KEY_BIT_MOTOR_VEHICLE):
      case GetBitMask(KEY_BIT_MOTORCAR): {
        std::string_view val = tagh.ToString(pt.val_st_idx);
        SetAccess(pt, tagh.ToString(pt.val_st_idx), ra_forw, ra_backw);
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
        std::string_view val = tagh.ToString(pt.val_st_idx);
        SetAccess(pt, tagh.ToString(pt.val_st_idx), ra_forw, ra_backw);
        if (way_id == 35433970 &&
            (pt.bits & ~modifier_bits) == GetBitMask(KEY_BIT_BICYCLE)) {
          LOG_S(INFO) << "BlaBla:" << way_id << " " << ra_forw->access;
        }
        break;
      }
      default:
        break;
    }
  }
}
