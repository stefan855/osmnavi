#pragma once

#include "absl/strings/str_format.h"
#include "base/util.h"
#include "graph/routing_attrs.h"
#include "osm/key_bits.h"
#include "osm/osm_helpers.h"
#include "osm/parsed_tag.h"

namespace {

inline ACCESS ExtendedAccessToEnum(std::string_view val, bool bicycle) {
  ACCESS acc = AccessToEnum(val);  // Returns ACC_MAX for empty strings.
  if (acc == ACC_MAX && bicycle && val == "use_sidepath") {
    // Special case, bicycle should use separate bicycle path.
    acc = ACC_NO;
  }
  return acc;
}

struct InterpretAccessResult {
  ACCESS acc;
  bool improve_only;
};

inline InterpretAccessResult InterpretAccessValue(std::string_view val,
                                                  bool lanes, bool bicycle) {
  if (!lanes) {
    return {.acc = ExtendedAccessToEnum(val, bicycle),
            .improve_only = val.empty()};
  }
  // When there are lanes, we have to be careful to distinguish between empty
  // and unknown values. Empty values mean: leave unchanged, unknown values
  // should be interpreted as "no".
  ACCESS acc = ACC_MAX;
  bool has_empty = false;
  for (std::string_view sub : absl::StrSplit(val, '|')) {
    has_empty |= sub.empty();
    ACCESS sub_acc = ExtendedAccessToEnum(sub, bicycle);
    // Only replace if value is "better".
    if (sub_acc != ACC_MAX && (acc == ACC_MAX || sub_acc > acc)) {
      acc = sub_acc;
    }
  }
  return {.acc = acc, .improve_only = has_empty};
}

// Set access in forward and backward direction based on tags.
// 'weak' indicates that only access that already is better than 'no' should be
// modified. For instance, a way with "highway=footway access=permissive" should
// not be set permissive for cars.
inline void SetAccess(const ParsedTag& pt, bool weak, std::string_view value,
                      RoutingAttrs* ra_forw, RoutingAttrs* ra_backw) {
  InterpretAccessResult res = InterpretAccessValue(
      value, pt.bits.test(KEY_BIT_LANES_INNER), pt.first == KEY_BIT_BICYCLE);
  if (res.acc == ACC_MAX) {
    // In general, if we don't know the access value, then it has to be
    // interpreted as a 'no'.
    // For instance, 'agricultural' or 'forestry' will go here.
    res.acc = ACC_NO;
  }

  const bool forward = pt.bits.test(KEY_BIT_FORWARD);
  const bool backward = pt.bits.test(KEY_BIT_BACKWARD);
  // If forward/backward are missing, then set both.
  if (forward || !backward) {
    if (!weak || ra_forw->access != ACC_NO) {
      if (!res.improve_only || res.acc > ra_forw->access) {
        ra_forw->access = res.acc;
      }
    }
  }
  if (backward || !forward) {
    if (!weak || ra_backw->access != ACC_NO) {
      if (!res.improve_only || res.acc > ra_backw->access) {
        ra_backw->access = res.acc;
      }
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
  constexpr KeySet selector_bits =
      KeySet({KEY_BIT_ACCESS, KEY_BIT_VEHICLE, KEY_BIT_MOTOR_VEHICLE,
              KEY_BIT_MOTORCAR});
  // ":both_ways" is not used here. It means a lane that is for both directions,
  // not both ":forward" and ":backward" for the road. See
  // https://wiki.openstreetmap.org/wiki/Forward_%26_backward,_left_%26_right
  constexpr KeySet modifier_bits =
      KeySet({KEY_BIT_FORWARD, KEY_BIT_BACKWARD, KEY_BIT_LANES_INNER});

  for (const ParsedTag& pt : ptags) {
    if ((pt.bits & selector_bits).none()) {
      continue;
    }

    const KeySet ks = pt.bits & ~modifier_bits;
    if (ks == KeySet({KEY_BIT_ACCESS}) ||
        ks == KeySet({KEY_BIT_ACCESS, KEY_BIT_VEHICLE}) ||
        ks == KeySet({KEY_BIT_ACCESS, KEY_BIT_MOTOR_VEHICLE}) ||
        ks == KeySet({KEY_BIT_ACCESS, KEY_BIT_MOTORCAR}) ||
        // Instead of access:motorcar=... one can say motorcar=...
        ks == KeySet({KEY_BIT_VEHICLE}) ||
        ks == KeySet({KEY_BIT_MOTOR_VEHICLE}) ||
        ks == KeySet({KEY_BIT_MOTORCAR})) {
      const bool weak = (ks == KeySet({KEY_BIT_ACCESS}));
      SetAccess(pt, weak, tagh.ToString(pt.val_st_idx), ra_forw, ra_backw);
    }
#if 0
    switch (pt.bits & ~modifier_bits) {
      case KeySet({KEY_BIT_ACCESS}):
      case KeySet({KEY_BIT_ACCESS, KEY_BIT_VEHICLE}):
      case KeySet({KEY_BIT_ACCESS, KEY_BIT_MOTOR_VEHICLE}):
      case KeySet({KEY_BIT_ACCESS, KEY_BIT_MOTORCAR}):
      // Instead of access:motorcar=... one can say motorcar=...
      case KeySet({KEY_BIT_VEHICLE}):
      case KeySet({KEY_BIT_MOTOR_VEHICLE)}:
      case KeySet({KEY_BIT_MOTORCAR}): {
        const bool weak =
            (pt.bits & ~modifier_bits) == KeySet({KEY_BIT_ACCESS});
        SetAccess(pt, weak, tagh.ToString(pt.val_st_idx), ra_forw, ra_backw);

        break;
      }
      default:
        break;
    }
#endif
  }
}

inline void BicycleAccess(const OSMTagHelper& tagh, std::int64_t way_id,
                          const std::vector<ParsedTag>& ptags,
                          RoutingAttrs* ra_forw, RoutingAttrs* ra_backw) {
  constexpr KeySet selector_bits =
      KeySet({KEY_BIT_ACCESS, KEY_BIT_VEHICLE, KEY_BIT_BICYCLE});
  constexpr KeySet modifier_bits =
      KeySet({KEY_BIT_FORWARD, KEY_BIT_BACKWARD, KEY_BIT_BOTH_WAYS,
              KEY_BIT_LANES_INNER});

  for (const ParsedTag& pt : ptags) {
    if ((pt.bits & selector_bits).none()) {
      continue;
    }

    const KeySet ks = pt.bits & ~modifier_bits;
    if (ks == KeySet({KEY_BIT_ACCESS}) ||
        ks == KeySet({KEY_BIT_ACCESS, KEY_BIT_VEHICLE}) ||
        ks == KeySet({KEY_BIT_ACCESS, KEY_BIT_BICYCLE}) ||
        // Instead of access:bicycle=... one can say bicycle=...
        ks == KeySet({KEY_BIT_VEHICLE}) || ks == KeySet({KEY_BIT_BICYCLE})) {
      const bool weak = (ks == KeySet({KEY_BIT_ACCESS}));
      SetAccess(pt, weak, tagh.ToString(pt.val_st_idx), ra_forw, ra_backw);
    }

#if 0
    switch (pt.bits & ~modifier_bits) {
      case KeySet({KEY_BIT_ACCESS}):
      case KeySet({KEY_BIT_ACCESS, KEY_BIT_VEHICLE}):
      case KeySet({KEY_BIT_ACCESS, KEY_BIT_BICYCLE}):
      // Instead of access:bicycle=... one can say bicycle=...
      case KeySet({KEY_BIT_VEHICLE}):
      case KeySet({KEY_BIT_BICYCLE}): {
        const bool weak =
            (pt.bits & ~modifier_bits) == KeySet({KEY_BIT_ACCESS});
        SetAccess(pt, weak, tagh.ToString(pt.val_st_idx), ra_forw, ra_backw);
        break;
      }
      default:
        break;
    }
#endif
  }
}
