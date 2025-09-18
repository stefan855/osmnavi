#pragma once

#include "absl/strings/str_format.h"
#include "base/util.h"
#include "graph/routing_attrs.h"
#include "osm/key_bits.h"
#include "osm/osm_helpers.h"
#include "osm/parsed_tag.h"

namespace {
// Return a valid maxspeed in the range [0..INFINITE_MAXSPEED], or -1 in case
// there was an error.
int GetSingleMaxspeed(std::string_view val) {
  if (val.empty()) {
    return -1;
  }
  uint16_t maxspeed = 0;
  if (!ParseNumericMaxspeed(val, &maxspeed)) {
    return -1;
  }
  return maxspeed;
}

// Return maxspeed or -1 if it couldn't be extracted.
int InterpretMaxspeedValue(std::string_view val, bool lanes) {
  int maxspeed = -1;
  if (!lanes) {
    maxspeed = GetSingleMaxspeed(val);
  } else {
    // Get the maximum from all the lanes, assuming that the car can use this
    // lane.
    for (std::string_view sub : absl::StrSplit(val, '|')) {
      int sub_maxspeed = GetSingleMaxspeed(sub);
      // Only replace if value is "better".
      if (sub_maxspeed > maxspeed) {
        maxspeed = sub_maxspeed;
      }
    }
  }
  return maxspeed;
}
}  // namespace

inline void CarMaxspeedFromWay(const OSMTagHelper& tagh, std::int64_t way_id,
                               const std::vector<ParsedTag>& ptags,
                               std::uint16_t* maxspeed_forw,
                               std::uint16_t* maxspeed_backw) {
  // Special (hard) cases:
  constexpr KeySet selector_bits = KeySet({KEY_BIT_MAXSPEED});
  // ":both_ways" is not used here. It means a lane that is for both directions,
  // not both ":forward" and ":backward" for the road. See
  // https://wiki.openstreetmap.org/wiki/Forward_%26_backward,_left_%26_right
  constexpr KeySet modifier_bits =
      KeySet({KEY_BIT_FORWARD, KEY_BIT_BACKWARD, KEY_BIT_LANES_INNER,
              KEY_BIT_VEHICLE, KEY_BIT_MOTOR_VEHICLE, KEY_BIT_MOTORCAR});
  *maxspeed_forw = 0;
  *maxspeed_backw = 0;

  for (const ParsedTag& pt : ptags) {
    if ((pt.bits & selector_bits).none()) {
      continue;
    }

    if ((pt.bits & ~modifier_bits) == KeySet({KEY_BIT_MAXSPEED})) {
      std::string_view val = tagh.ToString(pt.val_st_idx);
      int maxspeed =
          InterpretMaxspeedValue(val, pt.bits.test(KEY_BIT_LANES_INNER));
      if (maxspeed > 0 && maxspeed <= INFINITE_MAXSPEED) {
        if (pt.bits.test(KEY_BIT_FORWARD)) {
          *maxspeed_forw = maxspeed;
        } else if (pt.bits.test(KEY_BIT_BACKWARD)) {
          *maxspeed_backw = maxspeed;
        } else {
          // If forward/backward are missing, then set both.
          *maxspeed_forw = maxspeed;
          *maxspeed_backw = maxspeed;
        }
      }
    }

#if 0
    switch (pt.bits & ~modifier_bits) {
      case KeySet({KEY_BIT_MAXSPEED}): {
        std::string_view val = tagh.ToString(pt.val_st_idx);
        int maxspeed = InterpretMaxspeedValue(
            val, BitIsContained(KEY_BIT_LANES_INNER, pt.bits));
        if (maxspeed > 0 && maxspeed <= INFINITE_MAXSPEED) {
          if (BitIsContained(KEY_BIT_FORWARD, pt.bits)) {
            *maxspeed_forw = maxspeed;
          } else if (BitIsContained(KEY_BIT_BACKWARD, pt.bits)) {
            *maxspeed_backw = maxspeed;
          } else {
            // If forward/backward are missing, then set both.
            *maxspeed_forw = maxspeed;
            *maxspeed_backw = maxspeed;
          }
        }
        break;
      }
      default:
        break;
    }
#endif
  }
}

inline void BicycleMaxspeedFromWay(const OSMTagHelper& tagh,
                                   std::int64_t way_id,
                                   const std::vector<ParsedTag>& ptags,
                                   std::uint16_t* maxspeed_forw,
                                   std::uint16_t* maxspeed_backw) {
  // Special (hard) cases:
  constexpr KeySet selector_bits = KeySet({KEY_BIT_MAXSPEED});
  constexpr KeySet modifier_bits =
      KeySet({KEY_BIT_FORWARD, KEY_BIT_BACKWARD, KEY_BIT_LANES_INNER,
              KEY_BIT_VEHICLE, KEY_BIT_BICYCLE});
  *maxspeed_forw = 0;
  *maxspeed_backw = 0;

  for (const ParsedTag& pt : ptags) {
    if ((pt.bits & selector_bits).none()) {
      continue;
    }

    if ((pt.bits & ~modifier_bits) == KeySet({KEY_BIT_MAXSPEED})) {
      std::string_view val = tagh.ToString(pt.val_st_idx);
      int maxspeed =
          InterpretMaxspeedValue(val, pt.bits.test(KEY_BIT_LANES_INNER));
      if (maxspeed > 0 && maxspeed <= INFINITE_MAXSPEED) {
        if (pt.bits.test(KEY_BIT_FORWARD)) {
          *maxspeed_forw = maxspeed;
        } else if (pt.bits.test(KEY_BIT_BACKWARD)) {
          *maxspeed_backw = maxspeed;
        } else {
          // forward/backward are missing, either nothing or both_ways is
          // specified, so set both.
          *maxspeed_forw = maxspeed;
          *maxspeed_backw = maxspeed;
        }
      }
    }

#if 0
    switch (pt.bits & ~modifier_bits) {
      case KeySet({KEY_BIT_MAXSPEED}): {
        std::string_view val = tagh.ToString(pt.val_st_idx);
        int maxspeed = InterpretMaxspeedValue(
            val, BitIsContained(KEY_BIT_LANES_INNER, pt.bits));
        if (maxspeed > 0 && maxspeed <= INFINITE_MAXSPEED) {
          if (BitIsContained(KEY_BIT_FORWARD, pt.bits)) {
            *maxspeed_forw = maxspeed;
          } else if (BitIsContained(KEY_BIT_BACKWARD, pt.bits)) {
            *maxspeed_backw = maxspeed;
          } else {
            // forward/backward are missing, either nothing or both_ways is
            // specified, so set both.
            *maxspeed_forw = maxspeed;
            *maxspeed_backw = maxspeed;
          }
          break;
        }
      }
      default:
        break;
    }
#endif
  }
}
