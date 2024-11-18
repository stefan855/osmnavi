#pragma once

#include "absl/strings/str_format.h"
#include "base/util.h"
#include "graph/routing_attrs.h"
#include "osm/key_bits.h"
#include "osm/osm_helpers.h"
#include "osm/parsed_tag.h"

namespace {
inline void SetSurface(const ParsedTag& pt, std::string_view value,
                       RoutingAttrs* ra_forw, RoutingAttrs* ra_backw) {
  auto surface = SurfaceToEnum(value);
  ra_forw->surface = surface;
  ra_backw->surface = surface;
}

inline void SetTracktype(const ParsedTag& pt, std::string_view value,
                         RoutingAttrs* ra_forw, RoutingAttrs* ra_backw) {
  auto tracktype = TracktypeToEnum(value);
  ra_forw->tracktype = tracktype;
  ra_backw->tracktype = tracktype;
}

inline void SetSmoothness(const ParsedTag& pt, std::string_view value,
                          RoutingAttrs* ra_forw, RoutingAttrs* ra_backw) {
  auto smoothness = SmoothnessToEnum(value);
  ra_forw->smoothness = smoothness;
  ra_backw->smoothness = smoothness;
}

}  // namespace

// Fill in surface, tracktype and smoothness attributes for both directions,
// starting with default values.
inline void CarRoadSurface(const OSMTagHelper& tagh, int64_t way_id,
                           const std::vector<ParsedTag>& ptags,
                           RoutingAttrs* ra_forw, RoutingAttrs* ra_backw) {
  // surface=bla
  // cycleway:surface
  // footway:surface
  // sidewalk:surface
  // sidewalk:both:surface
  // sidewalk:right:surface
  // cycleway:right:surface
  // surface:lanes=bla1|bla2|bla3.
  //
  // And hope for something like "access:lanes:backward=yes|no|no"
  // Special (hard) cases:

  /*
  constexpr uint64_t modifier_bits =
      GetBitMask(KEY_BIT_BOTH) | GetBitMask(KEY_BIT_LEFT) |
      GetBitMask(KEY_BIT_RIGHT) | GetBitMask(KEY_BIT_FOOTWAY) |
      GetBitMask(KEY_BIT_CYCLEWAY) | GetBitMask(KEY_BIT_SIDEWALK);
  */

  constexpr uint64_t modifier_bits = GetBitMask(KEY_BIT_BOTH) |
                                     GetBitMask(KEY_BIT_LEFT) |
                                     GetBitMask(KEY_BIT_RIGHT);

  for (const ParsedTag& pt : ptags) {
    if ((pt.bits &
         (GetBitMask(KEY_BIT_SURFACE) | GetBitMask(KEY_BIT_TRACKTYPE) |
          GetBitMask(KEY_BIT_SMOOTHNESS))) == 0) {
      continue;
    }

    switch (pt.bits) {
      case GetBitMask(KEY_BIT_SURFACE): {
        SetSurface(pt, tagh.ToString(pt.val_st_idx), ra_forw, ra_backw);
        break;
      }
      case GetBitMask(KEY_BIT_TRACKTYPE): {
        SetTracktype(pt, tagh.ToString(pt.val_st_idx), ra_forw, ra_backw);
        break;
      }
      case GetBitMask(KEY_BIT_SMOOTHNESS): {
        SetSmoothness(pt, tagh.ToString(pt.val_st_idx), ra_forw, ra_backw);
        break;
      }
      default:
        break;
    }
  }
}
