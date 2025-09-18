#pragma once

#include "absl/strings/str_format.h"
#include "base/util.h"
#include "graph/routing_attrs.h"
#include "osm/key_bits.h"
#include "osm/osm_helpers.h"
#include "osm/parsed_tag.h"

namespace {
inline void SetSurface(const ParsedTag& pt, std::string_view tag_val,
                       RoutingAttrs* ra_forw, RoutingAttrs* ra_backw) {
  SURFACE surface = SURFACE_MAX;
  if (!pt.bits.test(KEY_BIT_LANES_INNER)) {
    surface = SurfaceToEnum(tag_val);
  } else {
    // Parse lanes and use best surface found.
    for (std::string_view v : absl::StrSplit(tag_val, '|')) {
      SURFACE x = SurfaceToEnum(v);
      if (x < surface) surface = x;
    }
  }
  if (!pt.bits.test(KEY_BIT_BACKWARD)) {
    ra_forw->surface = surface;
  }
  if (!pt.bits.test(KEY_BIT_FORWARD)) {
    ra_backw->surface = surface;
  }
}

inline void SetSmoothness(const ParsedTag& pt, std::string_view tag_val,
                          RoutingAttrs* ra_forw, RoutingAttrs* ra_backw) {
  SMOOTHNESS smoothness = SMOOTHNESS_MAX;
  if (!pt.bits.test(KEY_BIT_LANES_INNER)) {
    smoothness = SmoothnessToEnum(tag_val);
  } else {
    // Parse lanes and use best smoothness found.
    for (std::string_view v : absl::StrSplit(tag_val, '|')) {
      SMOOTHNESS x = SmoothnessToEnum(v);
      if (x < smoothness) smoothness = x;
    }
  }
  if (!pt.bits.test(KEY_BIT_BACKWARD)) {
    ra_forw->smoothness = smoothness;
  }
  if (!pt.bits.test(KEY_BIT_FORWARD)) {
    ra_backw->smoothness = smoothness;
  }
}

inline void SetTracktype(const ParsedTag& pt, std::string_view tag_val,
                         RoutingAttrs* ra_forw, RoutingAttrs* ra_backw) {
  TRACKTYPE tracktype = TracktypeToEnum(tag_val);
  ra_forw->tracktype = tracktype;
  ra_backw->tracktype = tracktype;
}
}  // namespace

// For cars, fill in surface, tracktype and smoothness attributes for both
// directions, starting with default values.
inline void CarRoadSurface(const OSMTagHelper& tagh, int64_t way_id,
                           const std::vector<ParsedTag>& ptags,
                           RoutingAttrs* ra_forw, RoutingAttrs* ra_backw) {
  // Tags selected from output of osmgrep with pattern on planet file.
  // -ways='^surface\b.*=&&!cycle&&!footway&&!check_date&&!sidewalk&&!grade
  //        &&!description&&!source&&!condition&&!material&&!middle&&!colour
  //        &&!:note&&!:de='
  // surface                  :     54916452 (id:205987637)
  // surface:lanes            :         1415 (id:768958881)
  // surface:lanes:forward    :          143 (id:86811699)
  // surface:lanes:backward   :          134 (id:1126042571)
  // surface:left             :          103 (id:841007497)
  // surface:right            :          103 (id:1047402016)
  //   left and right are ignored, for instance 841007497 doesn't say which
  //   surface is for which vehicle, so it seems to hard to interpret.
  // surface:backward         :          100 (id:632122409)
  // surface:forward          :           84 (id:852118701)
  //
  // Surface and smoothness look the same in terms of used combinations.
  // But tracktype isn't combined with other keys.

  constexpr KeySet selector_bits(
      {KEY_BIT_SURFACE, KEY_BIT_SMOOTHNESS, KEY_BIT_TRACKTYPE});

  constexpr KeySet modifier_bits(
      {KEY_BIT_LANES_INNER, KEY_BIT_FORWARD, KEY_BIT_BACKWARD});

  for (const ParsedTag& pt : ptags) {
    if (!BitsetsOverlap(pt.bits, selector_bits)) {
      continue;
    }
    if (!BitsetContainedIn(pt.bits,
                           BitsetUnion(selector_bits, modifier_bits))) {
      continue;
    }

    const std::string_view tag_val = tagh.ToString(pt.val_st_idx);

    if (pt.bits.test(KEY_BIT_SURFACE)) {
      SetSurface(pt, tag_val, ra_forw, ra_backw);
    } else if (pt.bits.test(KEY_BIT_SMOOTHNESS)) {
      SetSmoothness(pt, tag_val, ra_forw, ra_backw);
    } else {
      CHECK_S(pt.bits.test(KEY_BIT_TRACKTYPE));
      SetTracktype(pt, tag_val, ra_forw, ra_backw);
    }
  }
}

// For bicycles, fill in surface, tracktype and smoothness attributes for both
// directions, starting with default values.
inline void BicycleRoadSurface(const OSMTagHelper& tagh, int64_t way_id,
                               const std::vector<ParsedTag>& ptags,
                               RoutingAttrs* ra_forw, RoutingAttrs* ra_backw) {
  // Tags selected from output of osmgrep with pattern on planet file.
  // -ways='^surface\b.*=&&!cycle&&!footway&&!check_date&&!sidewalk&&!grade
  //        &&!description&&!source&&!condition&&!material&&!middle&&!colour
  //        &&!:note&&!:de='
  // surface                  :     54916452 (id:205987637)
  // surface:lanes            :         1415 (id:768958881)
  // surface:lanes:forward    :          143 (id:86811699)
  // surface:lanes:backward   :          134 (id:1126042571)
  // surface:left             :          103 (id:841007497)
  // surface:right            :          103 (id:1047402016)
  //   left and right are ignored, for instance 841007497 doesn't say which
  //   surface is for which vehicle, so it seems to hard to interpret.
  // surface:backward         :          100 (id:632122409)
  // surface:forward          :           84 (id:852118701)
  //
  // Surface and smoothness look the same in terms of used combinations.
  // But tracktype isn't combined with other keys.

  constexpr KeySet selector_bits(
      {KEY_BIT_SURFACE, KEY_BIT_SMOOTHNESS, KEY_BIT_TRACKTYPE});
  constexpr KeySet modifier_bits(
      {KEY_BIT_LANES_INNER, KEY_BIT_FORWARD, KEY_BIT_BACKWARD});

  for (const ParsedTag& pt : ptags) {
    if (!BitsetsOverlap(pt.bits, selector_bits)) {
      continue;
    }
    if (!BitsetContainedIn(pt.bits,
                           BitsetUnion(selector_bits, modifier_bits))) {
      continue;
    }

    const std::string_view tag_val = tagh.ToString(pt.val_st_idx);

    if (pt.bits.test(KEY_BIT_SURFACE)) {
      SetSurface(pt, tag_val, ra_forw, ra_backw);
    } else if (pt.bits.test(KEY_BIT_SMOOTHNESS)) {
      SetSmoothness(pt, tag_val, ra_forw, ra_backw);
    } else {
      CHECK_S(pt.bits.test(KEY_BIT_TRACKTYPE));
      SetTracktype(pt, tag_val, ra_forw, ra_backw);
    }
  }
}
