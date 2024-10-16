#pragma once

#include <bit>
#include <vector>

#include "osm/key_bits.h"
#include "osm/osm_helpers.h"

struct ParsedTag {
  // A bit set representing the components in the parsed key.
  // See osm/key_bits.h.
  uint64_t bits;
  // String value, given as index into the osm string table.
  uint32_t val_st_idx;
  // The bit of the first partial key, i.e. KEY_BIT_ONEWAY for 'oneway:psv=yes'
  uint8_t first;
};

namespace {
// Compute a selectivity value that is lower for broad keys (such as
// "oneway") and higher for more specific tags (such as
// "oneway:bicycle"). Executing modifications on data in increasing
// selectivity order properly overwrites broad values with more specific values.
uint32_t ComputeTagSelectivity(uint64_t bitset) {
  uint32_t selectivity = 0;
  // Cycleway is treated the same as a vehicle type for priority.
  if (bitset & BITSET_MODIFIERS) {
    selectivity += 1;
  }
  if (bitset & BITSET_LANES_INNER) {
    selectivity += 10;
  }
  if (bitset & GetBitMask(KEY_BIT_VEHICLE)) {
    selectivity += 100;
  }
  if (bitset & (GetBitMask(KEY_BIT_MOTOR_VEHICLE) | GetBitMask(KEY_BIT_PSV))) {
    selectivity += 1000;
  }
  if (bitset & (BITSET_VEHICLES | GetBitMask(KEY_BIT_CYCLEWAY))) {
    selectivity += 10000;
  }
  return selectivity;
}

// Sort tags by increasing selectivity value. After this, broad tags (such as
// "oneway=yes") are sorted before more selective tags (such as
// "oneway:bicycle=no"). The sort is stable, i.e. order doesn't change for tags
// with the same priority.
void SortParsedTagsBySelectivity(std::vector<ParsedTag>& tags) {
  std::stable_sort(
      tags.begin(), tags.end(), [](const ParsedTag& a, const ParsedTag& b) {
        return ComputeTagSelectivity(a.bits) < ComputeTagSelectivity(b.bits);
      });
}
}  // namespace

inline std::vector<ParsedTag> ParseTags(const OSMTagHelper& tagh,
                                        const OSMPBF::Way& osm_way) {
  std::vector<ParsedTag> ptags;
  for (int pos = 0; pos < osm_way.keys().size(); ++pos) {
    std::string_view key = tagh.ToString(osm_way.keys().at(pos));
    uint64_t bits = 0;
    uint8_t first = KEY_BIT_MAX;
    // TODO: force string_view vector.
    for (std::string_view part : absl::StrSplit(key, ':')) {
      int b = GetKeyPartBitFast(part);
      if (b >= 0) {
        if (bits == 0) {
          // First key part.
          first = b;
        } else {
          // Non-first key part.
          if (b == KEY_BIT_LANES) {
            b = KEY_BIT_LANES_INNER;
          }
        }
        bits |= GetBitMask(static_cast<uint8_t>(b));
      } else {
        bits = 0;
        break;
      }
    }
    if (bits != 0) {
      CHECK_NE_S(first, KEY_BIT_MAX) << "< " << key << ">";
      ptags.push_back(
          {.bits = bits,
           .val_st_idx = static_cast<uint32_t>(osm_way.vals().at(pos)),
           .first = first});
    }
  }
  SortParsedTagsBySelectivity(ptags);
  return ptags;
}
