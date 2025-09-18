#pragma once

#include <bit>
#include <string_view>
#include <vector>

#include "osm/key_bits.h"
#include "osm/osm_helpers.h"

struct ParsedTag {
  // A bit set representing the components in the parsed key.
  // See osm/key_bits.h.
  KeySet bits;
  // String value, given as index into the osm string table.
  uint32_t val_st_idx;
  // The bit of the first partial key, i.e. KEY_BIT_ONEWAY for 'oneway:psv=yes'
  uint8_t first;
};

class ParsedTagInfo {
 public:
  ParsedTagInfo() = delete;
  ParsedTagInfo(const OSMTagHelper& tagh, const std::vector<ParsedTag>& tags)
      : tagh_(tagh), tags_(tags), collected_bits_(CollectBits(tags)) {}

  const OSMTagHelper& tagh() const { return tagh_; }
  const std::vector<ParsedTag>& tags() const { return tags_; }
  const KeySet CollectedBits() const { return collected_bits_; };

  bool HasAll(KeySet ks) const { return (ks & collected_bits_) == ks; }
  bool HasAny(KeySet ks) const { return (ks & collected_bits_).any(); }
  bool HasNone(KeySet ks) const { return (ks & collected_bits_).none(); }

  const ParsedTag* Find(KeySet ks) const {
    if (!HasAll(ks)) return nullptr;
    for (const auto& pt : tags_) {
      if (pt.bits == ks) return &pt;
    }
    return nullptr;
  }

  const std::string_view FindValue(KeySet ks) const {
    if (HasAll(ks)) {
      for (const auto& pt : tags_) {
        if (pt.bits == ks) return tagh_.ToString(pt.val_st_idx);
      }
    }
    return "";
  }

 private:
  static KeySet CollectBits(const std::vector<ParsedTag>& tags) {
    KeySet ks;
    for (const auto& pt : tags) {
      ks |= pt.bits;
    }
    return ks;
  }

  const OSMTagHelper& tagh_;
  const std::vector<ParsedTag> tags_;
  // All bits that are set anywhere in 'tags_'.
  const KeySet collected_bits_;
};

namespace {
// Compute a selectivity value that is lower for broad keys (such as
// "oneway") and higher for more specific keys (such as
// "oneway:bicycle"). Executing modifications on data in increasing
// selectivity order properly overwrites broad values with more specific values.
uint32_t ComputeTagSelectivity(KeySet bitset) {
  uint32_t selectivity = 0;
  // Cycleway is treated the same as a vehicle type for priority.
  if (BitsetsOverlap(bitset, BITSET_MODIFIERS)) {
    selectivity += 10;
  }
  if (BitsetsOverlap(bitset, BITSET_LANES_INNER)) {
    selectivity += 100;
  }
  if (bitset.test(KEY_BIT_VEHICLE)) {
    selectivity += 1000;
  }
  if (BitsetsOverlap(bitset,
                     KeySet({KEY_BIT_MOTOR_VEHICLE, KEY_BIT_PSV}))) {
    selectivity += 10000;
  }
  if (BitsetsOverlap(bitset, BITSET_VEHICLES)) {
    selectivity += 100000;
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

// Templated, to make it work for nodes, ways and relations.
// Note that for nodes, OsmPbfReader::NodeWithTags has to be used.
template <class OSM_OBJ>
inline ParsedTagInfo ParseTags(const OSMTagHelper& tagh,
                               const OSM_OBJ& osm_obj) {
  std::vector<ParsedTag> ptags;
  for (size_t pos = 0; pos < static_cast<size_t>(osm_obj.keys().size());
       ++pos) {
    std::string_view key = tagh.ToString(osm_obj.keys().at(pos));
    KeySet bits;
    uint8_t first = KEY_BIT_MAX;
    // TODO: force string_view vector.
    for (std::string_view part : absl::StrSplit(key, ':')) {
      int b = GetKeyPartBitFast(part);
      if (b >= 0) {
        if (bits.none()) {
          // First key part.
          first = b;
        } else {
          // Non-first key part.
          if (b == KEY_BIT_LANES) {
            b = KEY_BIT_LANES_INNER;
          }
        }
        bits.set(static_cast<uint8_t>(b));
      } else {
        bits.clear();
        break;
      }
    }
    if (bits.any()) {
      CHECK_NE_S(first, KEY_BIT_MAX) << "< " << key << ">";
      ptags.push_back(
          {.bits = bits,
           .val_st_idx = static_cast<uint32_t>(osm_obj.vals().at(pos)),
           .first = first});
    }
  }
  SortParsedTagsBySelectivity(ptags);
  return ParsedTagInfo(tagh, ptags);
}
