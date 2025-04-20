#pragma once

/*
 * Turn restriction data structures used in both Graph and CompactGraph.
 * Note that the node indexes and edge offsets are valid within the referenced
 * Graph/CompactGraph.
 */

#include <string_view>
#include <vector>

#include "absl/container/flat_hash_map.h"

enum class TurnDirection : std::uint8_t {
  LeftTurn = 0,
  RightTurn = 1,
  StraightOn = 2,
  UTurn = 3,
  NoEntry = 4,
  NoExit = 5,
};

// Turn restriction parsed from a relation. "no_entry/no_exit" restrictions with
// multiple from/to ways are split into multiple turn restrictions with only one
// from/to-way each.
struct TurnRestriction {
  std::int64_t relation_id = -1;
  std::int64_t from_way_id = -1;
  std::vector<std::int64_t> via_ids;
  std::int64_t to_way_id = -1;
  bool via_is_node : 1 = 0;  // via is a node (true) or way(s) (false).
  bool forbidden : 1 = 0;
  TurnDirection direction : 3 = TurnDirection::LeftTurn;

  // A list of (from_node,to_node,way) entries that describe the path of the
  // turn restriction from start to end. The to_node of an entry is equal to the
  // from node of the following entry.
  // Note that the first entry in the path is the last edge in the from way,
  // ending in the first via node. And the last entry in the path is the first
  // edge in the to way, starting at the last via node. If the list has only two
  // entries, then the path represents a simple turn restriction.
  struct TREdge {
    std::uint32_t from_node_idx;
    std::uint32_t way_idx;
    std::uint32_t to_node_idx;
    std::uint32_t edge_idx;

    bool operator==(const TREdge& other) const {
      return from_node_idx == other.from_node_idx && way_idx == other.way_idx &&
             to_node_idx == other.to_node_idx;
    }
    bool operator<(const TREdge& b) const {
      if (from_node_idx != b.from_node_idx) {
        return from_node_idx < b.from_node_idx;
      }
      if (way_idx != b.way_idx) {
        return way_idx < b.way_idx;
      }
      return to_node_idx < b.to_node_idx;
    }
  };
  uint32_t path_start_node_idx;  // The start node of the first edge in 'path'.
  std::vector<TREdge> path;

  // Get the key for the edge that triggers the start of the turn restriction.
  // This works for both simple and complex turn restrictions and is simply the
  // first edge in the path.
  const TREdge& GetTriggerKey() const {
    CHECK_GE_S(path.size(), 2);
    return path.front();
  }
};

struct SimpleTurnRestrictionData {
  // 32 bits that select the allowed outgoing edges at the via node.
  uint32_t allowed_edge_bits;
  int64_t osm_relation_id;
};

// Make sure there are no filler bytes, because we hash the whole thing.
static_assert(sizeof(TurnRestriction::TREdge) == 4 * sizeof(std::uint32_t));

namespace std {
template <>

struct hash<TurnRestriction::TREdge> {
  size_t operator()(const TurnRestriction::TREdge& key) const {
    // Use already defined std::string_view hash function.
    // Note: this only works as long as there are no filler bytes in the struct!

    return std::hash<std::string_view>{}(
        std::string_view((char*)&key, sizeof(3 * sizeof(std::uint32_t))));
  }
};
}  // namespace std

using SimpleTurnRestrictionMap =
    absl::flat_hash_map<TurnRestriction::TREdge, SimpleTurnRestrictionData>;

// Points to index of turn restriction in complex_turn_restriction vector.
using ComplexTurnRestrictionMap =
    absl::flat_hash_map<TurnRestriction::TREdge, uint32_t>;

// Sort the turn restrictions by the edge that triggers the
// restriction, i.e. the edge of the from way that connects to the via node.
inline void SortTurnRestrictions(std::vector<TurnRestriction>* trs) {
  std::sort(trs->begin(), trs->end(),
            [](const TurnRestriction& a, const TurnRestriction& b) {
              if (a.GetTriggerKey() != b.GetTriggerKey()) {
                return a.GetTriggerKey() < b.GetTriggerKey();
              }
              return a.path.back() < b.path.back();
            });
}

// Create a ComplexTurnRestrictionMap, which stores for every TriggerKey the
// first position in trs where that key occurs. Note that trs needs to be sorted
// by SortTurnRestrictions() and must contain complex turn  restrictions only.
inline ComplexTurnRestrictionMap ComputeComplexTurnRestrictionMap(
    Verbosity verbosity, const std::vector<TurnRestriction>& trs) {
  ComplexTurnRestrictionMap res;
  if (trs.empty()) {
    return res;
  }
  size_t start = 0;
  for (size_t i = 0; i < trs.size(); ++i) {
    CHECK_S(!trs.at(i).via_is_node);
    if (i == trs.size() - 1 ||
        trs.at(i).GetTriggerKey() != trs.at(i + 1).GetTriggerKey()) {
      res[trs.at(start).GetTriggerKey()] = start;
      start = i + 1;
    }
  }
  return res;
}
