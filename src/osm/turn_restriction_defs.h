#pragma once

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
  struct NodePathEntry {
    std::uint32_t from_node_idx;
    std::uint32_t to_node_idx;
    std::uint32_t way_idx;
  };
  std::vector<NodePathEntry> node_path;
};

struct CondensedTurnRestrictionKey {
  uint32_t from_node_idx;
  uint32_t from_way_idx;
  uint32_t via_node_idx;
  bool operator==(const CondensedTurnRestrictionKey& other) const {
    return from_node_idx == other.from_node_idx &&
           from_way_idx == other.from_way_idx &&
           via_node_idx == other.via_node_idx;
  }
};

struct CondensedTurnRestrictionData {
  // 32 bits that select the allowed outgoing edges at the via node.
  uint32_t allowed_edge_bits;
  int64_t osm_relation_id;
};

namespace std {
template <>
struct hash<CondensedTurnRestrictionKey> {
  size_t operator()(const CondensedTurnRestrictionKey& key) const {
    // Use already defined std::string_view hash function.
    // Note: this only works as long as there are no filler bytes in the struct!
    return std::hash<std::string_view>{}(
        std::string_view((char*)&key, sizeof(key)));
  }
};
}  // namespace std

using CondensedTurnRestrictionMap =
    absl::flat_hash_map<CondensedTurnRestrictionKey,
                        CondensedTurnRestrictionData>;
