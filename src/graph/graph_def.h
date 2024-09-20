#pragma once

#include <vector>

#include "base/constants.h"
#include "base/huge_bitset.h"
#include "base/simple_mem_pool.h"
#include "base/util.h"
#include "base/varbyte.h"
#include "graph/routing_attrs.h"

enum class TurnMode : std::uint8_t {
  OnlyThis = 0,
  NotThis = 1,
};

enum class TurnDirection : std::uint8_t {
  LeftTurn = 0,
  RightTurn = 1,
  StraightOn = 2,
  UTurn = 3,
};

enum class Surface : std::uint8_t { Paved = 0, Max };

struct TurnRestriction {
  std::int64_t relation_id;
  std::int64_t from_way_id;
  std::int64_t to_way_id;
  std::vector<std::int64_t> via_ids;
  bool via_is_node : 1;  // via is a node (true) or a way (false).
  TurnMode mode : 1;
  TurnDirection direction : 2;
};

struct GWay {
  std::int64_t id : 40;
  HIGHWAY_LABEL highway_label : 5 = HW_MAX;

  // 'uniform_country' is 0 if more than one country_num exists for the nodes in
  // the way, 1 if all the nodes belong to the same country.
  std::uint16_t uniform_country : 1 = 0;
  // If uniform_country==1, then this value is the country of all the nodes.
  std::uint16_t ncc : 10 = INVALID_NCC;

  // Vehicle Type/Lane information here.
  // Directions directions : 2 = Directions::Both;
  DIRECTION dir_obsolete : 2 = DIR_BOTH;
  // std::uint32_t maxspeed : 10 = 0;  // 0..1023 km/h

  RoutingAttrs ri[2];  // Routing info in forward and backward direction.

  // TODO: encode streetname and node_ids into the same buffer.
  // 0-terminated street name. The memory is allocated in graph.unaligned_pool_.
  char* streetname;
  // Varbyte encoded node ids of the way. The memory is allocated in
  // graph.unaligned_pool_.
  uint8_t* node_ids;
};

inline bool RoutableAccess(ACCESS acc) {
  return acc >= ACC_CUSTOMERS && acc < ACC_MAX;
}

inline bool RoutableForward(const GWay& way) {
  return RoutableAccess(way.ri[0].access);
}

inline bool RoutableBackward(const GWay& way) {
  return RoutableAccess(way.ri[1].access);
}

constexpr std::uint32_t INVALID_CLUSTER_ID = (1 << 22) - 1;

struct GEdge;
struct GNode {
  std::int64_t node_id : 40;
  std::uint32_t num_edges_out : 11;
  std::uint32_t num_edges_in : 11;  // Is one byte actually enough?
  // This node is in a dead end, i.e. in a small subgraph that is connected
  // through a bridge edge to the rest of the graph. All routes to a
  // node outside of this dead end have to pass through the bridge edge.
  // Dead end subgraphs are small (<10k nodes or so) and help routing algorithms
  // because subgraphs behind bridges can be ignored unless from/to node are in
  // the dead end.
  std::uint32_t dead_end : 1;
  // 1 iff the node is in a large component of the graph.
  std::uint32_t large_component : 1;

  // Cluster id number. It is expected (and checked during construction)
  // that there are less than 2^22 clusters in the planet graph.
  std::uint32_t cluster_id : 22 = INVALID_CLUSTER_ID;
  // 1 iff the node connects to different clusters.
  std::uint32_t cluster_border_node : 1 = 0;

  std::int32_t lat = 0;
  std::int32_t lon = 0;
  GEdge* edges;  // Array of length 'num_edges_out + num_edges_in'.
};

inline size_t gnode_num_edges(const GNode& n) {
  return n.num_edges_out + n.num_edges_in;
}

struct GEdge {
  std::uint32_t other_node_idx;
  std::int32_t way_idx;
  std::uint64_t distance_cm : 40;
  // True iff this is the first time 'other_node_idx' has this value in the list
  // of edges of the node.
  // Can be used to selected edges for the undirected graph.
  std::uint64_t unique_other : 1;
  // This edge connects two components in the undirected graph. Removing it
  // creates two non-connected subgraphs. The nodes in the smaller of the two
  // subgraphs are all marked 'dead end'. The other component has 'dead_end' set
  // to 0 for all nodes.
  // Note: Only bridges connect dead-end with non-dead-end nodes.
  std::uint64_t bridge : 1;
  // 1 if the edge goes against the direction of the way, 0 if it goes in the
  // way's direction.
  // Important for example when selecting forward/backward speed.
  std::uint64_t contra_way : 1;
  // 1 iff edge connects two points in different countries, 0 if both points
  // belong to the same country.
  std::uint64_t cross_country : 1;
  // 1 iff the edge connects two nodes from different clusters.
  std::uint64_t cross_cluster : 1;
};

// Contains the list of border nodes and some metadata for a cluster.
struct GCluster {
  std::uint32_t cluster_id = 0;
  std::uint32_t num_nodes = 0;
  std::uint32_t num_border_nodes = 0;
  // Each edge is either 'inner', 'outer' (connects to another cluster) or a
  // bridge.
  std::uint32_t num_inner_edges = 0;
  std::uint32_t num_outer_edges = 0;
  std::uint32_t num_bridges = 0;
  // Sorted vector containing the border node indexes (pointing into
  // Graph::nodes). Sorted to improve data locality.
  std::vector<std::uint32_t> border_nodes;
  // For each border node, list distances to all other border nodes.
  std::vector<std::vector<std::uint32_t>> distances;
};

struct Graph {
  struct Component {
    uint32_t start_node;
    uint32_t size;
  };

  static constexpr uint32_t kLargeComponentMinSize = 10000;

  std::vector<GWay> ways;
  std::vector<GNode> nodes;
  // Large components, sorted by decreasing size.
  std::vector<Component> large_components;
  std::vector<GCluster> clusters;
  SimpleMemPool aligned_pool_;
  SimpleMemPool unaligned_pool_;

  std::size_t FindWayIndex(std::int64_t way_id) const {
    auto it = std::lower_bound(
        ways.begin(), ways.end(), way_id,
        [](const GWay& s, std::int64_t value) { return s.id < value; });
    if (it == ways.end() || it->id != way_id) {
      return ways.size();
    } else {
      return it - ways.begin();
    }
  }

  const GWay* FindWay(std::int64_t way_id) const {
    size_t idx = FindWayIndex(way_id);
    if (idx >= ways.size()) {
      return nullptr;
    }
    return &(ways.at(idx));
  }

  std::size_t FindNodeIndex(std::int64_t node_id) const {
    auto it = std::lower_bound(
        nodes.begin(), nodes.end(), node_id,
        [](const GNode& s, std::int64_t value) { return s.node_id < value; });
    if (it == nodes.end() || it->node_id != node_id) {
      return nodes.size();
    } else {
      return it - nodes.begin();
    }
  }

  static std::vector<uint64_t> GetGWayNodeIds(const GWay& way) {
    // Decode node_ids.
    std::uint64_t num_nodes;
    std::uint8_t* ptr = way.node_ids;
    ptr += DecodeUInt(ptr, &num_nodes);
    std::vector<uint64_t> ids;
    ptr += DecodeNodeIds(ptr, num_nodes, &ids);
    return ids;
  }

  // Given a way with the original node id list, return the list of node indexes
  // (in graph.nodes) of all graph nodes.
  // Note that this ignores nodes that don't exist or that are not relevant for
  // routing.
  std::vector<size_t> GetGWayNodeIndexes(const HugeBitset& graph_node_ids,
                                         const GWay& way) const {
    // Find nodes
    std::vector<size_t> node_idx;
    for (const uint64_t id : GetGWayNodeIds(way)) {
      if (graph_node_ids.GetBit(id)) {
        std::size_t idx = FindNodeIndex(id);
        CHECK_S(idx >= 0 && idx < nodes.size());
        node_idx.push_back(idx);
      }
    }
    return node_idx;
  }
};
