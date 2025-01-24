#pragma once

#include <type_traits>
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

// WaySharedAttrs (=WSA) contains many of the way attributes in a
// data structure that is shared between ways. The shared data is stored in
// Graph::way_shared_attrs vector and accessed through GWay::wsa_id.
// Used to decrease the storage needed to store ways, both in ram and on disk.
// Also see base/deduper_with_ids.h.
struct WaySharedAttrs {
  // Vehicles types used in the ra array.
  static constexpr VEHICLE RA_VEHICLES[] = {VH_MOTOR_VEHICLE, VH_BICYCLE,
                                            VH_FOOT};
  static constexpr uint32_t RA_MAX =
      2 * sizeof(RA_VEHICLES) / sizeof(RA_VEHICLES[0]);
  // Routing info in forward and backward direction.
  RoutingAttrs ra[RA_MAX];
};

// Check that WaySharedAttrs is POD. Note the struct has to be completely zeroed
// when creating one, because bot the equal operator and hash function below
// assume there are no unset areas in the data structure.
static_assert(std::is_standard_layout_v<WaySharedAttrs> == true);
static_assert(std::is_trivial_v<WaySharedAttrs> == true);

inline bool operator==(const WaySharedAttrs& a, const WaySharedAttrs& b) {
  return memcmp(&a, &b, sizeof(WaySharedAttrs)) == 0;
}

namespace std {
template <>
struct hash<WaySharedAttrs> {
  size_t operator()(const WaySharedAttrs& wsa) const {
    // Use already defined std::string_view hashes.
    return std::hash<std::string_view>{}(
        std::string_view((char*)&wsa, sizeof(wsa)));
  }
};
}  // namespace std

struct GWay {
  std::int64_t id : 40;
  HIGHWAY_LABEL highway_label : 5 = HW_MAX;

  // 'uniform_country' is 0 if more than one country_num exists for the nodes in
  // the way, 1 if all the nodes belong to the same country.
  std::uint16_t uniform_country : 1 = 0;
  // Country of the first node in the way.
  // If uniform_country==1, then this value is the country of all the nodes.
  std::uint16_t ncc : 10 = INVALID_NCC;

  // position of the WaySharedAttrs data in vector g.way_shared_attrs.
  std::uint32_t wsa_id = INFU32;

  // TODO: encode streetname and node_ids into the same buffer, such that we
  // need only one pointer here.

  // 0-terminated street name. The memory is
  // allocated in graph.unaligned_pool_.
  char* streetname = nullptr;
  // Varbyte encoded node ids of the way. The memory is allocated in
  // graph.unaligned_pool_.
  uint8_t* node_ids = nullptr;
};

constexpr std::uint32_t INVALID_CLUSTER_ID = (1 << 22) - 1;

struct GEdge;
struct GNode {
  std::int64_t node_id : 40;
  // 1 iff the node is in a large component of the graph.
  std::uint32_t large_component : 1;
  // Cluster id number. It is expected (and checked during construction)
  // that there are less than 2^22 clusters in the planet graph.
  std::uint32_t cluster_id : 22 = INVALID_CLUSTER_ID;
  // 1 iff the node connects to different clusters.
  std::uint32_t cluster_border_node : 1 = 0;
  // Position of the first edge of this node in g.edges. The last edge is given
  // by the start position of the next node in g.nodes, or by the length of
  // g.edges if this is the last node in g.nodes.
  // For easy access, check out helper functions gnode_edge_stop(),
  // gnode_all_edges(), gnode_forward_edges() and gnode_inverted_edges() below.
  std::int64_t edges_start_pos : 36;
  // This node is in a dead end, i.e. in a small subgraph that is connected
  // through a bridge edge to the rest of the graph. All routes to a
  // node outside of this dead end have to pass through the bridge edge.
  // Dead end subgraphs are small (<10k nodes or so) and help routing algorithms
  // because subgraphs behind bridges can be ignored unless from/to node are in
  // the dead end.
  std::uint32_t dead_end : 1;
  // Country associated with lat/lon.
  std::uint16_t ncc : 10 = INVALID_NCC;

  std::int32_t lat = 0;
  std::int32_t lon = 0;
};

struct GEdge {
  enum RESTRICTION : uint8_t {
    LABEL_UNSET = 0,
    LABEL_FREE,
    // Edge has access ACC_CUSTOMERS, ACC_DELIVERY or ACC_DESTINATION.
    LABEL_RESTRICTED,
    // Edge is not restricted, but can only be reached by going through
    // restricted edges.
    LABEL_RESTRICTED_SECONDARY,
    // Only used during initial processing, does not exist in the resulting road
    // network.
    LABEL_TEMPORARY,
  };
  std::uint32_t other_node_idx;
  std::uint32_t way_idx;
  // Distance between start and end point of the edge, in centimeters.
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
  // Each node in a dead-end has at least one edge that is marked 'to_bridge' or
  // 'bridge', i.e. by following edges with 'to_bridge==1' the bridge can be
  // found with a simple iteration instead of some thing more expensive such as
  // a BFS.
  std::uint64_t to_bridge : 1;
  // DIR_FORWARD (==0) or DIR_BACKWARD (==1), to indicate the
  // direction of the edge relative to the direction of the way. Is not typed
  // 'DIRECTION' because it can not hold all values of that enum. Important for
  // example when selecting forward/backward speed.
  std::uint64_t contra_way : 1;
  // 1 iff edge connects two points in different countries, 0 if both points
  // belong to the same country.
  std::uint64_t cross_country : 1;
  // True iff this edge exists in reality only in the other direction. The
  // inverted edge is added to enable backward search.
  std::uint64_t inverted : 1;
  // True if the edge represents a way that enables both directions for at least
  // one vehicle. In this case, there are two out edges at both end nodes.
  // If false, then the way only enables one direction, and the other direction
  // is represented by an inverted edge at the other node.
  // Inverted edges always have false as value.
  std::uint64_t both_directions : 1;
  // TODO: If the edge is restricted in access (DESTINATION etc). For cars only.
  RESTRICTION car_label : 3;
  // If there was some issue when setting the car label, such as a
  // restricted-secondary road that is a highway.
  // If this is set, then car_label is LABEL_RESTRICTED_SECONDARY.
  std::uint64_t car_label_strange : 1;
};

// Contains the list of border nodes and some metadata for a cluster.
struct GCluster {
  std::uint16_t ncc = 0;
  std::uint32_t cluster_id = 0;
  std::uint32_t num_nodes = 0;
  std::uint32_t num_border_nodes = 0;
  // Each edge is either 'inner', 'outer' (connects to another cluster) or a
  // bridge.
  std::uint32_t num_inner_edges = 0;
  std::uint32_t num_outer_edges = 0;
  // Sorted vector containing the border node indexes (pointing into
  // Graph::nodes). Sorted to improve data locality.
  std::vector<std::uint32_t> border_nodes;
  // For each border node, list distances to all other border nodes.
  // Distance INFU32 indicates that a node can't be reached.
  std::vector<std::vector<std::uint32_t>> distances;

  uint32_t FindBorderNodePos(uint32_t node_idx) const {
    const auto it =
        std::lower_bound(border_nodes.begin(), border_nodes.end(), node_idx);
    if (it == border_nodes.end()) {
      return border_nodes.size();
    }
    return it - border_nodes.begin();
  }

  const std::vector<std::uint32_t>& GetBorderNodeDistances(
      uint32_t node_idx) const {
    const uint32_t bn_pos = FindBorderNodePos(node_idx);
    CHECK_LT_S(bn_pos, border_nodes.size());
    return distances.at(bn_pos);
  }
};

struct Graph {
  struct Component {
    uint32_t start_node;
    uint32_t size;
  };

  static constexpr uint32_t kLargeComponentMinSize = 20000;

  std::vector<WaySharedAttrs> way_shared_attrs;
  std::vector<GWay> ways;
  std::vector<GNode> nodes;
  std::vector<GEdge> edges;
  // Large components, sorted by decreasing size.
  std::vector<Component> large_components;
  std::vector<GCluster> clusters;
  // SimpleMemPool aligned_pool_;
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

inline size_t gnode_edge_stop(const Graph& g, uint32_t node_idx) {
  return node_idx + 1 < g.nodes.size()
             ? g.nodes.at(node_idx + 1).edges_start_pos
             : g.edges.size();
}

inline std::span<GEdge> gnode_all_edges(Graph& g, uint32_t node_idx) {
  uint32_t e_start = g.nodes.at(node_idx).edges_start_pos;
  uint32_t e_stop = gnode_edge_stop(g, node_idx);
  return std::span<GEdge>(&(g.edges.at(e_start)), e_stop - e_start);
}

inline std::span<const GEdge> gnode_all_edges(const Graph& g,
                                              uint32_t node_idx) {
  uint32_t e_start = g.nodes.at(node_idx).edges_start_pos;
  uint32_t e_stop = gnode_edge_stop(g, node_idx);
  return std::span<const GEdge>(&(g.edges.at(e_start)), e_stop - e_start);
}

inline std::span<GEdge> gnode_forward_edges(Graph& g, uint32_t node_idx) {
  uint32_t e_start = g.nodes.at(node_idx).edges_start_pos;
  uint32_t e_stop = gnode_edge_stop(g, node_idx);
  while (e_stop > e_start && g.edges.at(e_stop - 1).inverted) {
    e_stop--;
  }
  return std::span<GEdge>(&(g.edges.at(e_start)), e_stop - e_start);
}

inline std::span<const GEdge> gnode_forward_edges(const Graph& g,
                                                  uint32_t node_idx) {
  uint32_t e_start = g.nodes.at(node_idx).edges_start_pos;
  uint32_t e_stop = gnode_edge_stop(g, node_idx);
  while (e_stop > e_start && g.edges.at(e_stop - 1).inverted) {
    e_stop--;
  }
  return std::span<const GEdge>(&(g.edges.at(e_start)), e_stop - e_start);
}

inline std::span<const GEdge> gnode_inverted_edges(const Graph& g,
                                                   uint32_t node_idx) {
  uint32_t e_start = g.nodes.at(node_idx).edges_start_pos;
  uint32_t e_stop = gnode_edge_stop(g, node_idx);
  while (e_start < e_stop && !g.edges.at(e_start).inverted) {
    e_start++;
  }
  // Use g.edges[], not g.edges.at(), because this could be outside range if the
  // last node has no inverted edges.
  //
  return std::span<const GEdge>(&(g.edges[e_start]), e_stop - e_start);
}

inline const WaySharedAttrs& GetWSA(const Graph& g, uint32_t way_idx) {
  return g.way_shared_attrs.at(g.ways.at(way_idx).wsa_id);
}

inline const WaySharedAttrs& GetWSA(const Graph& g, const GWay& way) {
  return g.way_shared_attrs.at(way.wsa_id);
}

// Returns the position of routing attrs for vehicle type 'vt' and direction
// 'dir'. The allowed values for 'vt' are VH_MOTOR_VEHICLE, VH_BICYCLE,
// VH_FOOT, for 'dir' they are DIR_FORWARD, DIR_BACKWARD. Check-fails if an
// attribute is not valid.
inline uint32_t RAinWSAIndex(VEHICLE vt, DIRECTION dir) {
  const uint32_t pos = vt * 2 + dir;
  CHECK_LT_S(dir, 2);
  CHECK_LT_S(pos, WaySharedAttrs::RA_MAX);
  return pos;
}

#define EDGE_DIR(edge) ((DIRECTION)edge.contra_way)
#define EDGE_INVERSE_DIR(edge) ((DIRECTION)(1 - edge.contra_way))

inline RoutingAttrs GetRAFromWSA(const WaySharedAttrs& wsa, VEHICLE vt,
                                 DIRECTION dir) {
  return wsa.ra[RAinWSAIndex(vt, dir)];
}

inline RoutingAttrs GetRAFromWSA(const Graph& g, uint32_t way_idx, VEHICLE vt,
                                 DIRECTION dir) {
  return GetRAFromWSA(GetWSA(g, way_idx), vt, dir);
}

inline RoutingAttrs GetRAFromWSA(const Graph& g, const GEdge& e, VEHICLE vt) {
  return GetRAFromWSA(GetWSA(g, e.way_idx), vt, EDGE_DIR(e));
}

inline RoutingAttrs GetRAFromWSA(const Graph& g, const GWay& way, VEHICLE vt,
                                 DIRECTION dir) {
  return GetRAFromWSA(GetWSA(g, way), vt, dir);
}

inline bool RoutableAccess(ACCESS acc) {
  return acc >= ACC_CUSTOMERS && acc < ACC_MAX;
}

inline bool FreeAccess(ACCESS acc) {
  return acc >= ACC_PERMISSIVE && acc < ACC_MAX;
}

// Access is allowed but restricted.
inline bool RestrictedAccess(ACCESS acc) {
  return acc >= ACC_CUSTOMERS && acc < ACC_PERMISSIVE;
}

// Access is allowed but restricted.
inline bool WSARestrictedAccess(const WaySharedAttrs& wsa, VEHICLE vt,
                                DIRECTION dir) {
  return RestrictedAccess(GetRAFromWSA(wsa, vt, dir).access);
}

// Return true if any vehicle is routable in direction 'dir', false if not.
inline bool WSAAnyRoutable(const WaySharedAttrs& wsa, DIRECTION dir) {
  for (const VEHICLE vt : WaySharedAttrs::RA_VEHICLES) {
    if (RoutableAccess(GetRAFromWSA(wsa, vt, dir).access)) {
      return true;
    }
  }
  return false;
}

inline bool WSAVehicleAnyRoutable(const WaySharedAttrs& wsa, VEHICLE vt) {
  return RoutableAccess(GetRAFromWSA(wsa, vt, DIR_FORWARD).access) ||
         RoutableAccess(GetRAFromWSA(wsa, vt, DIR_BACKWARD).access);
}

// Return true if any vehicle is routable, false if not.
inline bool WSAAnyRoutable(const WaySharedAttrs& wsa) {
  return WSAAnyRoutable(wsa, DIR_FORWARD) || WSAAnyRoutable(wsa, DIR_BACKWARD);
}

inline bool RoutableForward(const Graph& g, const GWay& way, VEHICLE vt) {
  return RoutableAccess(GetRAFromWSA(g, way, vt, DIR_FORWARD).access);
}

inline bool RoutableBackward(const Graph& g, const GWay& way, VEHICLE vt) {
  return RoutableAccess(GetRAFromWSA(g, way, vt, DIR_BACKWARD).access);
}
