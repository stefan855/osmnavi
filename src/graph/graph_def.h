#pragma once

#include <type_traits>
#include <vector>

// #include "absl/container/flat_hash_map.h"
#include "base/constants.h"
#include "base/huge_bitset.h"
#include "base/simple_mem_pool.h"
#include "base/small_vector.h"
#include "base/util.h"
#include "base/varbyte.h"
#include "graph/routing_attrs.h"
#include "osm/turn_restriction_defs.h"

// For an edge in the graph, this data describes the turn costs when arriving
// at the target node of the edge and continuing on an edge starting at the
// target node (i.e. an outgoing edge).
//
// Note that the dimension of the 'turn_costs' vector of edge e must be equal
// the number of outgoing edges at the target node of e.
//
// A special value indicates that a turn is not possible. This means that a turn
// restriction forbids the turn, a barrier disallows passing the target node, a
// u-turn is not allowed or something else.
struct TurnCostData {
  // TODO: Use a more memory-efficient data structure than a vector.
  // std::vector<uint8_t> turn_costs;
  SmallVector<uint8_t, 7> turn_costs;

  bool operator==(const TurnCostData& other) const {
    return turn_costs == other.turn_costs;
  }
};

// Attributes extracted from node key-val pairs.
// Currently, only barrier based access restrictions are extracted.
struct NodeAttribute {
  int64_t node_id : 40 = 0;

  // has_bit_attr is true if any of the directly following bits below is 1.
  std::uint32_t has_bit_attr : 1 = 0;
  std::uint32_t bit_turning_circle : 1 = 0;
  std::uint32_t bit_stop : 1 = 0;
  std::uint32_t bit_traffic_signals : 1 = 0;
  std::uint32_t bit_crossing : 1 = 0;
  std::uint32_t bit_railway_crossing : 1 = 0;
  std::uint32_t bit_public_transport : 1 = 0;
  std::uint32_t bit_traffic_calming : 1 = 0;

  // Access for each individual vehicle type for barriers.
  std::uint32_t has_barrier : 1 = 0;
  ACCESS vh_acc[VH_MAX];
};

// Number of bits at least needed when storing way_idx.
// TODO: create and use the same for other fundamental data types, including
// node_idx, edge_idx and all kinds of osm-ids.
constexpr uint64_t WAY_IDX_BITS = 31;

// 9955WaySharedAttrs (=WSA) contains many of the way attributes in a
// data structure that is shared between ways. The shared data is stored in
// Graph::way_shared_attrs vector and accessed through GWay::wsa_id.
// Used to decrease the storage needed to store ways, both in ram and on disk.
// Also see base/deduper_with_ids.h.
struct WaySharedAttrs final {
  // Vehicles types used in the ra array.
  static constexpr VEHICLE RA_VEHICLES[] = {VH_MOTORCAR, VH_BICYCLE, VH_FOOT};
  static constexpr uint32_t RA_MAX =
      2 * sizeof(RA_VEHICLES) / sizeof(RA_VEHICLES[0]);
  // Routing info in forward and backward direction.
  RoutingAttrs ra[RA_MAX];

  static WaySharedAttrs Create(const RoutingAttrs dflt) {
    WaySharedAttrs wsa;
    for (RoutingAttrs& val : wsa.ra) {
      val = dflt;
    }
    return wsa;
  }
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
  std::uint8_t uniform_country : 1 = 0;
#if 0
  // Some ways have loops. This is some information about them.
  std::uint8_t head_loop : 1 = 0;   // First node appears multiple times.
  std::uint8_t tail_loop : 1 = 0;   // Last node appears multiple times.
  std::uint8_t inner_loop : 1 = 0;  // Some inner node appears multiple times.
#endif
  std::uint8_t closed_way : 1 = 0;  // First node == Last node.
  std::uint8_t area : 1 = 0;        // Has tag area=yes.
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

constexpr std::uint32_t NUM_CLUSTER_BITS = 22;
constexpr std::uint32_t INVALID_CLUSTER_ID = (1 << NUM_CLUSTER_BITS) - 1;
constexpr std::uint32_t MAX_CLUSTER_ID = INVALID_CLUSTER_ID - 1;

constexpr std::uint32_t NUM_EDGES_OUT_BITS = 5;
constexpr std::uint32_t MAX_NUM_EDGES_OUT = (1 << NUM_EDGES_OUT_BITS) - 1;

struct GNode {
  std::int64_t node_id : 40;
  // 1 iff the node is in a large component of the graph.
  std::uint32_t large_component : 1;
  // Cluster id number. It is expected (and checked during construction)
  // that there are less than 2^22 clusters in the planet graph.
  std::uint32_t cluster_id : NUM_CLUSTER_BITS = INVALID_CLUSTER_ID;
  // 1 iff the node connects to different clusters.
  std::uint32_t cluster_border_node : 1 = 0;
  // Position of the first edge of this node in g.edges. The last edge is given
  // by the start position of the next node in g.nodes, or by the length of
  // g.edges if this is the last node in g.nodes.
  // For easy access, check out helper functions gnode_edge_stop(),
  // gnode_all_edges(), gnode_forward_edges() and gnode_inverted_edges() below.
  std::int64_t edges_start_pos : 36;
  // Number of forward edges of this node. These are allocated in
  // [edges_start_pos, edges_start_pos + num_edges_forward). All other edges are
  // "inverted", e.g. point to another node that has an forward edge to the
  // current node.
  std::uint32_t num_edges_forward : NUM_EDGES_OUT_BITS;
  // This node is in a dead end, i.e. in a small subgraph that is connected
  // through a bridge edge to the rest of the graph. All routes to a
  // node outside of this dead end have to pass through the bridge edge.
  // Dead end subgraphs are small (<10k nodes or so) and help routing algorithms
  // because subgraphs behind bridges can be ignored unless from/to node are in
  // the dead end.
  std::uint32_t dead_end : 1;
  // Country associated with lat/lon.
  std::uint16_t ncc : 10 = INVALID_NCC;

  // True iff the node is the via node in a simple turn restriction.
  std::uint32_t simple_turn_restriction_via_node : 1;

#if 0
  // Node has special keyvals.
  std::uint32_t has_node_attrs : 1;
  // Attributes that represent special keyvals stored in g.node_attrs. If
  // has_node_attrs is 0 then all of the following bits must be 0 too.
  std::uint32_t turning_circle : 1;
  std::uint32_t stop : 1;
  std::uint32_t traffic_signals : 1;
  std::uint32_t pedestrian_crossing : 1;
  std::uint32_t railway_crossing : 1;
  std::uint32_t public_transport_platform : 1;
  std::uint32_t toll_booth : 1;
  std::uint32_t traffic_calming : 1;
#endif

  std::int32_t lat = 0;
  std::int32_t lon = 0;
};

// TODO: 32 bits are needed to represent all distances on earth, but for edges
// this is only actually used when calculating heuristic distances for AStar.
// For real edges, something like 25 bits (335km) would probably be enough.
constexpr std::uint32_t MAX_EDGE_DISTANCE_CM_BITS = 32;
// roughly 1342 km.
constexpr std::uint32_t MAX_EDGE_DISTANCE_CM =
    (1ull << MAX_EDGE_DISTANCE_CM_BITS) - 1;
constexpr uint32_t MAX_TURN_COST_IDX_BITS = 18;
constexpr uint32_t MAX_TURN_COST_IDX = (1ull << MAX_TURN_COST_IDX_BITS) - 1;

struct GEdge {
  enum RESTRICTION : uint8_t {
    LABEL_UNSET = 0,
    LABEL_FREE,
    // Edge has access ACC_CUSTOMERS, ACC_DELIVERY or ACC_DESTINATION.
    LABEL_RESTRICTED,
    // Edge is not restricted, but can only be reached by going through
    // restricted edges.
    LABEL_RESTRICTED_SECONDARY,
    // Only used during initial processing, does not exist in the resulting
    // road network.
    LABEL_TEMPORARY,
  };
  std::uint32_t other_node_idx;
  std::uint32_t way_idx;
  // Distance between start and end point of the edge, in centimeters.
  // std::uint64_t distance_cm : 40;
  std::uint32_t distance_cm;  // MAX_EDGE_DISTANCE_CM_BITS is 32 anyways.
  // True iff this is the first time 'other_node_idx' has this value in the list
  // of edges of the node.
  // Can be used to selected edges for the undirected graph.
  std::uint32_t unique_other : 1;
  // This edge connects two components in the undirected graph. Removing it
  // creates two non-connected subgraphs. The nodes in the smaller of the two
  // subgraphs are all marked 'dead end'. The other component has 'dead_end' set
  // to 0 for all nodes.
  // Note: Only bridges connect dead-end with non-dead-end nodes.
  std::uint32_t bridge : 1;
  // Each node in a dead-end has at least one edge that is marked 'to_bridge' or
  // 'bridge', i.e. by following edges with 'to_bridge==1' the bridge can be
  // found with a simple iteration instead of some thing more expensive such as
  // a BFS.
  std::uint32_t to_bridge : 1;
  // DIR_FORWARD (==0) or DIR_BACKWARD (==1), to indicate the
  // direction of the edge relative to the direction of the way. Is not typed
  // 'DIRECTION' because it can not hold all values of that enum. Important for
  // example when selecting forward/backward speed.
  std::uint32_t contra_way : 1;
  // 1 iff edge connects two points in different countries, 0 if both points
  // belong to the same country.
  std::uint32_t cross_country : 1;
  // True iff this edge exists in reality only in the other direction. The
  // inverted edge is added to enable backward search.
  std::uint32_t inverted : 1;
  // True if the edge represents a way that enables both directions for at least
  // one vehicle. In this case, there are two out edges at both end nodes.
  // If false, then the way only enables one direction, and the other direction
  // is represented by an inverted edge at the other node.
  // Inverted edges always have false as value.
  std::uint32_t both_directions : 1;
  // TODO: If the edge is restricted in access (DESTINATION etc). For cars only.
  RESTRICTION car_label : 3;
  // If there was some issue when setting the car label, such as a
  // restricted-secondary road that is a highway.
  // If this is set, then car_label is LABEL_RESTRICTED_SECONDARY.
  std::uint32_t car_label_strange : 1;
  // True if at 'other_node_idx' it is allowed to travel back to 'from' node of
  // this edge. There are two cases for this that are selected automatically. It
  // is always allowed to do a u-turn at an endpoint of a street. Additionally,
  // it is allowed to do a u-turn if the edge is non-restricted and one would
  // have to enter a restricted access area if not doing a u-turn.
  std::uint32_t car_uturn_allowed : 1;
  std::uint32_t complex_turn_restriction_trigger : 1;

  std::uint32_t turn_cost_idx : MAX_TURN_COST_IDX_BITS;
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
  // Sorted vector containing the border node indexes (pointing into
  // Graph::nodes). Sorted to improve data locality.
  std::vector<std::uint32_t> border_nodes;
  // For each border node, list distances to all other border nodes.
  // Distance INFU32 indicates that a node can't be reached.
  std::vector<std::vector<std::uint32_t>> distances;
  // Color number for drawing clusters. Avoids neighbouring clusters having the
  // same color.
  std::uint16_t color_no = 0;
  std::uint16_t ncc = 0;

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

  std::vector<NodeAttribute> node_attrs;
  std::vector<WaySharedAttrs> way_shared_attrs;
  std::vector<TurnCostData> turn_costs;
  std::vector<GWay> ways;
  std::vector<GNode> nodes;
  std::vector<GEdge> edges;
  // Large components, sorted by decreasing size.
  std::vector<Component> large_components;

  // Turn restrictions. Both types are indexed by the first trigger edge.
  SimpleTurnRestrictionMap simple_turn_restriction_map;

  std::vector<TurnRestriction> complex_turn_restrictions;
  // Map from trigger edge to the index in 'complex_turn_restrictions' above.
  TurnRestrictionMapToFirst complex_turn_restriction_map;

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

  // Find the stored node attribute for a given node_id. Returns the found
  // record or nullptr if it doesn't exist.
  const NodeAttribute* FindNodeAttr(int64_t node_id) const {
    auto it = std::lower_bound(node_attrs.begin(), node_attrs.end(), node_id,
                               [](const NodeAttribute& s, std::int64_t value) {
                                 return s.node_id < value;
                               });
    if (it == node_attrs.end() || it->node_id != node_id) {
      return nullptr;
    } else {
      return &(*it);
    }
  }

  static std::vector<uint64_t> GetGWayNodeIds(const GWay& way) {
    // Decode node_ids.
    std::uint64_t num_nodes;
    CHECK_S(way.node_ids != nullptr);
    std::uint8_t* ptr = way.node_ids;
    ptr += DecodeUInt(ptr, &num_nodes);
    std::vector<uint64_t> ids;
    ptr += DecodeNodeIds(ptr, num_nodes, &ids);
    return ids;
  }

  // Given a way with the original node id list, return the list of node indexes
  // (in graph.nodes) of all graph nodes.
  // Note that this ignores nodes that don't exist or that are not relevant for
  // routing, according to the graph_node_ids bitset.
  std::vector<uint32_t> GetGWayNodeIndexes(const HugeBitset& graph_node_ids,
                                           const GWay& way) const {
    // Find nodes
    std::vector<uint32_t> node_idx;
    for (const uint64_t id : GetGWayNodeIds(way)) {
      if (graph_node_ids.GetBit(id)) {
        std::size_t idx = FindNodeIndex(id);
        CHECK_LT_S(idx, nodes.size());
        node_idx.push_back(idx);
      }
    }
    return node_idx;
  }

  // Same as above, but without the bitset for selecting nodes. This is slightly
  // slower.
  std::vector<uint32_t> GetGWayNodeIndexes(const GWay& way) const {
    std::vector<uint32_t> node_idx;
    for (const uint64_t id : GetGWayNodeIds(way)) {
      std::size_t idx = FindNodeIndex(id);
      if (idx < nodes.size()) {
        node_idx.push_back(idx);
      }
    }
    return node_idx;
  }

  inline size_t gnode_edge_stop(uint32_t node_idx) const {
    return node_idx + 1 < nodes.size() ? nodes.at(node_idx + 1).edges_start_pos
                                       : edges.size();
  }

  void DebugPrint() const {
    LOG_S(INFO) << "=============== Graph with " << nodes.size()
                << " nodes and " << edges.size() << " edges ================";
    for (uint32_t n_idx = 0; n_idx < nodes.size(); ++n_idx) {
      const GNode& n = nodes.at(n_idx);
      LOG_S(INFO) << absl::StrFormat(
          "Node:%i id:%lld cl:%u e_start:%u #forw:%u #tot:%u", n_idx, n.node_id,
          n.cluster_id, n.edges_start_pos, n.num_edges_forward,
          gnode_edge_stop(n_idx) - n.edges_start_pos);
      for (uint32_t e_idx = n.edges_start_pos; e_idx < gnode_edge_stop(n_idx);
           ++e_idx) {
        const GEdge& e = edges.at(e_idx);
        LOG_S(INFO) << absl::StrFormat("    Edge to:%u w:%u contra:%u inv:%u",
                                       e.other_node_idx, e.way_idx,
                                       e.contra_way, e.inverted);
      }
    }
  }
};

inline int64_t GetGNodeId(const Graph& g, uint32_t node_idx) {
  return g.nodes.at(node_idx).node_id;
}

inline int64_t GetGNodeIdSafe(const Graph& g, uint32_t node_idx) {
  if (node_idx < g.nodes.size()) {
    return g.nodes.at(node_idx).node_id;
  }
  return -1;
}

inline int64_t GetGWayIdSafe(const Graph& g, uint32_t way_idx) {
  if (way_idx < g.ways.size()) {
    return g.ways.at(way_idx).id;
  }
  return -1;
}

inline size_t gnode_edge_stop(const Graph& g, uint32_t node_idx) {
  return node_idx + 1 < g.nodes.size()
             ? g.nodes.at(node_idx + 1).edges_start_pos
             : g.edges.size();
}

inline uint32_t gnode_num_edges(const Graph& g, uint32_t node_idx) {
  return gnode_edge_stop(g, node_idx) - g.nodes.at(node_idx).edges_start_pos;
}

inline std::span<GEdge> gnode_all_edges(Graph& g, uint32_t node_idx) {
  uint32_t e_start = g.nodes.at(node_idx).edges_start_pos;
  uint32_t e_stop = gnode_edge_stop(g, node_idx);
  return std::span<GEdge>(&(g.edges[e_start]), e_stop - e_start);
}

inline std::span<const GEdge> gnode_all_edges(const Graph& g,
                                              uint32_t node_idx) {
  uint32_t e_start = g.nodes.at(node_idx).edges_start_pos;
  uint32_t e_stop = gnode_edge_stop(g, node_idx);
  return std::span<const GEdge>(&(g.edges[e_start]), e_stop - e_start);
}

inline std::span<GEdge> gnode_forward_edges(Graph& g, uint32_t node_idx) {
  uint32_t e_start = g.nodes.at(node_idx).edges_start_pos;
  return std::span<GEdge>(&(g.edges[e_start]),
                          g.nodes.at(node_idx).num_edges_forward);
}

inline std::span<const GEdge> gnode_forward_edges(const Graph& g,
                                                  uint32_t node_idx) {
  uint32_t e_start = g.nodes.at(node_idx).edges_start_pos;
  return std::span<const GEdge>(&(g.edges[e_start]),
                                g.nodes.at(node_idx).num_edges_forward);
}

struct FullGEdge {
  uint32_t start_idx;  // offset of the start node of the edge in g.nodes
  uint32_t offset : NUM_EDGES_OUT_BITS;  // offset of the edge in g.edges.
};
// Note, this returns a new vector, not a span.
inline std::vector<FullGEdge> gnode_incoming_edges(const Graph& g,
                                                   uint32_t node_idx) {
  std::vector<FullGEdge> res;
  for (const GEdge& out : gnode_all_edges(g, node_idx)) {
    if (!out.unique_other || out.other_node_idx == node_idx) {
      continue;
    }

    const GNode& other = g.nodes.at(out.other_node_idx);
    for (uint32_t offset = 0; offset < other.num_edges_forward; ++offset) {
      const GEdge& incoming = g.edges.at(other.edges_start_pos + offset);
      if (incoming.other_node_idx == node_idx) {
        res.emplace_back(out.other_node_idx, offset);
      }
    }
  }
  return res;
}

inline const uint32_t gnode_find_forward_edge_offset(const Graph& g,
                                                     uint32_t from_node_idx,
                                                     uint32_t to_node_idx,
                                                     uint32_t way_idx) {
  const GNode& from = g.nodes.at(from_node_idx);
  uint32_t e_start = from.edges_start_pos;
  for (uint32_t off = 0; off < from.num_edges_forward; ++off) {
    if (g.edges.at(e_start + off).other_node_idx == to_node_idx &&
        g.edges.at(e_start + off).way_idx == way_idx) {
      return off;
    }
  }
  ABORT_S() << absl::StrFormat(
      "Node %lld has no forward edge to node %lld with way %lld",
      GetGNodeIdSafe(g, from_node_idx), GetGNodeIdSafe(g, to_node_idx),
      g.ways.at(way_idx).id);
}

inline const uint32_t gnode_find_edge_idx(const Graph& g,
                                          uint32_t from_node_idx,
                                          uint32_t to_node_idx,
                                          uint32_t way_idx) {
  uint32_t e_start = g.nodes.at(from_node_idx).edges_start_pos;
  uint32_t num = gnode_edge_stop(g, from_node_idx) - e_start;
  for (uint32_t off = 0; off < num; ++off) {
    if (g.edges.at(e_start + off).other_node_idx == to_node_idx &&
        g.edges.at(e_start + off).way_idx == way_idx) {
      return e_start + off;
    }
  }
  ABORT_S() << absl::StrFormat(
      "Node %lld has no forward edge to node %lld with way %lld",
      GetGNodeIdSafe(g, from_node_idx), GetGNodeIdSafe(g, to_node_idx),
      g.ways.at(way_idx).id);
}

inline const GEdge& gnode_find_edge(const Graph& g, uint32_t from_node_idx,
                                    uint32_t to_node_idx, uint32_t way_idx) {
  for (const GEdge& e : gnode_all_edges(g, from_node_idx)) {
    if (e.other_node_idx == to_node_idx && e.way_idx == way_idx) return e;
  }
  ABORT_S() << absl::StrFormat(
      "Node %lld has no forward edge to node %lld with way %lld",
      GetGNodeIdSafe(g, from_node_idx), GetGNodeIdSafe(g, to_node_idx),
      g.ways.at(way_idx).id);
}

inline GEdge& gnode_find_edge(Graph& g, uint32_t from_node_idx,
                              uint32_t to_node_idx, uint32_t way_idx) {
  for (GEdge& e : gnode_all_edges(g, from_node_idx)) {
    if (e.other_node_idx == to_node_idx && e.way_idx == way_idx) return e;
  }
  ABORT_S() << absl::StrFormat(
      "Node %lld has no forward edge to node %lld with way %lld",
      GetGNodeIdSafe(g, from_node_idx), GetGNodeIdSafe(g, to_node_idx),
      g.ways.at(way_idx).id);
}

#if 0
// Find edge between two nodes. If way_idx is >=0, then also checks the way of
// the edge.
// Returns a pointer to the edge or nullptr, if the edge couldn't be found.
const GEdge* FindForwardEdge(uint32_t from_node_idx, uint32_t to_node_idx,
                             int64_t way_idx = -1) const {
  for (const GEdge& e : gnode_forward_edges(*this, from_node_idx)) {
    if (e.other_node_idx == to_node_idx &&
        (way_idx == -1 || e.way_idx == way_idx)) {
      return &e;
    }
  }
  return nullptr;
}
#endif

inline std::span<const GEdge> gnode_inverted_edges(const Graph& g,
                                                   uint32_t node_idx) {
  uint32_t e_start = g.nodes.at(node_idx).edges_start_pos +
                     g.nodes.at(node_idx).num_edges_forward;
  uint32_t e_stop = gnode_edge_stop(g, node_idx);
  /*
  while (e_start < e_stop && !g.edges.at(e_start).inverted) {
    e_start++;
  }
  // Use g.edges[], not g.edges.at(), because this could be outside range if the
  // last node has no inverted edges.
  //
  */
  return std::span<const GEdge>(&(g.edges[e_start]), e_stop - e_start);
}

inline const WaySharedAttrs& GetWSA(const Graph& g, uint32_t way_idx) {
  return g.way_shared_attrs.at(g.ways.at(way_idx).wsa_id);
}

inline const WaySharedAttrs& GetWSA(const Graph& g, const GWay& way) {
  return g.way_shared_attrs.at(way.wsa_id);
}

// Describe a path of length two (two edges, three nodes).
// The first edge starts at 'node0_idx' with edge offset 'edge0_off'.
// The second edge starts at 'node1_idx' with edge offset 'edge1_off'.
struct PathLen2Data final {
  bool valid = false;
  uint32_t node0_idx = 0;
  uint32_t edge0_off = 0;
  uint32_t node1_idx = 0;
  uint32_t edge1_off = 0;
  uint32_t node2_idx = 0;

  const GEdge& edge0(const Graph& g) {
    return g.edges.at(g.nodes.at(node0_idx).edges_start_pos + edge0_off);
  }
  const GEdge& edge1(const Graph& g) {
    return g.edges.at(g.nodes.at(node1_idx).edges_start_pos + edge1_off);
  }
  // The compressed turn cost between the first and the second edge.
  uint32_t get_compressed_turn_cost_0to1(const Graph& g) {
    return g.turn_costs.at(edge0(g).turn_cost_idx).turn_costs.at(edge1_off);
  }
};

// Find a path of length 2 (nodes and edges), given the three nodes on the
// path.
// If a path was found, return the path with 'valid' set to true.
// If the path can't be found, return 'valid' set to false.
inline PathLen2Data FindPathLen2(const Graph& g, uint32_t node0_idx,
                           uint32_t node1_idx, uint32_t node2_idx) {
  const GNode& n0 = g.nodes.at(node0_idx);
  for (uint32_t off0 = 0; off0 < n0.num_edges_forward; ++off0) {
    if (g.edges.at(n0.edges_start_pos + off0).other_node_idx == node1_idx) {
      const GNode& n1 = g.nodes.at(node1_idx);
      for (uint32_t off1 = 0; off1 < n1.num_edges_forward; ++off1) {
        if (g.edges.at(n1.edges_start_pos + off1).other_node_idx == node2_idx) {
          // Found a path
          return {.valid = true,
                  .node0_idx = node0_idx,
                  .edge0_off = off0,
                  .node1_idx = node1_idx,
                  .edge1_off = off1,
                  .node2_idx = node2_idx};
        }
      }
    }
  }
  return {.valid = false};
}

// Returns the position of routing attrs for vehicle type 'vt' and direction
// 'dir'. The allowed values for 'vt' are VH_MOTORCAR, VH_BICYCLE,
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
  return acc >= ACC_DISMOUNT && acc < ACC_MAX;
}

// Access is allowed but restricted.
inline bool RestrictedAccess(ACCESS acc) {
  return acc >= ACC_CUSTOMERS && acc <= ACC_DESTINATION;
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

inline bool RoutableForward(const Graph& g, const GEdge& e, VEHICLE vt) {
  return RoutableAccess(
      GetRAFromWSA(g, g.ways.at(e.way_idx), vt, EDGE_DIR(e)).access);
}

inline bool RoutableBackward(const Graph& g, const GEdge& e, VEHICLE vt) {
  return RoutableAccess(
      GetRAFromWSA(g, g.ways.at(e.way_idx), vt, EDGE_INVERSE_DIR(e)).access);
}

namespace std {
template <>
struct hash<TurnCostData> {
  size_t operator()(const TurnCostData& tcd) const {
    return std::hash<std::string_view>{}(
        std::string_view((char*)tcd.turn_costs.data(), tcd.turn_costs.size()));
  }
};
}  // namespace std
