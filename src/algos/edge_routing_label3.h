#pragma once

#include "algos/complex_turn_restriction.h"
#include "base/constants.h"
#include "base/deduper_with_ids.h"
#include "graph/graph_def.h"

using CTRList = std::vector<ActiveCtrs>;

// EdgeRoutingLabel3 uniquely identifies an edge in a graph data structure, with
// additional context that is needed for cluster edges or when edges are
// travelled more than once.
//
// It works for graph edges (GEdge) and cluster edges (artificial edges
// representing the connection between two border edges of a cluster). Graph
// edges can have additional context about active CTRs. Overall, there are three
// edge label types.
//
// The class encapsulates the index of the from-node (4 bytes), the edge-offset
// (9 bits), the type of the edge (graph vs. cluster, vs. complex turn
// restriction) and an additional bit that can be used by the router. The data
// is stored in a way that it can be aligned at 2-byte boundaries.
//
// UInt64Key() returns all attributes mapped into a unique uint64_t.
class alignas(2) EdgeRoutingLabel3 final {
 public:
  enum TYPE : uint8_t { GRAPH = 0, CLUSTER, COMPLEX_TURN_RESTRICTION };

  EdgeRoutingLabel3() = delete;

  uint32_t GetType() const { return type_; }
  bool IsClusterEdge() const { return type_ == CLUSTER; }

  // Note: for CLUSTER, this returns the arrival border node.
  uint32_t GetFromIdx(const Graph& g, const CTRList& ctr_list) const {
    if (type_ == GRAPH) {
      return (static_cast<uint32_t>(id_data_high_) << 16) + id_data_low_;
    } else if (type_ == CLUSTER) {
      return g.edges.at(GetInEdgeIdx()).target_idx;
    } else {
      CHECK_S(type_ == COMPLEX_TURN_RESTRICTION);
      return GetActiveTREdge(g, ctr_list).from_node_idx;
    }
  }

  // Note: defined only for type COMPLEX_TURN_RESTRICTION.
  uint32_t GetCtrConfigId() const {
    CHECK_EQ_S(type_, COMPLEX_TURN_RESTRICTION);
    return (static_cast<uint32_t>(id_data_high_) << 16) + id_data_low_;
  }

  // Note: defined only for type CLUSTER.
  uint32_t GetInEdgeIdx() const {
    CHECK_EQ_S(type_, CLUSTER);
    return (static_cast<uint32_t>(id_data_high_) << 16) + id_data_low_;
  }

  // Note: only for type CLUSTER.
  const GCluster::EdgeDescriptor& GetIncomingEdgeDescriptor(
      const Graph& g) const {
    const GCluster& cl = g.clusters.at(GetClusterId(g));
    return cl.border_in_edges.at(cl.FindIncomingEdgePos(GetInEdgeIdx()));
  }

  // Note: only for type CLUSTER.
  const GCluster::EdgeDescriptor& GetOutgoingEdgeDescriptor(
      const Graph& g) const {
    const GCluster& cl = g.clusters.at(GetClusterId(g));
    return cl.border_out_edges.at(GetOffset());
  }

  // Note: defined only for type CLUSTER.
  uint32_t GetClusterId(const Graph& g) const {
    return g.nodes.at(g.edges.at(GetInEdgeIdx()).target_idx).cluster_id;
  }

  uint16_t GetOffset() const { return offset_; }

  bool GetBit() const { return bit_; }

  // Note: For type cluster, returns the to-node of the outgoing edge.
  uint32_t GetToIdx(const Graph& g, const CTRList& ctr_list) const {
    if (type_ != COMPLEX_TURN_RESTRICTION) {
      return GetEdge(g, ctr_list).target_idx;
    } else {
      return GetActiveTREdge(g, ctr_list).to_node_idx;
    }
  }

  // Get the index of the current edge. For CLUSTER, it returns the index of the
  // outgoing edge.
  const uint32_t GetEdgeIdx(const Graph& g, const CTRList& ctr_list) const {
    if (type_ == GRAPH) {
      const GNode& n = g.nodes.at(GetFromIdx(g, ctr_list));
      return n.edges_start_pos + GetOffset();
    } else if (type_ == COMPLEX_TURN_RESTRICTION) {
      const TurnRestriction::TREdge& e = GetActiveTREdge(g, ctr_list);
      return gnode_find_edge_idx(g, e.from_node_idx, e.to_node_idx, e.way_idx);
    } else {
      const auto& ed = GetOutgoingEdgeDescriptor(g);
      return ed.g_edge_idx;
    }
  }

  const GEdge& GetEdge(const Graph& g, const CTRList& ctr_list) const {
    return g.edges.at(GetEdgeIdx(g, ctr_list));
#if 0
    if (type_ == GRAPH) {
      const GNode& n = g.nodes.at(GetFromIdx(g, ctr_list));
      return g.edges.at(n.edges_start_pos + GetOffset());
    } else if (type_ == COMPLEX_TURN_RESTRICTION) {
      const TurnRestriction::TREdge& e = GetActiveTREdge(g, ctr_list);
      return gnode_find_edge(g, e.from_node_idx, e.to_node_idx, e.way_idx);
    } else {
      ABORT_S() << type_;
    }
#endif
  }

  const GNode& FromNode(const Graph& g, const CTRList& ctr_list) const {
    return g.nodes.at(GetFromIdx(g, ctr_list));
  }

  const GNode& ToNode(const Graph& g, const CTRList& ctr_list) const {
    return g.nodes.at(GetToIdx(g, ctr_list));
  }

  static EdgeRoutingLabel3 CreateGraphEdgeLabel(const Graph& g,
                                                uint32_t from_idx,
                                                const GEdge& e, bool bit) {
    uint32_t offset =
        (&e - &g.edges.front()) - g.nodes.at(from_idx).edges_start_pos;
    CHECK_LT_S(offset, 1024);
    CHECK_EQ_S(
        e.target_idx,
        g.edges.at(g.nodes.at(from_idx).edges_start_pos + offset).target_idx)
        << offset;
    return EdgeRoutingLabel3(GRAPH, from_idx, offset, bit);
  }

  // Create a cluster edge. The stored data allows retrieval of the incoming
  // edge, the two connected border nodes and the outgoing edge.
  //
  // 'g_in_edge_idx': index of incoming edge in graph.edges.
  // 'offset': Position of the outgoing edge in cluster->border_out_edges.
  //
  // From 'in_edge_idx', the node_idx of the arrival border node and the
  // cluster id are found.
  // From 'offset' (given the cluster id), the start node and edge offset of the
  // outgoing edge are found.
  static EdgeRoutingLabel3 CreateClusterEdgeLabel(const Graph& g,
                                                  uint32_t in_edge_idx,
                                                  uint16_t offset, bool bit) {
    CHECK_EQ_S(bit, 0);
    CHECK_LT_S(offset, 1024);
    CHECK_S(g.nodes.at(g.edges.at(in_edge_idx).target_idx).cluster_id !=
            INVALID_CLUSTER_ID);
    return EdgeRoutingLabel3(CLUSTER, in_edge_idx, offset, bit);
  }

  // Create a label that points to a specific position in a list of complex turn
  // restrictions.
  //
  // * 'ctr_config_id': Position of ActiveCtr in the ctr list that is supplied
  //    in calls.
  // * 'bit': additional bit for router.
  //
  // Note that 'offset' is not needed for CTRs.
  static EdgeRoutingLabel3 CreateCTREdgeLabel(const Graph& g,
                                              uint32_t ctr_config_id,
                                              bool bit) {
    return EdgeRoutingLabel3(COMPLEX_TURN_RESTRICTION, ctr_config_id,
                             /*offset=*/0, bit);
  }

  // Convert the data to a *unique* 64 bit key that can be used for indexing and
  // hashing.
  // Note that for cluster edges this returns (node_idx, offset) identifying the
  // outgoing edge at the cluster. This reflects that we want to put edges
  // leaving a cluster path the same way as edges just leaving from a cluster
  // after not having entered the cluster.
  uint64_t UInt64Key(const Graph& g, const CTRList& ctr_list) const {
    if (type_ == GRAPH) {
      return (static_cast<uint64_t>(GetFromIdx(g, ctr_list)) << 31) +
             (static_cast<uint64_t>(GetFromIdx(g, ctr_list)) << 13) +
             (static_cast<uint64_t>(GetOffset()) << 3) + (GetBit() << 2) +
             type_;
    } else if (type_ == CLUSTER) {
      CHECK_S(!GetBit());
      // Output the same format as for GRAPH, using the data of the outgoing
      // edge.
      const auto& ed = GetOutgoingEdgeDescriptor(g);
      uint32_t offset = gnode_edge_offset(g, ed.g_from_idx, ed.g_edge_idx); 
      return (static_cast<uint64_t>(ed.g_from_idx) << 31) +
             (static_cast<uint64_t>(ed.g_from_idx) << 13) +
             (static_cast<uint64_t>(offset) << 3) + (GetBit() << 2) +
             GRAPH;
#if 0
      return (static_cast<uint64_t>(FromNode(g, ctr_list).cluster_id) << 31) +
             (static_cast<uint64_t>(FromNode(g, ctr_list).cluster_id) << 17) +
             (static_cast<uint64_t>(GetOffset()) << 3) + (GetBit() << 2) +
             type_;
#endif
    } else {
      return (static_cast<uint64_t>(GetCtrConfigId()) << 31) +
             (static_cast<uint64_t>(GetCtrConfigId()) << 19) +
             (static_cast<uint64_t>(GetOffset()) << 3) + (GetBit() << 2) +
             type_;
    }
  }

 private:
  EdgeRoutingLabel3(TYPE type, uint32_t id_data, uint16_t offset, bool bit) {
    SetIdData(id_data);
    offset_ = offset;
    type_ = type;
    bit_ = bit;
  }

  const TurnRestriction::TREdge& GetActiveTREdge(
      const Graph& g, const CTRList& ctr_list) const {
    // Get arbitrary CTRPosition, which knows the active TREdge.
    const CTRPosition& ctrpos = ctr_list.at(GetCtrConfigId()).at(0);
    const TurnRestriction& tr = g.complex_turn_restrictions.at(ctrpos.ctr_idx);
    return tr.path.at(ctrpos.position);
  }

  void SetIdData(uint32_t id_data) {
    id_data_low_ = id_data & ((1ul << 16) - 1);
    id_data_high_ = id_data >> 16;
  }

  uint16_t id_data_low_;
  uint16_t id_data_high_;
  uint16_t offset_ : 10;
  TYPE type_ : 2;
  uint16_t bit_ : 1;
};
