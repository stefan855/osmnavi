#pragma once

#include "algos/complex_turn_restriction.h"
#include "base/constants.h"
#include "base/deduper_with_ids.h"
#include "graph/graph_def.h"

// GEdgeKey uniquely identifies an edge in a graph data structure. It works both
// for graph edges (GEdge) and cluster edges. It encapsulates the index of the
// from-node (4 bytes), the edge-offset (9 bits), the type of the edge (graph
// vs. cluster) and an additional bit that can be used by the router. The data
// is stored in a way that it can be aligned at 2-byte boundaries.
//
// UInt64Key() returns all attributes mapped into a unique uint64_t.
class alignas(2) GEdgeKey final {
 public:
  enum TYPE : uint8_t { GRAPH = 0, CLUSTER, TURN_RESTRICTION };
  GEdgeKey() = delete;
  GEdgeKey(TYPE type, uint32_t id_data, uint8_t offset, bool bit) {
    SetIdData(id_data);
    offset_ = offset;
    type_ = type;
    bit_ = bit;
  }

  uint32_t GetType() const { return type_; }
  bool IsClusterEdge() const { return type_ == CLUSTER; }

  uint32_t GetFromIdx(const Graph& g, const CTRDeDuper& dd) const {
    if (type_ != TURN_RESTRICTION) {
      return (static_cast<uint32_t>(id_data_high_) << 16) + id_data_low_;
    } else {
      return GetActiveTREdge(g, dd).from_node_idx;
    }
  }

  uint32_t GetCtrConfigId() const {
    CHECK_EQ_S(type_, TURN_RESTRICTION);
    return (static_cast<uint32_t>(id_data_high_) << 16) + id_data_low_;
  }

  const TurnRestriction::TREdge& GetActiveTREdge(const Graph& g,
                                                 const CTRDeDuper& dd) const {
    // GetCtrConfigId() checks type_ == TURN_RESTRICTION.
    const CTRPosition& ctrpos = dd.GetObj(GetCtrConfigId()).at(0);
    const TurnRestriction& tr = g.complex_turn_restrictions.at(ctrpos.ctr_idx);
    return tr.path.at(ctrpos.position);
  }

  uint16_t GetOffset() const { return offset_; }

  bool GetBit() const { return bit_; }

  uint32_t GetToIdx(const Graph& g, const CTRDeDuper& dd) const {
    if (type_ == GRAPH) {
      return GetEdge(g, dd).target_idx;
    } else if (type_ == CLUSTER) {
      const GCluster& cluster = g.clusters.at(FromNode(g, dd).cluster_id);
      return cluster.border_nodes.at(GetOffset());
    } else {
      return GetActiveTREdge(g, dd).to_node_idx;
    }
  }

  const GEdge& GetEdge(const Graph& g, const CTRDeDuper& dd) const {
    if (type_ == GRAPH) {
      const GNode& n = g.nodes.at(GetFromIdx(g, dd));
      return g.edges.at(n.edges_start_pos + GetOffset());
    } else if (type_ == TURN_RESTRICTION) {
      const TurnRestriction::TREdge& e = GetActiveTREdge(g, dd);
      return gnode_find_edge(g, e.from_node_idx, e.to_node_idx, e.way_idx);
    } else {
      ABORT_S() << type_;
    }
  }

  const GNode& FromNode(const Graph& g, const CTRDeDuper& dd) const {
    return g.nodes.at(GetFromIdx(g, dd));
  }

  const GNode& ToNode(const Graph& g, const CTRDeDuper& dd) const {
    return g.nodes.at(GetToIdx(g, dd));
  }

  static GEdgeKey CreateGraphEdge(const Graph& g, uint32_t from_idx,
                                  const GEdge& e, bool bit) {
    uint32_t offset =
        (&e - &g.edges.front()) - g.nodes.at(from_idx).edges_start_pos;
    CHECK_LT_S(offset, 1024);
    CHECK_EQ_S(
        e.target_idx,
        g.edges.at(g.nodes.at(from_idx).edges_start_pos + offset).target_idx)
        << offset;
    return GEdgeKey(GRAPH, from_idx, offset, bit);
  }

  static GEdgeKey CreateClusterEdge(const Graph& g, uint32_t from_idx,
                                    uint32_t offset, bool bit) {
    CHECK_LT_S(offset, 1024);
    CHECK_S(g.nodes.at(from_idx).cluster_id != INVALID_CLUSTER_ID);
    return GEdgeKey(CLUSTER, from_idx, offset, bit);
  }

  static GEdgeKey CreateTurnRestrictionEdge(const Graph& g,
                                            uint32_t ctr_config_id,
                                            uint32_t offset, bool bit) {
    CHECK_LT_S(offset, 1024);
    return GEdgeKey(TURN_RESTRICTION, ctr_config_id, offset, bit);
  }

  // Convert the data to a *unique* 64 bit key that can be used for indexing and
  // hashing.
  // Note that for cluster edges this returns (cluster_id, offset), which
  // identifies the target node of the edge. This reflects that when using the
  // key, we are not interested from which start node we're arriving at the
  // target node.
  uint64_t UInt64Key(const Graph& g, const CTRDeDuper& dd) {
    if (type_ == GRAPH) {
      return (static_cast<uint64_t>(GetFromIdx(g, dd)) << 31) +
             (static_cast<uint64_t>(GetFromIdx(g, dd)) << 13) +
             (static_cast<uint64_t>(GetOffset()) << 3) + (GetBit() << 2) +
             type_;
    } else if (type_ == CLUSTER) {
      CHECK_S(!GetBit());
      return (static_cast<uint64_t>(FromNode(g, dd).cluster_id) << 31) +
             (static_cast<uint64_t>(FromNode(g, dd).cluster_id) << 17) +
             (static_cast<uint64_t>(GetOffset()) << 3) + (GetBit() << 2) +
             type_;
    } else {
      return (static_cast<uint64_t>(GetCtrConfigId()) << 31) +
             (static_cast<uint64_t>(GetCtrConfigId()) << 19) +
             (static_cast<uint64_t>(GetOffset()) << 3) + (GetBit() << 2) +
             type_;
    }
  }

 private:
  // friend std::hash<GEdgeKey>;

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
