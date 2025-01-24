#pragma once

#include "base/constants.h"
#include "graph/graph_def.h"

// GEdgeKey uniquely identifies an edge in a graph data structure. It works both
// for graph edges (GEdge) and cluster edges. It encapsulates the index of the
// from-node (4 bytes), the edge-offset (9 bits), the type of the edge (graph
// vs. cluster) and an additional bit that can be used by the router. The data
// is stored in a way that it can be aligned at 2-byte boundaries.
//
// HashKey() returns all attributes mapped into a uint64_t for non-cluster
// edges. For cluster edges it is different, it uses cluster_id instead of
// from_idx.
class alignas(2) GEdgeKey {
 public:
  GEdgeKey() = delete;
  GEdgeKey(uint32_t from_idx, uint8_t offset, bool is_cluster_edge, bool bit) {
    from_idx_low_ = from_idx & ((1ul << 16) - 1);
    from_idx_high_ = from_idx >> 16;
    offset_ = offset;
    is_cluster_edge_ = is_cluster_edge;
    bit_ = bit;
  }

  uint32_t GetFromIdx() const {
    return (static_cast<uint32_t>(from_idx_high_) << 16) + from_idx_low_;
  }

  uint16_t GetOffset() const { return offset_; }

  bool IsClusterEdge() const { return is_cluster_edge_; }
  bool GetBit() const { return bit_; }

  uint32_t GetToIdx(const Graph& g) const {
    if (!IsClusterEdge()) {
      return GetEdge(g).other_node_idx;
    } else {
      const GCluster& cluster = g.clusters.at(FromNode(g).cluster_id);
      return cluster.border_nodes.at(GetOffset());
    }
  }

  const GEdge& GetEdge(const Graph& g) const {
    CHECK_S(!IsClusterEdge());
    const GNode& n = g.nodes.at(GetFromIdx());
    return g.edges.at(n.edges_start_pos + GetOffset());
  }

  const GNode& FromNode(const Graph& g) const {
    return g.nodes.at(GetFromIdx());
  }

  const GNode& ToNode(const Graph& g) const { return g.nodes.at(GetToIdx(g)); }

  static GEdgeKey Create(const Graph& g, uint32_t from_idx, const GEdge& e,
                         bool bit) {
    uint32_t offset =
        (&e - &g.edges.front()) - g.nodes.at(from_idx).edges_start_pos;
    CHECK_LT_S(offset, 512);
    CHECK_EQ_S(e.other_node_idx,
               g.edges.at(g.nodes.at(from_idx).edges_start_pos + offset)
                   .other_node_idx)
        << offset;
    return GEdgeKey(from_idx, offset, /*cluster_edge=*/false, bit);
  }

  static GEdgeKey CreateClusterEdge(const Graph& g, uint32_t from_idx,
                                    uint32_t offset, bool bit) {
    CHECK_LT_S(offset, 512);
    CHECK_S(g.nodes.at(from_idx).cluster_id != INVALID_CLUSTER_ID);
    return GEdgeKey(from_idx, offset, /*cluster_edge=*/true, bit);
  }

  uint64_t HashKey(const Graph& g) {
    if (!IsClusterEdge()) {
      return (static_cast<uint64_t>(GetFromIdx()) << 32) +
             (static_cast<uint64_t>(GetOffset()) << 2) + (GetBit() << 1) + 0;
    } else {
      CHECK_S(!GetBit());
      return (static_cast<uint64_t>(FromNode(g).cluster_id) << 32) +
             (static_cast<uint64_t>(GetOffset()) << 2) + (GetBit() << 1) + 1;
    }
  }

 private:
  friend std::hash<GEdgeKey>;
  uint16_t from_idx_low_;
  uint16_t from_idx_high_;
  uint16_t offset_ : 14;
  uint16_t is_cluster_edge_ : 1;
  uint16_t bit_ : 1;
};

#if 0
  bool operator==(const GEdgeKey& other) const {
    return memcmp(arr_, other.arr_, total_dim) == 0;
  }
namespace std {
template <>
struct hash<GEdgeKey> {
  size_t operator()(const GEdgeKey& key) const {
    // Use already defined std::string_view hashes.
    return std::hash<std::string_view>{}(
        std::string_view((char*)key.arr_, GEdgeKey::total_dim));
  }
};
}  // namespace std
#endif
