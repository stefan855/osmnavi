#pragma once

#include <sys/mman.h>

#include <ranges>
#include <span>
#include <vector>

#include "algos/routing_metric.h"
#include "base/deduper_with_ids.h"
#include "base/mmap_base.h"
#include "geometry/geometry.h"
#include "graph/graph_def.h"

// Stores basic node data in an uint64_t.
struct MMNode {
  bool border_node() const { return (data__ & 1ull) != 0; }
  bool dead_end() const { return (data__ & 2ull) != 0; }
  bool off_cluster_node() const { return (data__ & 4ull) != 0; }
  uint64_t edge_start_idx() const { return data__ >> 3ull; }

  // Set individual values.
  void set_border_node(bool val) { data__ = (data__ & ~1ull) | val; }
  void set_dead_end(bool val) { data__ = (data__ & ~2ull) | (val << 1ull); }
  void set_off_cluster_node(bool val) {
    data__ = (data__ & ~4ull) | (val << 2ull);
  }
  void set_edge_start_idx(uint64_t val) {
    CHECK_LT_S(val, 1ull << 61);
    data__ = (val << 3ull) | (data__ & 7ull);
  }

  // Data.
  uint64_t data__;
};
CHECK_IS_MM_OK(MMNode);
#define MM_NODE(x) ((const MMNode)(x))
#define MM_NODE_RW(x) ((MMNode&)(x))

// Stores basic edge data in an uint64_t. The data consists of several 1-bit and
// one unsigned multi-bit integer. The 1-bit values are stored in the lowest
// bits, the multi-bit value is stored in the bits just above the
// 1-bit values. Because of this layout, the uint64_t can be efficiently stored
// in a compressed vector.
struct MMEdge {
  bool dead_end() const { return (data__ & 1ull) != 0; }
  bool bridge() const { return (data__ & 2ull) != 0; }
  bool restricted() const { return (data__ & 4ull) != 0; }
  bool contra_way() const { return (data__ & 8ull) != 0; }
  bool cluster_border_edge() const { return (data__ & 16ull) != 0; }
  bool complex_turn_restriction_trigger() const {
    return (data__ & 32ull) != 0;
  }
  uint64_t target_idx() const { return data__ >> 6ull; }

  // Set individual values.
  void set_dead_end(bool val) { data__ = (data__ & ~1ull) | val; }
  void set_bridge(bool val) { data__ = (data__ & ~2ull) | (val << 1ull); }
  void set_restricted(bool val) { data__ = (data__ & ~4ull) | (val << 2ull); }
  void set_contra_way(bool val) { data__ = (data__ & ~8ull) | (val << 3ull); }
  void set_cluster_border_edge(bool val) {
    data__ = (data__ & ~16ull) | (val << 4ull);
  }
  void set_complex_turn_restriction_trigger(bool val) {
    data__ = (data__ & ~32ull) | (val << 5ull);
  }
  void set_target_idx(uint64_t val) {
    CHECK_LT_S(val, 1ull << 58ull);
    data__ = (val << 6ull) | (data__ & 63ull);
  }

  // Data.
  uint64_t data__;
};
CHECK_IS_MM_OK(MMEdge);
#define MM_EDGE(x) ((const MMEdge)(x))
#define MM_EDGE_RW(x) ((MMEdge&)(x))

struct MMInEdge {
  uint32_t from_cluster_id;  // The cluster id of the other cluster.
  uint32_t from_node_idx;
  uint32_t to_node_idx;
  uint32_t edge_idx;     // Index of this edge in the edge array.
  uint32_t in_edge_pos;  // Position in the list of in-edges of this cluster.
};

struct MMOutEdge {
  uint32_t from_node_idx;
  uint32_t to_node_idx;
  uint32_t edge_idx;
  uint32_t to_cluster_id;  // Id of the other cluster.
  uint32_t out_edge_pos;   // Position in the list of in-edges of this cluster.
};

struct MMComplexTurnRestriction {
  uint32_t trigger_edge_idx;
  uint32_t first_node_idx;  // from-node of the first leg
  uint32_t first_leg_pos;
  uint16_t num_legs;
  uint16_t forbidden : 1;
};

struct MMLatLon {
  int32_t lat;
  int32_t lon;
};

struct MMBoundingRect {
  // Upper left and lower right corner of the bounding rectangle;
  MMLatLon min;
  MMLatLon max;
};

struct MMFullNode {
  uint32_t node_idx;
  uint32_t cluster_id;
};

// An edge with enough data to find it in a mmgraph.
struct MMFullEdge {
  uint32_t from_node_idx;
  uint32_t cluster_id : 24;
  uint32_t edge_offset : 8;
};

// This is the memory mapped data structure that represents one cluster in the
// file.
struct MMCluster {
  uint32_t cluster_id;

  // Nodes in 'nodes' are sorted by type, with the following layout:
  // border_nodes | off_cluster_nodes | inner_nodes | dead_end_nodes.
  uint16_t num_border_nodes;
  uint16_t num_off_cluster_nodes;
  uint32_t num_inner_nodes;
  uint32_t num_dead_end_nodes;

  MMBoundingRect bounding_rect;

  // *** Cross cluster routing.
  MMVec64<MMInEdge> in_edges;
  MMVec64<MMOutEdge> out_edges;

  // Metrics from in_edge to out_edge computed using standard settings. The
  // client might compute new distances with different settings. Dimension is
  // #in_edges x #out_edges.
  MMVec64<uint32_t> path_metrics;
  // Bit is set for a node exactly when there is a shortest path that passes
  // through this node.
  MMBitset skeleton_nodes;

  // *** Inside cluster routing.
  // MMVec64<MMNode> nodes;
  // MMVec64<MMEdge> edges;
  MMCompressedUIntVec nodes;  // Cast to MMNode to write or read.
  MMCompressedUIntVec edges;  // Cast to MMEdge to write or read.
  MMCompressedUIntVec edge_to_distance;
  MMCompressedUIntVec edge_to_way;
  MMCompressedUIntVec edge_to_turn_costs_pos;
  MMCompressedUIntVec way_to_wsa;

  // Deduped shared attributes, referencded by index.
  MMVec64<WaySharedAttrs> way_shared_attrs;
  MMTurnCostsTable turn_costs_table;
  // A vector of complex turn restrictions needed for routing. Sorted by
  // 'trigger_edge_idx'.
  MMVec64<MMComplexTurnRestriction> complex_turn_restrictions;
  // Array referenced by entries in complex_turn_restrictions.
  MMVec64<uint32_t> complex_turn_restriction_legs;

  // *** Way description generation.
  MMVec64<MMLatLon> node_to_latlon;
  MMCompressedUIntVec way_to_streetname_pos;
  MMStringsTable streetnames_table;

  // *** Debugging.
  MMGroupedOSMIds grouped_node_to_osm_id;
  MMGroupedOSMIds grouped_way_to_osm_id;

  // Helper functions.
  MMNode get_node(uint32_t node) const { return MM_NODE(nodes.at(node)); }

  MMEdge get_edge(uint32_t edge_idx) const {
    return MM_EDGE(edges.at(edge_idx));
  }

  uint32_t edge_start_idx(uint32_t node_idx) const {
    return MM_NODE(nodes.at(node_idx)).edge_start_idx();
  }
  uint32_t edge_stop_idx(uint32_t node_idx) const {
    return (node_idx < nodes.size() - 1)
               ? MM_NODE(nodes.at(node_idx + 1)).edge_start_idx()
               : edges.size();
  }

  inline std::ranges::iota_view<uint32_t, uint32_t> edge_indices(
      uint32_t node_idx) const {
    return std::views::iota(edge_start_idx(node_idx), edge_stop_idx(node_idx));
  }

  inline std::ranges::iota_view<uint32_t, uint32_t> edge_offsets(
      uint32_t node_idx) const {
    return std::views::iota(0u, get_num_edges(node_idx));
  }

  uint32_t get_num_edges(uint32_t node_idx) const {
    return edge_stop_idx(node_idx) - edge_start_idx(node_idx);
  }

  int64_t get_node_id(uint32_t node_idx) const {
    return grouped_node_to_osm_id.at(node_idx);
  }

  int64_t get_edge_to_way_id(uint32_t edge_idx) const {
    return grouped_way_to_osm_id.at(edge_to_way.at(edge_idx));
  }
  int64_t get_way_to_way_id(uint32_t way_idx) const {
    return grouped_way_to_osm_id.at(way_idx);
  }

  std::span<const uint8_t> get_turn_costs(uint32_t edge_idx) const {
    return turn_costs_table.at(edge_to_turn_costs_pos.at(edge_idx));
  }

  // Find the first index in complex_turn_restrictions that is triggered by
  // edge_idx. Note that one has to iterate from this index until until the
  // first leg doesn't match edge_idx anymore (or until the end).
  uint32_t find_complex_turn_restriction_idx(uint32_t edge_idx) const {
    for (uint32_t idx = 0; idx < complex_turn_restrictions.size(); ++idx) {
      /*
      LOG_S(INFO) << "AA0:"
                  << complex_turn_restrictions.at(idx).trigger_edge_idx;
      LOG_S(INFO) << "AA1:" << edge_idx;
      for (auto leg : get_complex_turn_restriction_legs(
               complex_turn_restrictions.at(idx))) {
        LOG_S(INFO) << "AA2:" << leg;
      }
      */

      if (complex_turn_restrictions.at(idx).trigger_edge_idx == edge_idx) {
        return idx;
      }
    }
    ABORT_S() << "Complex turn restriction not found";
  }

  std::span<const uint32_t> get_complex_turn_restriction_legs(
      const MMComplexTurnRestriction& ctr) const {
    // LOG_S(INFO) << "BB0:" << ctr.first_leg_pos;
    // LOG_S(INFO) << "BB1:" << ctr.num_legs;
    // LOG_S(INFO) << "BB2:" << complex_turn_restriction_legs.size();
    const uint32_t* ptr = &complex_turn_restriction_legs.at(ctr.first_leg_pos);
    // LOG_S(INFO) << "BB3:";
    return std::span<const uint32_t>(ptr, ctr.num_legs);
  }

  const WaySharedAttrs& get_wsa(uint32_t way_idx) const {
    return way_shared_attrs.at(way_to_wsa.at(way_idx));
  }

  const std::string_view get_streetname(uint32_t way_idx) const {
    return streetnames_table.at(way_to_streetname_pos.at(way_idx));
  }

  uint32_t start_border_nodes() const { return 0; }
  uint32_t start_off_cluster_nodes() const { return num_border_nodes; }
  uint32_t start_inner_nodes() const {
    return num_border_nodes + num_off_cluster_nodes;
  }
  uint32_t start_dead_end_nodes() const {
    return num_border_nodes + num_off_cluster_nodes + num_inner_nodes;
  }

  // Debugging

  // Search node with OSM-id == 'id'. Returns the index of the node or -1 if not
  // found.
  int64_t find_node_idx_by_id(int64_t id) const {
    return grouped_node_to_osm_id.find_idx(id);
  }
};
CHECK_IS_MM_OK(MMCluster);

struct MMClusterBoundingRect {
  uint32_t cluster_id;
  MMBoundingRect bounding_rect;
};

struct MMGraph {
  uint64_t magic;
  uint32_t version_major;
  uint32_t version_minor;
  uint64_t file_size;
  uint64_t timestamp;

  // Sorted cluster bounding rectangles. Sort order is by increasing
  // bounding_rect.min.lon;
  MMVec64<MMClusterBoundingRect> sorted_bounding_rects;
  MMVec64<MMCluster> clusters;

  // Debugging

  // Find node by OSM id.
  bool find_node_by_id(int64_t id, MMFullNode* fn) const {
    for (const MMCluster& mmc : clusters.span()) {
      int64_t ret = mmc.find_node_idx_by_id(id);
      if (ret >= mmc.start_inner_nodes() ||
          (ret >= 0 && ret < mmc.start_off_cluster_nodes())) {
        *fn = {.node_idx = static_cast<uint32_t>(ret),
               .cluster_id = mmc.cluster_id};
        return true;
      }
    }
    return false;
  }
};
CHECK_IS_MM_OK(MMGraph);

// TODO: add scope.
constexpr uint64_t kMagic = 7715514337782280064ull;
constexpr uint64_t kVersionMajor = 0;
constexpr uint64_t kVersionMinor = 1;

struct MMClusterWrapper {
  const MMCluster& g;
  std::vector<uint32_t> edge_weights;
  const uint8_t* const base_ptr;

  // Pre-compute all edge weights for the edges of the cluster.
  void FillEdgeWeights(VEHICLE vt, const RoutingMetric& metric) {
    edge_weights.clear();
    edge_weights.reserve(g.edges.size());
    for (uint32_t edge_idx = 0; edge_idx < g.edges.size(); ++edge_idx) {
      const WaySharedAttrs& wsa = g.get_wsa(g.edge_to_way.at(edge_idx));
      const DIRECTION direction =
          ((DIRECTION)MM_EDGE(g.edges.at(edge_idx)).contra_way());
      const uint32_t distance_cm = g.edge_to_distance.at(edge_idx);
      edge_weights.push_back(metric.Compute(wsa, vt, direction, distance_cm));
    }
  }
};
