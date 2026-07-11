#pragma once

#include <sys/mman.h>

#include <ranges>
#include <span>
#include <vector>

#include "algos/routing_metric.h"
#include "base/deduper_with_ids.h"
#include "base/deg_coord.h"
#include "base/frequency_table.h"
#include "base/mmap_base.h"
#include "geometry/geometry.h"
#include "graph/graph_def.h"

constexpr uint64_t kMMMagic = 7715514337782280064ull;
constexpr uint32_t kMMVersionMajor = 0;
constexpr uint32_t kMMVersionMinor = 7;

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
  bool cross_cluster_edge() const { return (data__ & 16ull) != 0; }
  bool complex_turn_restriction_trigger() const {
    return (data__ & 32ull) != 0;
  }
  uint64_t target_idx() const { return data__ >> 6ull; }

  // Set individual values.
  void set_dead_end(bool val) { data__ = (data__ & ~1ull) | val; }
  void set_bridge(bool val) { data__ = (data__ & ~2ull) | (val << 1ull); }
  void set_restricted(bool val) { data__ = (data__ & ~4ull) | (val << 2ull); }
  void set_contra_way(bool val) { data__ = (data__ & ~8ull) | (val << 3ull); }
  void set_cross_cluster_edge(bool val) {
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

struct MMGraph;
struct MMFullEdge;

// An edge with start node in another cluster.
struct MMIncomingEdge {
  uint32_t from_cluster_id;  // The cluster id of the other cluster.
  uint32_t from_node_idx;
  uint32_t to_cluster_id;  // The "home" cluster.
  uint32_t to_node_idx;
  uint32_t edge_idx;     // Index of this edge in the edge array.
  uint16_t in_edge_pos;  // Position of this entry in the containing vector.
  // OSM ids to help connecting clusters.
  int64_t from_node_id;
  int64_t to_node_id;
  int64_t way_id;

  std::string DebugString() const {
    return absl::StrFormat("Edge %lld->%lld cl-id:%u->%u way:%lld incoming",
                           from_node_id, to_node_id, from_cluster_id,
                           to_cluster_id, way_id);
  }

  inline MMFullEdge ToFullEdge(const MMGraph& mg) const;
};

// An edge with target node in another cluster.
struct MMOutgoingEdge {
  uint32_t from_cluster_id;  // The "home" cluster.
  uint32_t from_node_idx;
  uint32_t to_cluster_id;  // The cluster id of the other cluster.
  uint32_t to_node_idx;
  uint32_t edge_idx;      // Index of this edge in the edge array.
  uint16_t out_edge_pos;  // Position of this entry in the containing vector.
  // OSM ids to help connecting clusters.
  int64_t from_node_id;
  int64_t to_node_id;
  int64_t way_id;

  std::string DebugString() const {
    return absl::StrFormat("Edge %lld->%lld cl-id:%u->%u way:%lld outgoing",
                           from_node_id, to_node_id, from_cluster_id,
                           to_cluster_id, way_id);
  }

  inline MMFullEdge ToFullEdge(const MMGraph& mg) const;
};

struct MMComplexTurnRestriction {
  uint32_t trigger_edge_idx;
  uint32_t first_node_idx;  // from-node of the first leg
  uint32_t first_leg_pos;
  uint16_t num_legs;
  uint16_t forbidden : 1;
};

struct MMBoundingRect {
  // Upper left and lower right corner of the bounding rectangle;
  MMLatLon min;
  MMLatLon max;
};

// This is the memory mapped data structure that represents one cluster in the
// file.
struct MMCluster {
  uint32_t cluster_id;
  // The "color" of a cluster. A small number drawn in such a way that adjacent
  // clusters have different numbers.
  uint16_t color_no = 0;

  // Nodes in 'nodes' are sorted by type, with the following layout:
  // border_nodes | off_cluster_nodes | inner_nodes | dead_end_nodes.
  uint16_t num_border_nodes;
  uint16_t num_off_cluster_nodes;
  uint32_t num_inner_nodes;
  uint32_t num_dead_end_nodes;

  MMBoundingRect bounding_rect;

  // *** Cross cluster routing.
  MMVec64<MMIncomingEdge> in_edges;
  MMVec64<MMOutgoingEdge> out_edges;

  // Metrics from the end of the in_edge to the end of the out_edge, computed
  // using standard settings. The client might compute new distances with
  // different settings. Dimension is #in_edges x #out_edges.
  //
  // See 'get_path_metric()' below.
  MMVec64<uint32_t> path_metrics;

  // *** Inside cluster routing.
  // MMVec64<MMNode> nodes;
  // MMVec64<MMEdge> edges;
  MMCompressedUIntVec nodes;  // Cast to MMNode to write or read.
  MMCompressedUIntVec edges;  // Cast to MMEdge to write or read.
  MMCompressedUIntVec edge_to_distance;
  MMCompressedUIntVec edge_to_way;
  MMCompressedUIntVec edge_to_turn_costs_pos;
  MMCompressedUIntVec way_to_wsa;

  // Deduped shared attributes, referenced by index.
  MMVec64<WaySharedAttrs> way_shared_attrs;
  MMTurnCostsTable turn_costs_table;
  // A vector of complex turn restrictions needed for routing. Sorted by
  // 'trigger_edge_idx'.
  MMVec64<MMComplexTurnRestriction> complex_turn_restrictions;
  // Array referenced by entries in complex_turn_restrictions.
  MMVec64<uint32_t> complex_turn_restriction_legs;

  // Store coordinates relative to bounding_rect.min
  // Use node_to_latlon()/node_to_lat()/node_to_lon() to query.
  MMCompressedUIntVec node_to_rel_lat;
  MMCompressedUIntVec node_to_rel_lon;

  // *** Way description generation.
  MMCompressedUIntVec way_to_streetname_pos;
  MMStringsTable streetnames_table;

  // *** Debugging.
  MMGroupedOSMIds grouped_node_to_osm_id;
  MMGroupedOSMIds grouped_way_to_osm_id;

  MMShapeCoords edge_shape_coords;

  // Helper functions.
  uint32_t get_path_metric(uint32_t in_edge_pos, uint32_t out_edge_pos) const {
    /*
    LOG_S(INFO) << "in_edge_pos:" << in_edge_pos
                << " out_edge_pos:" << out_edge_pos;
    LOG_S(INFO) << "in_edges size:" << in_edges.size()
                << " out_edges size:" << out_edges.size();
    */
    return path_metrics.at(in_edge_pos * out_edges.size() + out_edge_pos);
  }
  uint32_t get_path_metric(const MMIncomingEdge& in,
                           const MMOutgoingEdge& out) const {
    return get_path_metric(in.in_edge_pos, out.out_edge_pos);
  }

  MMNode get_node(uint32_t node_idx) const {
    return MM_NODE(nodes.at(node_idx));
  }

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

  MMLatLon node_to_latlon(uint32_t node_idx) const {
    return {.lat = node_to_lat(node_idx), .lon = node_to_lon(node_idx)};
  }

  DegE6 node_to_lat(uint32_t node_idx) const {
    return DegE6(bounding_rect.min.lat.v64() +
                 static_cast<int64_t>(node_to_rel_lat.at(node_idx)));
  }

  DegE6 node_to_lon(uint32_t node_idx) const {
    return DegE6(bounding_rect.min.lon.v64() +
                 static_cast<int64_t>(node_to_rel_lon.at(node_idx)));
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
  // The first index of an edge belonging to a dead end. This is the first edge
  // that is labelled bridge() or dead_end().
  // Note that this is - by construction - the same as the number of
  // non-dead-end edges in the cluster.
  uint32_t start_dead_end_edges() const {
    uint32_t first_n_idx = start_dead_end_nodes();
    if (first_n_idx == 0) {
      return 0;
    }
    return edge_stop_idx(first_n_idx - 1);
  }

  // The number of non-dead-end edges. They are stored at the beginning of the
  // edges vector, i.e. in [0..num_non_dead_end_edges()-1].
  uint32_t num_non_dead_end_edges() const { return start_dead_end_edges(); }

  // Debugging

  // Search node with OSM-id == 'id'. Returns the index of the node or -1 if not
  // found.
  int64_t find_node_idx_by_id(int64_t id) const {
    return grouped_node_to_osm_id.find_idx(id);
  }

  uint32_t find_edge_idx(uint32_t from_node_idx, uint32_t target_idx,
                         uint32_t way_idx) const {
    for (uint32_t idx : edge_indices(from_node_idx)) {
      if (get_edge(idx).target_idx() == target_idx &&
          edge_to_way.at(idx) == way_idx) {
        return idx;
      }
    }
    return INFU32;
  }

  // Find the position of an incoming edge in the array of incoming edges.
  // Returns the position or INFU32 if the edge wasn't found.
  uint32_t find_incoming_edge_pos(uint32_t edge_idx) const {
    for (const MMIncomingEdge& in_edge : in_edges.span()) {
      if (in_edge.edge_idx == edge_idx) {
        return in_edge.in_edge_pos;
      }
    }
    return INFU32;
  }

  const MMIncomingEdge& find_incoming_edge(uint32_t edge_idx) const {
    uint32_t pos = find_incoming_edge_pos(edge_idx);
    CHECK_NE_S(pos, INFU32);
    return in_edges.at(pos);
  }

  // Find the position of an outgoing edge in the array of outgoing edges.
  // Returns the position or INFU32 if the edge wasn't found.
  uint32_t find_outgoing_edge_pos(uint32_t edge_idx) const {
    for (const MMOutgoingEdge& out_edge : out_edges.span()) {
      if (out_edge.edge_idx == edge_idx) {
        return out_edge.out_edge_pos;
      }
    }
    return INFU32;
  }

  const MMOutgoingEdge& find_outgoing_edge(uint32_t edge_idx) const {
    uint32_t pos = find_outgoing_edge_pos(edge_idx);
    CHECK_NE_S(pos, INFU32) << edge_idx << " " << out_edges.size();
    return out_edges.at(pos);
  }

  std::vector<MMLatLon> get_shape_coords(uint32_t from_node_idx,
                                         uint32_t edge_idx) const {
    bool use_reverse_edge;
    if (edge_shape_coords.is_empty(edge_idx, &use_reverse_edge)) {
      if (!use_reverse_edge) {
        return {};  // Edge doesn't have shape coords.
      }
      uint32_t rev_edge_idx =
          find_edge_idx(get_edge(edge_idx).target_idx(), from_node_idx,
                        edge_to_way.at(edge_idx));
      // The edge *must* exist.
      CHECK_NE_S(rev_edge_idx, INFU32);
      MMShapeCoords::Result res;
      edge_shape_coords.get(node_to_latlon(get_edge(edge_idx).target_idx()),
                            rev_edge_idx, &res);
      // The list *must* be non-empty.
      CHECK_S(!res.latlon.empty());
      CHECK_S(!res.use_reverse_edge);
      std::reverse(res.latlon.begin(), res.latlon.end());
      return res.latlon;  // We found shape coords at the reverse edge.
    } else {
      MMShapeCoords::Result res;
      edge_shape_coords.get(node_to_latlon(from_node_idx), edge_idx, &res);
      CHECK_S(!res.use_reverse_edge);
      return res.latlon;  // We found shape coords at the normal edge.
    }
  }

  // Return the "FullEdge" debug string for this edge.
  std::string DebugStringEdge(uint32_t from_idx, uint32_t edge_idx) const;
};
CHECK_IS_MM_OK(MMCluster);

struct MMFullNode {
  uint32_t node_idx;
  uint32_t cluster_id;
};

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

  const MMCluster& mc(uint32_t cluster_id) const {
    return clusters.at(cluster_id);
  }

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

  // Given an outgoing edge of some cluster, find the corresponding incoming
  // edge at the target cluster.
  const MMIncomingEdge& out_edge_to_in_edge(
      const MMOutgoingEdge& out_edge) const {
    const MMCluster& to_mc = clusters.at(out_edge.to_cluster_id);
    for (const MMIncomingEdge& in_edge : to_mc.in_edges.span()) {
      if (out_edge.from_node_id == in_edge.from_node_id &&
          out_edge.to_node_id == in_edge.to_node_id &&
          out_edge.way_id == in_edge.way_id) {
        return in_edge;
      }
    }
    ABORT_S() << "could not find incoming edge " << out_edge.from_node_id << " "
              << out_edge.to_node_id << " " << out_edge.way_id;
  }

  // Given an incoming edge of some cluster, find the corresponding outgoing
  // edge at the source cluster.
  const MMOutgoingEdge& in_edge_to_out_edge(
      const MMIncomingEdge& in_edge) const {
    const MMCluster& from_mc = clusters.at(in_edge.from_cluster_id);
    for (const MMOutgoingEdge& out_edge : from_mc.out_edges.span()) {
      if (out_edge.from_node_id == in_edge.from_node_id &&
          out_edge.to_node_id == in_edge.to_node_id &&
          out_edge.way_id == in_edge.way_id) {
        return out_edge;
      }
    }
    ABORT_S() << "could not find outgoing edge " << in_edge.from_node_id << " "
              << in_edge.to_node_id << " " << in_edge.way_id;
  }

 private:
  template <typename T>
  static constexpr bool mm_has_bitwidth(const T& container) {
    return std::is_same_v<T, MMCompressedUIntVec>;
  }

  void LogLine(const char name[], const MinMaxAvg<uint64_t>& stat_cnt,
               const MinMaxAvg<uint64_t>& stat_bw,
               const MinMaxAvg<uint64_t>& stat_by) const {
    RAW_LOG_F(
        INFO,
        "%-29s %8lu %8lu %8lu %3lu %3lu %3lu %7lu %8lu %8lu %11lu (%5.2f%%)",
        name, stat_cnt.Min(), stat_cnt.Avg(), stat_cnt.Max(), stat_bw.Min(),
        stat_bw.Avg(), stat_bw.Max(), stat_by.Min(), stat_by.Avg(),
        stat_by.Max(), stat_by.Sum(), (100.0 * stat_by.Sum()) / file_size);
#if 0
    LOG_S(INFO) << absl::StrFormat(
        "%-29s %7llu %8llu %8llu %11llu (%5.2f%%)", name, stat.Min(),
        stat.Max(), stat.Avg(), stat.Sum(), (100.0 * stat.Sum()) / file_size);
#endif
  }

#define DO_STATS_FOR_CLUSTER_ATTR(attr_name, total_bytes)              \
  {                                                                    \
    MinMaxAvg<uint64_t> stat_cnt;                                      \
    MinMaxAvg<uint64_t> stat_bytes;                                    \
    MinMaxAvg<uint64_t> stat_bw;                                       \
    for (const auto& mc : clusters.span()) {                           \
      stat_bytes.Add(mc.attr_name.num_data_bytes());                   \
      stat_cnt.Add(mc.attr_name.size());                               \
      if (mc.attr_name.size() > 0) {                                   \
        uint64_t bit_width =                                           \
            (8 * mc.attr_name.num_data_bytes()) / mc.attr_name.size(); \
        stat_bw.Add(bit_width);                                        \
      }                                                                \
    }                                                                  \
    LogLine(#attr_name, stat_cnt, stat_bw, stat_bytes);                \
    total_bytes += stat_bytes.Sum();                                   \
  }

 public:
  void PrintInfo() const {
    RAW_LOG_F(INFO, "File size:           %12lu", file_size);
    RAW_LOG_F(INFO, "Size MMGraph struct:   %10lu", sizeof(MMGraph));
    RAW_LOG_F(INFO, "  Size bounding rects: %10lu",
              sorted_bounding_rects.num_data_bytes());
    RAW_LOG_F(INFO, "  Size clusters vector:%10lu", clusters.num_data_bytes());

    RAW_LOG_F(INFO, std::string(115, '=').c_str());
    RAW_LOG_F(INFO, "Cluster Disk Usage Stats (bytes)");
    RAW_LOG_F(INFO,
              "                                          counts          "
              "bits/entry       bytes per cluster       total bytes");
    RAW_LOG_F(INFO,
              "What                               min      avg     max  min "
              "avg max     min      avg      max         sum");
    RAW_LOG_F(INFO, std::string(115, '=').c_str());
    uint64_t total = 0;
    DO_STATS_FOR_CLUSTER_ATTR(in_edges, total);
    DO_STATS_FOR_CLUSTER_ATTR(out_edges, total);
    DO_STATS_FOR_CLUSTER_ATTR(path_metrics, total);
    DO_STATS_FOR_CLUSTER_ATTR(nodes, total);
    DO_STATS_FOR_CLUSTER_ATTR(edges, total);
    DO_STATS_FOR_CLUSTER_ATTR(edge_to_distance, total);
    DO_STATS_FOR_CLUSTER_ATTR(edge_to_way, total);
    DO_STATS_FOR_CLUSTER_ATTR(edge_to_turn_costs_pos, total);
    DO_STATS_FOR_CLUSTER_ATTR(way_to_wsa, total);
    DO_STATS_FOR_CLUSTER_ATTR(way_shared_attrs, total);
    DO_STATS_FOR_CLUSTER_ATTR(turn_costs_table, total);
    DO_STATS_FOR_CLUSTER_ATTR(complex_turn_restrictions, total);
    DO_STATS_FOR_CLUSTER_ATTR(complex_turn_restriction_legs, total);
    DO_STATS_FOR_CLUSTER_ATTR(node_to_rel_lat, total);
    DO_STATS_FOR_CLUSTER_ATTR(node_to_rel_lon, total);
    DO_STATS_FOR_CLUSTER_ATTR(way_to_streetname_pos, total);
    DO_STATS_FOR_CLUSTER_ATTR(streetnames_table, total);
    DO_STATS_FOR_CLUSTER_ATTR(grouped_node_to_osm_id, total);
    DO_STATS_FOR_CLUSTER_ATTR(grouped_way_to_osm_id, total);
    DO_STATS_FOR_CLUSTER_ATTR(edge_shape_coords, total);
    RAW_LOG_F(INFO, std::string(115, '-').c_str());
    RAW_LOG_F(INFO, "Total %6lu clusters %63lu %20lu (%5.2f%%)",
              clusters.size(), total / clusters.size(), total,
              (100.0 * total) / file_size);
    RAW_LOG_F(INFO, std::string(115, '=').c_str());
  }
};
CHECK_IS_MM_OK(MMGraph);

// An edge with enough data to find it in a mmgraph.
struct MMFullEdge {
  uint32_t from_node_idx;
  uint32_t cluster_id : 24;
  uint32_t edge_offset : 8;

  uint32_t edge_idx(const MMCluster& mc) const {
    CHECK_EQ_S(cluster_id, mc.cluster_id);
    return mc.edge_start_idx(from_node_idx) + edge_offset;
  }
  const MMEdge edge(const MMCluster& mc) const {
    CHECK_EQ_S(cluster_id, mc.cluster_id);
    return MM_EDGE(mc.edges.at(edge_idx(mc)));
  }
  uint32_t target_idx(const MMCluster& mc) const {
    CHECK_EQ_S(cluster_id, mc.cluster_id);
    return edge(mc).target_idx();
  }
  uint32_t way_idx(const MMCluster& mc) const {
    CHECK_EQ_S(cluster_id, mc.cluster_id);
    return mc.edge_to_way.at(edge_idx(mc));
  }
  const MMCluster& mc(const MMGraph& mg) const {
    return mg.clusters.at(cluster_id);
  }
  inline bool IsCrossClusterEdge(const MMCluster& mc) const {
    return edge(mc).cross_cluster_edge();
  }
  bool IsIncomingEdge(const MMCluster& mc) const {
    return mc.get_node(from_node_idx).off_cluster_node();
  }
  // Fails if it is not an incoming edge.
  const MMIncomingEdge& ToIncomingEdge(const MMCluster& mc) const {
    return mc.find_incoming_edge(edge_idx(mc));
  }
  bool IsOutgoingEdge(const MMCluster& mc) const {
    return mc.get_node(target_idx(mc)).off_cluster_node();
  }
  // Fails if it is not an outgoing edge.
  const MMOutgoingEdge& ToOutgoingEdge(const MMCluster& mc) const {
    return mc.find_outgoing_edge(edge_idx(mc));
  }
  const MMOutgoingEdge& ToOutgoingEdge(const MMGraph& mg) const {
    return ToOutgoingEdge(mc(mg));
  }

  // Returns the cluster id of the connected external cluster. Check fails if
  // the edge isn't a cross cluster edge.
  uint32_t GetCrossClusterId(const MMCluster& mc) const {
    CHECK_S(IsCrossClusterEdge(mc));
    if (IsIncomingEdge(mc)) {
      return mc.find_incoming_edge(edge_idx(mc)).from_cluster_id;
    } else {
      return mc.find_outgoing_edge(edge_idx(mc)).to_cluster_id;
    }
  }

  // A cross cluster edge exists in the two clusters. This method computes the
  // dual edge of a cross cluster edge.
  // Check fails if the edge is not a cross cluster edge.
  MMFullEdge GetDualCrossClusterEdge(const MMGraph& mg) const {
    const MMCluster& mmc = mc(mg);
    CHECK_S(IsCrossClusterEdge(mmc));
    if (IsIncomingEdge(mmc)) {
      const MMIncomingEdge& in_edge = ToIncomingEdge(mmc);
      return mg.in_edge_to_out_edge(in_edge).ToFullEdge(mg);
    } else {
      CHECK_S(IsOutgoingEdge(mmc));
      const MMOutgoingEdge& out_edge = ToOutgoingEdge(mmc);
      return mg.out_edge_to_in_edge(out_edge).ToFullEdge(mg);
    }
  }

  std::string DebugString(const MMCluster& mc) const {
    CHECK_EQ_S(cluster_id, mc.cluster_id);
    // Find the from/to clusters.
    uint32_t cluster_id_from = cluster_id;
    uint32_t cluster_id_to = cluster_id;
    if (edge(mc).cross_cluster_edge()) {
      if (mc.get_node(from_node_idx).off_cluster_node()) {
        // incoming edge.
        cluster_id_from = mc.find_incoming_edge(edge_idx(mc)).from_cluster_id;
      } else {
        // outgoing edge.
        cluster_id_to = mc.find_outgoing_edge(edge_idx(mc)).to_cluster_id;
      }
    }
    return absl::StrFormat(
        "Edge %lld->%lld cl-id:%u->%u", mc.get_node_id(from_node_idx),
        mc.get_node_id(target_idx(mc)), cluster_id_from, cluster_id_to);
  }
  std::string DebugString(const MMGraph& mg) const {
    return DebugString(mc(mg));
  }

  static inline MMFullEdge CreateWithEdgeIdx(const MMCluster& mc,
                                             uint32_t from_node_idx,
                                             uint32_t edge_idx) {
    uint32_t offset = edge_idx - mc.edge_start_idx(from_node_idx);
    CHECK_LT_S(offset, 256);
    return {.from_node_idx = from_node_idx,
            .cluster_id = mc.cluster_id,
            .edge_offset = offset};
  }

  // Spaceship operator, implements all comparison functions at once.
  auto operator<=>(const MMFullEdge& other) const = default;
};

inline std::string MMCluster::DebugStringEdge(uint32_t from_idx,
                                              uint32_t edge_idx) const {
  return MMFullEdge::CreateWithEdgeIdx(*this, from_idx, edge_idx)
      .DebugString(*this);
}

inline MMFullEdge MMOutgoingEdge::ToFullEdge(const MMGraph& mg) const {
  const MMCluster& mc = mg.mc(from_cluster_id);
  return MMFullEdge::CreateWithEdgeIdx(mc, from_node_idx, edge_idx);
}

inline MMFullEdge MMIncomingEdge::ToFullEdge(const MMGraph& mg) const {
  const MMCluster& mc = mg.mc(to_cluster_id);
  return MMFullEdge::CreateWithEdgeIdx(mc, from_node_idx, edge_idx);
}

// Returns the edges incoming at 'target_node_idx'.
inline std::vector<MMFullEdge> mm_get_incoming_edges_slow(
    const MMCluster& mc, uint32_t target_node_idx) {
  std::vector<MMFullEdge> res;
  uint32_t node_idx = 0;
  for (uint32_t edge_idx = 0; edge_idx < mc.edges.size(); ++edge_idx) {
    if (MM_EDGE(mc.edges.at(edge_idx)).target_idx() == target_node_idx) {
      // We found and edge, now fast forward the node array to find the node.
      while (node_idx + 1 < mc.nodes.size() &&
             mc.edge_start_idx(node_idx + 1) <= edge_idx) {
        node_idx++;
      }
      CHECK_GE_S(edge_idx, mc.edge_start_idx(node_idx));
      CHECK_LT_S(edge_idx, mc.edge_stop_idx(node_idx));
      res.push_back(MMFullEdge::CreateWithEdgeIdx(mc, node_idx, edge_idx));
    }
  }
  return res;
}

struct MMClusterWrapper {
  const MMCluster& mc;
  std::vector<uint32_t> edge_weights;

  MMClusterWrapper() = delete;
  MMClusterWrapper(const MMCluster& mc, VEHICLE vt, const RoutingMetric& metric,
                   bool include_dead_ends)
      : mc(mc) {
    FillEdgeWeights(vt, metric, include_dead_ends);
  }

  // Pre-compute all edge weights for the edges of the cluster.
  void FillEdgeWeights(VEHICLE vt, const RoutingMetric& metric,
                       bool include_dead_ends) {
    const size_t num =
        include_dead_ends ? mc.edges.size() : mc.num_non_dead_end_edges();
    edge_weights.clear();
    edge_weights.reserve(num);
    for (uint32_t edge_idx = 0; edge_idx < num; ++edge_idx) {
      const WaySharedAttrs& wsa = mc.get_wsa(mc.edge_to_way.at(edge_idx));
      const DIRECTION direction =
          ((DIRECTION)MM_EDGE(mc.edges.at(edge_idx)).contra_way());
      const uint32_t distance_cm = mc.edge_to_distance.at(edge_idx);
      edge_weights.push_back(metric.Compute(wsa, vt, direction, distance_cm));
    }
  }

  static MMClusterWrapper Create(const MMCluster& mc, VEHICLE vt,
                                 const RoutingMetric& metric,
                                 bool include_dead_ends) {
    MMClusterWrapper mcw(mc, vt, metric, include_dead_ends);
    return mcw;
  }
};
