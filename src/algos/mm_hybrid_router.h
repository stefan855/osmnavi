// Route within and across clusters, i.e. on the full graph in the memory
// mapped file.
//
// The router uses MMClusterRouter to route within start and target clusters.
// Additionally, it uses special cross-cluster routing code (implemented here)
// to route across multiple clusters using the precomputed "hybrid" shortcuts.
//
// The hybrid router "sees" the following parts of the underlying graph:
//   * Fully expanded start and target clusters, including all incoming and
//   outgoing cross border edges.
//   * All cross border edges between clusters
//   * All precomputed "shortcut" edges between border edges inside of
//   non-expanded clusters.
//
// If routing finalizes an outgoing edge of an expanded cluster, then there are
// two cases:
//   (1) The cluster of the target of the edge is expanded. In this case, an
//   incoming edge (which is a duplicate of the outgoing edge) is added to the
//   queue of expanded cluster router.
//   (2) The cluster is not expanded, i.e. the outgoing edge of the expanded
//   cluster corresponds to an incoming edge at a non-expanded cluster. In this
//   case, all outgoing edges which are reachable through a shortcut edge from
//   the incoming edge are added to the queue of the hybrid router.
//
// If routing finalizes a hybrid edge, and the target cluster of the edge is
// expanded, then an incoming edge is added to the target cluster router (same a
// (1) above).

#pragma once
#include <algorithm>
#include <fstream>
#include <memory>
#include <queue>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "algos/mm_cluster_router.h"
#include "algos/mm_router_defs.h"
#include "algos/routing_defs.h"
#include "algos/routing_metric.h"
#include "base/util.h"

class MMHybridRouter final {
 public:
  enum RouterType : uint16_t {
    START = 0,
    TARGET = 1,
    HYBRID = 2,
    ROUTER_TYPE_MAX = 3
  };

  struct VisitedEdge {
    uint32_t min_metric;
    // True if this is a target edge in the TARGET router.
    uint32_t target_edge : 1;
    uint32_t done : 1;
    RouterType prev_source : 2;
    // A v_idx or a hybrid_key, depending on prev_source.
    uint32_t prev_key_or_v_idx;
  };

  struct Options {
    bool handle_restricted_access = false;
    bool include_dead_end = false;
  };

  // This might exist multiple times for each edge, when it gets
  // reinserted into the priority queue with a lower priority.
  struct QueuedEdge {
    uint32_t min_metric;
    uint32_t key;  // key into hybrid_map.
  };

  struct MetricCmpEdge {
    bool operator()(const QueuedEdge& left, const QueuedEdge& right) const {
      return left.min_metric > right.min_metric;
    }
  };

  using THybridMap = absl::flat_hash_map<uint32_t, VisitedEdge>;

  // Find or allocate a visited edge for 'key'.
  static inline VisitedEdge& FindOrAllocEdge(uint32_t key,
                                             THybridMap* hybrid_map) {
    // Prevent doing two lookups by following
    // https://stackoverflow.com/questions/1409454
    const auto iter = hybrid_map->insert(
        absl::flat_hash_map<uint32_t, VisitedEdge>::value_type(key, {0}));
    if (iter.second) {
      iter.first->second = {.min_metric = INFU32,  // Marks it as 'unused'.
                            .done = 0};
    }
    return iter.first->second;
  }

  // All data needed for a specific routing request.
  // The MMHybridRouter object itself acctually doesn't store any data,
  // therefore all methods are 'static'.
  struct RouterData {
    // Only entries 0 and 1 are used.
    // The pointer is shared so we can have identical START and TARGET.
    std::shared_ptr<MMClusterWrapper> mcw[ROUTER_TYPE_MAX];
    // Only entries 0 and 1 are used.
    // The pointer is shared so we can have identical START and TARGET.
    std::shared_ptr<MMClusterRouter> router[ROUTER_TYPE_MAX];
    std::priority_queue<MMHybridRouter::QueuedEdge,
                        std::vector<MMHybridRouter::QueuedEdge>, MetricCmpEdge>
        hybrid_queue;
    THybridMap hybrid_map;

    absl::Time start_time_routing = absl::UnixEpoch();
    absl::Time start_time_assemble_segments = absl::UnixEpoch();
    absl::Time start_time_expand_clusters = absl::UnixEpoch();

    // Given the edges in 'target_anchor', add them to the hybrid routing table
    // if they are outgoing edges from some cluster.
    // Note that this touches only the 'hybrid' part of non-target clusters.
    void MarkTargetEdgesForHybrid(const MMGraph& mg,
                                  const GeoAnchor& target_anchor) {
      for (const EdgePoint& ep : target_anchor.edge_points()) {
        // The target cluster must contain these edges and we expand it anyways,
        // so access it there. We're interested in incoming/outgoing edges only.
        const MMFullEdge& fe = ep.fe;
        CHECK_EQ_S(fe.cluster_id, mcw[TARGET]->mc.cluster_id);
        const MMCluster& mc = mg.clusters.at(fe.cluster_id);
        if (fe.edge(mc).cross_cluster_edge()) {  // Between clusters?
          const MMOutgoingEdge* out_edge = nullptr;
          uint32_t pos = mc.find_outgoing_edge_pos(fe.edge_idx(mc));
          if (pos != INFU32) {
            out_edge = &mc.out_edges.at(pos);
          } else {
            // It must be incoming, so check fail if we can't find it.
            const MMIncomingEdge& in_e = mc.find_incoming_edge(fe.edge_idx(mc));
            out_edge = &mg.in_edge_to_out_edge(in_e);
          }
          CHECK_NE_S(out_edge, nullptr);
          const uint32_t key =
              hybrid_key(out_edge->from_cluster_id, out_edge->out_edge_pos);
          FindOrAllocEdge(key, &hybrid_map).target_edge = 1;
          LOG_S(INFO) << "Mark outgoing edge as hybrid target edge: "
                      << out_edge->DebugString();
        }
      }
    }

    void Init(const MMGraph& mg, const GeoAnchor& start_anchor,
              const GeoAnchor& target_anchor) {
      start_time_routing = absl::Now();
      const MMClusterRouter::Options opt = {.handle_restricted_access = true,
                                            .include_dead_end = true};
      // Anchors should follow this scheme:
      //   All edges have to belong to the same cluster
      //   The start node of the first edge dictates the cluster.
      CHECK_S(!start_anchor.edge_points().empty() &&
              !target_anchor.edge_points().empty());
      const GeoAnchor::Info start_info = start_anchor.GetInfo(mg);
      const GeoAnchor::Info target_info = target_anchor.GetInfo(mg);

      // Create a start router.
      mcw[START].reset(
          new MMClusterWrapper(mg.clusters.at(start_info.cluster_id),
                               VH_MOTORCAR, RoutingMetricTime(),
                               /*include_dead_ends=*/true));
      router[START].reset(new MMClusterRouter(*mcw[START], opt));

      if (start_info.cluster_id == target_info.cluster_id) {
        // Initialise start and target edges in the one router we have.
        router[START]->RouteInit(start_anchor, target_anchor);
        mcw[TARGET] = mcw[START];
        router[TARGET] = router[START];
        LOG_S(INFO) << "start and target cluster identical:"
                    << start_info.cluster_id;
      } else {
        // Create a target_router.
        mcw[TARGET].reset(
            new MMClusterWrapper(mg.clusters.at(target_info.cluster_id),
                                 VH_MOTORCAR, RoutingMetricTime(),
                                 /*include_dead_ends=*/true));
        router[TARGET].reset(new MMClusterRouter(*mcw[TARGET], opt));
        router[START]->RouteInit(start_anchor, target_anchor.AdaptToCluster(
                                                   mg, start_info.cluster_id));
        router[TARGET]->RouteInit({}, target_anchor);
        LOG_S(INFO) << "start and target cluster different";
      }
      MarkTargetEdgesForHybrid(mg, target_anchor);
    }
  };

  // This function handles the next step after a outgoing edge has been
  // finalized in any of the routers.
  // It creates an incoming edge in the corresponding router if the target
  // cluster is expanded.
  // If the target is hybrid, then it expands all shortcuts in the hybrid
  // cluster and adds them to the hybrid router queue.
  static inline void HandleOutgoingEdgeTransition(
      const MMGraph& mg, const uint32_t source_min_metric,
      const MMOutgoingEdge& out_edge, RouterType source, uint32_t source_key,
      RouterData& d) {
    const MMIncomingEdge& in_edge = mg.out_edge_to_in_edge(out_edge);

    // Some checks:
    if (source == START) {
      CHECK_EQ_S(in_edge.from_cluster_id, d.mcw[START]->mc.cluster_id);
      // Must be different cluster, otherwise the edge couldn't be outgoing.
      CHECK_NE_S(out_edge.to_cluster_id, d.mcw[START]->mc.cluster_id);
    }
    if (source == TARGET) {
      CHECK_EQ_S(in_edge.from_cluster_id, d.mcw[TARGET]->mc.cluster_id);
      // Must be different cluster, otherwise the edge couldn't be outgoing.
      CHECK_NE_S(out_edge.to_cluster_id, d.mcw[TARGET]->mc.cluster_id);
    }

    if (source != TARGET &&
        out_edge.to_cluster_id == d.mcw[TARGET]->mc.cluster_id) {
      // Add a copy edge to TARGET.
      d.router[TARGET]->AddIncomingEdge(in_edge, source_min_metric);
      // CHECK_EQ_S(min_metric[TARGET], source_min_metric);
    } else if (source != START &&
               out_edge.to_cluster_id == d.mcw[START]->mc.cluster_id) {
      // Add a copy edge to START.
      d.router[START]->AddIncomingEdge(in_edge, source_min_metric);
      // CHECK_EQ_S(min_metric[START], source_min_metric);
    } else {
      // Route edges in HYBRID.
      const MMCluster& mc = mg.clusters.at(out_edge.to_cluster_id);
      for (uint32_t og_edge_idx = 0; og_edge_idx < mc.out_edges.size();
           ++og_edge_idx) {
        const MMOutgoingEdge& new_edge = mc.out_edges.at(og_edge_idx);
        uint32_t cross_metric = mc.get_path_metric(in_edge, new_edge);
        if (cross_metric == INFU32) {
          continue;
        }
        const uint32_t key = hybrid_key(mc.cluster_id, og_edge_idx);
        VisitedEdge& vis = FindOrAllocEdge(key, &d.hybrid_map);
        if (vis.done) {
          CHECK_GE_S(source_min_metric + cross_metric, vis.min_metric);
          continue;
        }

        if (vis.target_edge) {
          // 'cross_metric' can be too large, because it spans the full edge,
          // but the target edge may be used only partially. To fix the metric,
          // use the edge data in the target cluster.
          LOG_S(INFO) << "AA Really Add target edge to hybrid router "
                      << debug_hybrid_key(key)
                      << "metric:" << source_min_metric + cross_metric;
          CHECK_EQ_S(new_edge.from_cluster_id, out_edge.to_cluster_id);
          CHECK_NE_S(new_edge.from_cluster_id, d.mcw[TARGET]->mc.cluster_id);
          CHECK_EQ_S(new_edge.to_cluster_id, d.mcw[TARGET]->mc.cluster_id);
          // The new edge is an incoming edge in the target cluster.
          const GeoAnchor& ta = d.router[TARGET]->GetTargetAnchor();
          const MMIncomingEdge& in_edge = mg.out_edge_to_in_edge(new_edge);
          const uint32_t pos =
              ta.FindPosByEdgeIdx(d.mcw[TARGET]->mc, in_edge.edge_idx);
          CHECK_NE_S(pos, INFU32);
          // 'cross_metric' is too high, and we need to remove the part *from*
          // the point on the edge to the end of the edge.
          const float diff_fraction =
              ta.edge_points().at(pos).GetFromFraction();
          const uint32_t diff_metric =
              diff_fraction * d.mcw[TARGET]->edge_weights.at(in_edge.edge_idx);
          CHECK_LE_S(diff_metric, cross_metric);
          LOG_S(INFO) << "AA2 cross_metric:" << cross_metric << " -> "
                      << cross_metric - diff_metric;
          cross_metric -= diff_metric;
          LOG_S(INFO) << "AA3 new cross_metric:" << cross_metric;
        }

        if (source_min_metric + cross_metric < vis.min_metric) {
          const uint32_t new_metric = source_min_metric + cross_metric;
          if (vis.target_edge) {
            LOG_S(INFO) << "AA4 add target edge with metric:" << new_metric;
          }
          vis.min_metric = new_metric;
          vis.prev_source = source;
          vis.prev_key_or_v_idx = source_key;
          d.hybrid_queue.emplace(new_metric, key);
        }
      }
    }
  }

  static MMRoutingResult Route(const MMGraph& mg, const GeoAnchor& start_anchor,
                               const GeoAnchor& target_anchor,
                               RouterData* router_data = nullptr) {
    LOG_S(INFO) << "=========================================================";
    FuncTimer timer("MMHybridRouter::Route()", __FILE__, __LINE__);
    LOG_S(INFO) << "=========================================================";
    LOG_S(INFO) << absl::StrFormat(
        "Start anchor cluster:%u edges:%llu",
        start_anchor.edge_points().front().fe.cluster_id,
        start_anchor.edge_points().size());
    for (const auto& ep : start_anchor.edge_points()) {
      LOG_S(INFO) << "  " << ep.fe.DebugString(mg)
                  << " frac: " << ep.to_fraction;
    }
    LOG_S(INFO) << absl::StrFormat(
        "Target anchor cluster:%u edges:%llu",
        target_anchor.edge_points().front().fe.cluster_id,
        target_anchor.edge_points().size());
    for (const auto& ep : target_anchor.edge_points()) {
      LOG_S(INFO) << "  " << ep.fe.DebugString(mg)
                  << " frac: " << ep.to_fraction;
    }

    // Use the externally provided router data if it exists, otherwise use the
    // internal one and make it accessible through as 'd'.
    RouterData router_data_internal;
    if (router_data == nullptr) {
      router_data = &router_data_internal;
    }
    RouterData& d = *router_data;

    d.Init(mg, start_anchor, target_anchor);

    while (true) {
      uint32_t min_metric[ROUTER_TYPE_MAX];
      min_metric[START] = d.router[START]->QueueMinMetric();
      min_metric[TARGET] = d.router[TARGET]->QueueMinMetric();
      min_metric[HYBRID] =
          !d.hybrid_queue.empty() ? d.hybrid_queue.top().min_metric : INFU32;

      // Start cluster routing.
      /*
      LOG_S(INFO) << absl::StrFormat(
          "Hybrid Loop start:%u (%llu) target:%u (%llu) hybrid:%u (%llu)",
          min_metric[START], d.router[START]->QueueSize(), min_metric[TARGET],
          d.router[TARGET]->QueueSize(), min_metric[HYBRID],
          d.hybrid_queue.size());
          */

      if (min_metric[START] != INFU32 &&
          min_metric[START] <= min_metric[TARGET] &&
          min_metric[START] <= min_metric[HYBRID]) {
        // Inspect the minimal element in the start cluster.
        const uint32_t v_idx = d.router[START]->QueueMinVIdx();
        if (d.router[START]->IsOutgoingEdge(v_idx)) {
          const MMOutgoingEdge& out_edge = d.mcw[START]->mc.find_outgoing_edge(
              d.router[START]->GetGraphEdgeIdx(v_idx));
          HandleOutgoingEdgeTransition(mg, min_metric[START], out_edge, START,
                                       /*source_key=*/v_idx, d);
        }
        // Finalize the edge.
        MMClusterRouterStatus rs = d.router[START]->RouteOneStep();
        /*
        LOG_S(INFO) << "Route start cluster one step metric "
                    << min_metric[START] << " to "
                    << d.router[START]->QueueMinMetric();
                    */
        if (rs.finished && rs.found) {
          LOG_S(INFO) << "Found target edge";
          return AssembleSegments(mg, d, START, rs);
        }

      } else if (min_metric[TARGET] != INFU32 &&
                 min_metric[TARGET] <= min_metric[START] &&
                 min_metric[TARGET] <= min_metric[HYBRID]) {
        // Inspect the minimal element in the target cluster.
        const uint32_t v_idx = d.router[TARGET]->QueueMinVIdx();
        if (d.router[TARGET]->IsOutgoingEdge(v_idx)) {
          const MMOutgoingEdge& out_edge = d.mcw[TARGET]->mc.find_outgoing_edge(
              d.router[TARGET]->GetGraphEdgeIdx(v_idx));
          HandleOutgoingEdgeTransition(mg, min_metric[TARGET], out_edge, TARGET,
                                       /*source_key=*/v_idx, d);
        }
        // Finalize the edge.
        MMClusterRouterStatus rs = d.router[TARGET]->RouteOneStep();
        /*
        LOG_S(INFO) << "Route target cluster one step metric "
                    << min_metric[TARGET] << " to "
                    << d.router[TARGET]->QueueMinMetric();
                    */
        if (rs.finished && rs.found) {
          LOG_S(INFO) << "Found target edge";
          return AssembleSegments(mg, d, TARGET, rs);
        }

      } else if (min_metric[HYBRID] != INFU32) {
        // We know that START and TARGET are not smaller, so it must be HYBRID.
        CHECK_S(min_metric[HYBRID] <= min_metric[START] &&
                min_metric[HYBRID] <= min_metric[TARGET]);
        const QueuedEdge e = d.hybrid_queue.top();
        d.hybrid_queue.pop();

        auto it = d.hybrid_map.find(e.key);
        CHECK_S(it != d.hybrid_map.end());
        VisitedEdge& vis = it->second;
        if (vis.done) {
          // Old entry, just skip it.
          continue;
        }
        /*
        LOG_S(INFO) << "Finalize hybrid edge <" << e.key << "> at metric "
                    << e.min_metric;
                    */
        CHECK_EQ_S(e.min_metric, vis.min_metric);
        vis.done = 1;
        const MMOutgoingEdge& out_edge = out_edge_from_hybrid_key(mg, e.key);
        HandleOutgoingEdgeTransition(mg, e.min_metric, out_edge, HYBRID,
                                     /*source_key=*/e.key, d);

      } else {
        // All queues empty, stop without finding target.
        break;
      }
    }
    return {};
  }

  static const MMOutgoingEdge& out_edge_from_hybrid_key(const MMGraph& mg,
                                                        uint32_t key) {
    const MMCluster& mc = mg.clusters.at(cluster_id_from_hybrid_key(key));
    return mc.out_edges.at(og_edge_idx_from_hybrid_key(key));
  }

 private:
  struct HybridEdge {
    uint32_t key;
    VisitedEdge vis;
  };
  struct Segment {
    // Source router for the segment.
    RouterType source;
    // Result for sources START and TARGET (empty for HYBRID source)..
    MMRoutingResult res;
    // Result for source HYBRID, empty for START/TARGET source.
    // Contains a list of outgoing edges from the hybrid router, already in
    // forward direction. Each segment carries information about the
    // predecessor edge.
    std::vector<HybridEdge> hybrid_edges;
  };

  static inline uint32_t hybrid_key(uint32_t cluster_id, uint32_t og_edge_idx) {
    return (cluster_id << 10) + og_edge_idx;
  }
  static inline std::string debug_hybrid_key(uint32_t key) {
    return absl::StrFormat("key(cluster:%u pos:%u",
                           cluster_id_from_hybrid_key(key),
                           og_edge_idx_from_hybrid_key(key));
  }
  static inline uint32_t cluster_id_from_hybrid_key(uint32_t hybrid_key) {
    return hybrid_key >> 10;
  }
  static inline uint32_t og_edge_idx_from_hybrid_key(uint32_t hybrid_key) {
    return hybrid_key & ((1u << 10) - 1);
  }

  // Add a START/TARGET segment and return true iff the segment has the
  // start_anchor.
  static bool AddExpandedSegment(const RouterData& d, const RouterType source,
                                 uint32_t v_idx,
                                 std::vector<Segment>* segments) {
    CHECK_S(source != HYBRID);
    MMRoutingResult res = d.router[source]->GetRoutingResult(v_idx);
    segments->push_back({source, res, {}});
    return segments->back().res.start_is_anchor;
  }

  static void AddStatistics(const MMGraph& mg, const RouterData& d,
                            MMRoutingResult* res) {
    res->num_path_full_clusters =
        d.router[START].get() == d.router[TARGET].get() ? 1 : 2;
    res->num_vis_start = d.router[START]->GetVisitedEdges().size();
    res->num_vis_target = d.router[TARGET]->GetVisitedEdges().size();
    res->num_vis_hybrid = d.hybrid_map.size();

    auto now = absl::Now();
    if (d.start_time_expand_clusters != absl::UnixEpoch()) {
      // We have all three times
      res->time_for_expand_hybrid_clusters =
          ToDoubleSeconds(now - d.start_time_expand_clusters);
      res->time_for_assemble = ToDoubleSeconds(d.start_time_expand_clusters -
                                               d.start_time_assemble_segments);
      res->time_for_route_algorithm = ToDoubleSeconds(
          d.start_time_assemble_segments - d.start_time_routing);
    } else if (d.start_time_assemble_segments != absl::UnixEpoch()) {
      res->time_for_assemble =
          ToDoubleSeconds(now - d.start_time_assemble_segments);
      res->time_for_route_algorithm = ToDoubleSeconds(
          d.start_time_assemble_segments - d.start_time_routing);
    } else {
      res->time_for_route_algorithm =
          ToDoubleSeconds(now - d.start_time_routing);
    }
  }

  static MMRoutingResult AssembleSegments(const MMGraph& mg, RouterData& d,
                                          const RouterType last_source,
                                          const MMClusterRouterStatus& rs) {
    d.start_time_assemble_segments = absl::Now();
    if (!rs.finished || !rs.found) {
      MMRoutingResult res = {};
      AddStatistics(mg, d, &res);
      return res;
    }

    CHECK_S(last_source == START || last_source == TARGET) << (int)last_source;
    std::vector<Segment> segments;
    {
      MMRoutingResult res =
          d.router[last_source]->GetRoutingResult(rs.last_v_idx);
      CHECK_S(res.target_is_anchor);
      if (res.start_is_anchor) {
        // We have the full path in one cluster, without any visits to the
        // hybrid router. CHECK_EQ_S(d.hybrid_map.size(), 0);
        LogPath(mg, res);
        AddStatistics(mg, d, &res);
        return res;
      }

      // We go *backward* and collect the route segments in 'segments'.
      // The predecessor segment is found in two ways:
      //   * If a segment is in an expanded cluster, then the first edge in the
      //   route is an incoming edge. To go backward, We find the corresponding
      //   outgoing edge in another router.
      //   * If a segment is in the hybrid router, then the previous router is
      //   stored explicitly in the "HybridEdge'.
      segments.push_back({last_source, res, {}});
    }

    while (true) {
      const Segment& segment = segments.back();
      if (segment.source == HYBRID) {
        // Previous must be != HYBRID
        HybridEdge he = segment.hybrid_edges.front();
        if (AddExpandedSegment(d, he.vis.prev_source, he.vis.prev_key_or_v_idx,
                               &segments)) {
          break;
        }
      } else {
        CHECK_S(segment.source == START || segment.source == TARGET)
            << (int)segment.source;
        // The first edge must be an incoming edge.
        const MMCluster& mc = d.mcw[segment.source]->mc;
        const MMIncomingEdge& in_edge =
            mc.find_incoming_edge(segment.res.full_edges.front().edge_idx(mc));
        // Find the corresponding outgoing edge.
        const MMOutgoingEdge& out_edge = mg.in_edge_to_out_edge(in_edge);

        if (out_edge.from_cluster_id == d.mcw[START]->mc.cluster_id) {
          // out_edge belongs to the START cluster.
          if (AddExpandedSegment(d, START, out_edge.edge_idx, &segments)) {
            break;
          }
        } else if (out_edge.from_cluster_id == d.mcw[TARGET]->mc.cluster_id) {
          // out_edge belongs to the TARGET cluster.
          if (AddExpandedSegment(d, TARGET, out_edge.edge_idx, &segments)) {
            break;
          }
        } else {
          // look for the out_edge in HYBRID.
          uint32_t key =
              hybrid_key(out_edge.from_cluster_id, out_edge.out_edge_pos);
          const auto it = d.hybrid_map.find(key);
          CHECK_S(it != d.hybrid_map.end());
          VisitedEdge vis = it->second;
          segments.push_back({HYBRID, {}, {}});
          auto& hybrid_edges = segments.back().hybrid_edges;
          while (true) {
            hybrid_edges.push_back({key, vis});
            if (vis.prev_source != HYBRID) {
              break;
            }
            key = vis.prev_key_or_v_idx;
            const auto it = d.hybrid_map.find(key);
            CHECK_S(it != d.hybrid_map.end());
            vis = it->second;
          }
          std::reverse(hybrid_edges.begin(), hybrid_edges.end());
          LOG_S(INFO) << "Add hybrid segment of length " << hybrid_edges.size();
        }
      }
    }
    std::reverse(segments.begin(), segments.end());
    PrintSegments(mg, segments);
    MMRoutingResult res = ExpandHybridClusters(mg, segments, d);
    AddStatistics(mg, d, &res);
    return res;
  }

  static void PrintSegments(const MMGraph& mg,
                            const std::vector<Segment>& segments) {
    for (const Segment& segment : segments) {
      if (segment.source == HYBRID) {
        LOG_S(INFO) << "***** HYBRID Segment:";

        for (size_t i = 0; i < segment.hybrid_edges.size(); ++i) {
          const HybridEdge he = segment.hybrid_edges.at(i);
          const MMOutgoingEdge& out_edge = out_edge_from_hybrid_key(mg, he.key);
          LOG_S(INFO) << "min_metric:" << he.vis.min_metric << " "
                      << out_edge.DebugString();
        }
      } else {
        LOG_S(INFO) << "***** "
                    << (segment.source == START ? "START Segment"
                                                : "TARGET Segment");
        for (size_t i = 0; i < segment.res.full_edges.size(); ++i) {
          if (i >= 3 && i < segment.res.full_edges.size() - 3) {
            if (i == 3) {
              LOG_S(INFO) << "       ... ("
                          << (segment.res.full_edges.size() - 6) << " omitted)";
            }
          } else {
            LOG_S(INFO) << absl::StrFormat(
                "%5llu. min_metric:%u edge_metric:%u %s", i + 1,
                segment.res.min_metrics.at(i), segment.res.edge_metric(i),
                segment.res.full_edges.at(i).DebugString(mg));
          }
        }
      }
    }
  }

  template <typename T>
  static void AppendVector(const std::vector<T>& src, size_t offset,
                           std::vector<T>* dest) {
    // dest->insert(dest->end(), src.cbegin() + offset, src.cend());
    CHECK_S(!dest->empty());
    dest->pop_back();
    dest->insert(dest->end(), src.cbegin(), src.cend());
  }

  static MMRoutingResult ExpandHybridClusters(
      const MMGraph& mg, const std::vector<Segment>& segments, RouterData& d) {
    d.start_time_expand_clusters = absl::Now();
    MMRoutingResult res;
    for (size_t seg_pos = 0; seg_pos < segments.size(); ++seg_pos) {
      const Segment& seg = segments.at(seg_pos);
      if (seg.source != HYBRID) {
        if (seg_pos == 0) {
          res = seg.res;
        } else {
          // LOG_S(INFO) << "Ignore duplicate edge "
          //             << seg.res.full_edges.front().DebugString(mg);
          constexpr uint32_t off = 1;  // First edge is a dup.
          AppendVector(seg.res.full_edges, off, &res.full_edges);
          AppendVector(seg.res.min_metrics, off, &res.min_metrics);
          // AppendVector(seg.res.edge_metric, off, &res.edge_metric);
          if (seg_pos == segments.size() - 1) {
            res.target = seg.res.target;
            res.final_metric = seg.res.final_metric;
            res.target_is_anchor = seg.res.target_is_anchor;
          }
        }
      } else {
        LOG_S(INFO) << "***** HYBRID Segment:";
        CHECK_GT_S(seg_pos, 0);
        CHECK_S(segments.at(seg_pos - 1).source != HYBRID);
        CHECK_S(!res.full_edges.empty());

        const MMOutgoingEdge* prev_out_edge =
            &(res.full_edges.back().ToOutgoingEdge(mg));
        for (size_t i = 0; i < seg.hybrid_edges.size(); ++i) {
          const MMIncomingEdge& in_edge =
              mg.out_edge_to_in_edge(*prev_out_edge);
          const HybridEdge he = seg.hybrid_edges.at(i);
          const MMOutgoingEdge& out_edge = out_edge_from_hybrid_key(mg, he.key);
          // Now route from in_edge to out_edge.
          // TODO: Get this instead from the memfile, once we store the
          // routes.
          const MMCluster& mc = mg.mc(in_edge.to_cluster_id);
          GeoAnchor start;
          start.AddEdge(mg, 1.0, in_edge.ToFullEdge(mg));
          GeoAnchor target;
          target.AddEdge(mg, 1.0, out_edge.ToFullEdge(mg));
          LOG_S(INFO) << "Route from start: "
                      << start.edge_points().front().DebugString(mc, 0, 0);
          LOG_S(INFO) << "Route to target: "
                      << target.edge_points().front().DebugString(mc, 0, 0);
          MMClusterWrapper mcw(mc, VH_MOTORCAR, RoutingMetricTime(),
                               /*include_dead_ends=*/true);
          MMClusterRouter router(mcw, {.handle_restricted_access = true,
                                       .include_dead_end = false});
          LOG_S(INFO) << "Start subrouting at " << res.min_metrics.back();
          MMClusterRouterStatus status =
              router.Route(start, target, res.min_metrics.back());
          CHECK_S(status.finished);
          CHECK_S(status.found);
          MMRoutingResult sub_res = router.GetRoutingResult(status.last_v_idx);
          AppendVector(sub_res.full_edges, 1, &res.full_edges);
          AppendVector(sub_res.min_metrics, 1, &res.min_metrics);
          // AppendVector(sub_res.edge_metric, 1, &res.edge_metric);
          prev_out_edge = &out_edge;
        }
      }
    }
    CHECK_EQ_S(res.full_edges.size(), res.min_metrics.size());
    // CHECK_EQ_S(res.full_edges.size(), res.edge_metric.size());
    LogPath(mg, res);
    return res;
  }

  static void LogPath(const MMGraph& mg, const MMRoutingResult& res) {
    LOG_S(INFO) << "**************** PATH ***************";
    for (size_t i = 0; i < res.full_edges.size(); ++i) {
      if (i == 5 && res.full_edges.size() > 10) {
        LOG_S(INFO) << "       ... (" << res.full_edges.size() - 10
                    << " omitted)";
        i = res.full_edges.size() - 5;
      }
      LOG_S(INFO) << absl::StrFormat(
          "%5i. min_metric:%u edge_metric:%u %s", i + 1, res.min_metrics.at(i),
          res.edge_metric(i), res.full_edges.at(i).DebugString(mg));
    }
    LOG_S(INFO) << "*************************************";
  }
};
