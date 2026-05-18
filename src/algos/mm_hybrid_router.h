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
// If routing finalizes an outgoing edge in an expanded cluster, then there are
// two cases:
//   (1) The target cluster of the edge is expanded. In this case, an incoming
//   edge (which is a duplicate of the outgoing edge) is added to the target
//   cluster router.
//   (2) The target cluster is not expanded. In this case, all hybrid edges
//   starting at the target border node are added to the hybrid router.
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

  // Find or allocate a visited edge for 'key'.
  static inline VisitedEdge& FindOrAllocEdge(
      uint32_t key, absl::flat_hash_map<uint32_t, VisitedEdge>* hybrid_map) {
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

  struct RouterData {
    // Only entries 0 and 1 are used.
    std::shared_ptr<MMClusterWrapper> mcw[ROUTER_TYPE_MAX];
    // Only entries 0 and 1 are used.
    std::shared_ptr<MMClusterRouter> router[ROUTER_TYPE_MAX];
    std::priority_queue<MMHybridRouter::QueuedEdge,
                        std::vector<MMHybridRouter::QueuedEdge>, MetricCmpEdge>
        hybrid_queue;
    absl::flat_hash_map<uint32_t, VisitedEdge> hybrid_map;

    void Init(const MMGraph& mg, const MMGeoAnchor& start_anchor,
              const MMGeoAnchor& target_anchor) {
      const MMClusterRouter::Options opt = {.handle_restricted_access = true,
                                            .include_dead_end = true};
      // Anchors should follow this scheme:
      //   All edges have to belong to the same cluster
      //   The start node of the first edge dictates the cluster.
      const MMGeoAnchor::Info start_info = start_anchor.GetInfo(mg);
      const MMGeoAnchor::Info target_info = target_anchor.GetInfo(mg);
      CHECK_S(
          !start_anchor.edge_points.empty() && start_info.all_same_cluster &&
          !target_anchor.edge_points.empty() && target_info.all_same_cluster);

      // Create a start router.
      mcw[START].reset(
          new MMClusterWrapper(mg.clusters.at(start_info.cluster_id),
                               VH_MOTORCAR, RoutingMetricTime(),
                               /*include_dead_ends=*/true));
      router[START].reset(new MMClusterRouter(*mcw[START], opt));

      if (start_info.cluster_id == target_info.cluster_id) {
        // Initialise start and target edges in the one router we have.
        router[START]->RouteInit(start_anchor, target_anchor);
        router[TARGET] = router[START];
        LOG_S(INFO) << "start and target cluster identical";
      } else {
        // Create a target_router.
        mcw[TARGET].reset(
            new MMClusterWrapper(mg.clusters.at(target_info.cluster_id),
                                 VH_MOTORCAR, RoutingMetricTime(),
                                 /*include_dead_ends=*/true));
        router[TARGET].reset(new MMClusterRouter(*mcw[TARGET], opt));
        router[START]->RouteInit(start_anchor, {});
        router[TARGET]->RouteInit({}, target_anchor);
        LOG_S(INFO) << "start and target cluster different";
      }
    }
  };

  // template <RouterType source>
  static inline void HandleOutgoingEdge(const MMGraph& mg,
                                        const uint32_t source_min_metric,
                                        const MMOutgoingEdge& out_edge,
                                        RouterType source, uint32_t source_key,
                                        RouterData& d) {
    const MMIncomingEdge& in_edge = mg.find_incoming_edge(out_edge);

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
      // Copy edge to TARGET.
      d.router[TARGET]->AddIncomingEdge(in_edge, source_min_metric);
      // CHECK_EQ_S(min_metric[TARGET], source_min_metric);
    } else if (source != START &&
               out_edge.to_cluster_id == d.mcw[START]->mc.cluster_id) {
      // Copy edge to START.
      LOG_S(INFO) << "CC0:" << source << " " << source_min_metric;
      d.router[START]->AddIncomingEdge(in_edge, source_min_metric);
      // CHECK_EQ_S(min_metric[START], source_min_metric);
    } else {
      // Route edges in HYBRID.
      const MMCluster& mc = mg.clusters.at(out_edge.to_cluster_id);
      for (uint32_t og_edge_idx = 0; og_edge_idx < mc.out_edges.size();
           ++og_edge_idx) {
        const MMOutgoingEdge& outleg = mc.out_edges.at(og_edge_idx);
        const uint32_t cross_metric = mc.get_path_metric(in_edge, outleg);
        const uint32_t key = hybrid_key(mc.cluster_id, og_edge_idx);
        VisitedEdge& vis = FindOrAllocEdge(key, &d.hybrid_map);
        if (cross_metric != INFU32 &&
            source_min_metric + cross_metric < vis.min_metric) {
          CHECK_S(!vis.done) << source_min_metric << " " << cross_metric << " "
                             << vis.min_metric;
          const uint32_t new_metric = source_min_metric + cross_metric;
          LOG_S(INFO) << "Find or alloc hybrid egde <" << key
                      << "> with new metric " << new_metric << " old metric "
                      << vis.min_metric;
          vis.min_metric = new_metric;
          vis.prev_source = source;
          vis.prev_key_or_v_idx = source_key;
          d.hybrid_queue.emplace(new_metric, key);
        }
      }
    }
  }

  void Route(const MMGraph& mg, const MMGeoAnchor& start_anchor,
             const MMGeoAnchor& target_anchor) {
    FuncTimer timer("MMHybridRouter::Route()", __FILE__, __LINE__);
    LOG_S(INFO) << "start anchor has " << start_anchor.edge_points.size()
                << " edges";
    for (const auto& ep : start_anchor.edge_points) {
      LOG_S(INFO) << "  " << ep.fe.DebugString(mg) << " frac: " << ep.fraction;
    }
    LOG_S(INFO) << "target anchor has " << target_anchor.edge_points.size()
                << " edges";
    for (const auto& ep : target_anchor.edge_points) {
      LOG_S(INFO) << "  " << ep.fe.DebugString(mg) << " frac: " << ep.fraction;
    }

    RouterData d;
    d.Init(mg, start_anchor, target_anchor);

    while (true) {
      uint32_t min_metric[ROUTER_TYPE_MAX];
      min_metric[START] = d.router[START]->QueueMinMetric();
      min_metric[TARGET] = d.router[TARGET]->QueueMinMetric();
      min_metric[HYBRID] =
          !d.hybrid_queue.empty() ? d.hybrid_queue.top().min_metric : INFU32;

      // Start cluster routing.
      LOG_S(INFO) << absl::StrFormat(
          "Hybrid Loop start:%u (%llu) target:%u (%llu) hybrid:%u (%llu)",
          min_metric[START], d.router[START]->QueueSize(), min_metric[TARGET],
          d.router[TARGET]->QueueSize(), min_metric[HYBRID],
          d.hybrid_queue.size());

      if (min_metric[START] != INFU32 &&
          min_metric[START] <= min_metric[TARGET] &&
          min_metric[START] <= min_metric[HYBRID]) {
        LOG_S(INFO) << "Route START";
        const uint32_t v_idx = d.router[START]->QueueMinVIdx();
        if (d.router[START]->IsOutgoingEdge(v_idx)) {
          const MMOutgoingEdge& out_edge = d.mcw[START]->mc.find_outgoing_edge(
              d.router[START]->GetGraphEdgeIdx(v_idx));
          HandleOutgoingEdge(mg, min_metric[START], out_edge, START,
                             /*source_key=*/v_idx, d);
        }
        // Finalize the edge.
        RouterStatus rs = d.router[START]->RouteOneStep();
        LOG_S(INFO) << "Route start cluster one step metric "
                    << min_metric[START] << " to "
                    << d.router[START]->QueueMinMetric();
        if (rs.finished && rs.found) {
          LOG_S(INFO) << "Found target edge";

          break;
        }

      } else if (min_metric[TARGET] != INFU32 &&
                 min_metric[TARGET] <= min_metric[START] &&
                 min_metric[TARGET] <= min_metric[HYBRID]) {
        LOG_S(INFO) << "Route TARGET";
        // Target cluster routing.
        const uint32_t v_idx = d.router[TARGET]->QueueMinVIdx();
        if (d.router[TARGET]->IsOutgoingEdge(v_idx)) {
          const MMOutgoingEdge& out_edge = d.mcw[TARGET]->mc.find_outgoing_edge(
              d.router[TARGET]->GetGraphEdgeIdx(v_idx));
          HandleOutgoingEdge(mg, min_metric[TARGET], out_edge, TARGET,
                             /*source_key=*/v_idx, d);
        }
        // Finalize the edge.
        RouterStatus rs = d.router[TARGET]->RouteOneStep();
        LOG_S(INFO) << "Route target cluster one step metric "
                    << min_metric[TARGET] << " to "
                    << d.router[TARGET]->QueueMinMetric();
        if (rs.finished && rs.found) {
          LOG_S(INFO) << "Found target edge";
          break;
        }

      } else if (min_metric[HYBRID] != INFU32 &&
                 min_metric[HYBRID] <= min_metric[START] &&
                 min_metric[HYBRID] <= min_metric[TARGET]) {
        LOG_S(INFO) << "Route HYBRID";
        // Hybrid routing
        const QueuedEdge e = d.hybrid_queue.top();
        LOG_S(INFO) << "FF0 before:" << d.hybrid_queue.top().min_metric;
        d.hybrid_queue.pop();

        LOG_S(INFO) << "FF0 after:"
                    << (d.hybrid_queue.empty()
                            ? 0
                            : d.hybrid_queue.top().min_metric);
        auto it = d.hybrid_map.find(e.key);
        CHECK_S(it != d.hybrid_map.end());
        VisitedEdge& vis = it->second;
        if (vis.done) {
          // Old entry, just skip it.
          continue;
        }
        LOG_S(INFO) << "Finalize hybrid edge <" << e.key << "> at metric "
                    << e.min_metric;
        CHECK_EQ_S(e.min_metric, vis.min_metric);
        LOG_S(INFO) << "min_metric[HYBRID] is " << min_metric[HYBRID];
        vis.done = 1;
        const MMCluster& mc = mg.clusters.at(cluster_id_from_hybrid_key(e.key));
        const MMOutgoingEdge& out_edge =
            mc.out_edges.at(og_edge_idx_from_hybrid_key(e.key));
        HandleOutgoingEdge(mg, e.min_metric, out_edge, HYBRID,
                           /*source_key=*/e.key, d);

      } else {
        // All queues empty.
        break;
      }
    }
    // return {};
  }

 private:
  static inline uint32_t hybrid_key(uint32_t cluster_id, uint32_t og_edge_idx) {
    return (cluster_id << 10) + og_edge_idx;
  }
  static inline uint32_t cluster_id_from_hybrid_key(uint32_t hybrid_key) {
    return hybrid_key >> 10;
  }
  static inline uint32_t og_edge_idx_from_hybrid_key(uint32_t hybrid_key) {
    return hybrid_key & ((1u << 10) - 1);
  }
};
