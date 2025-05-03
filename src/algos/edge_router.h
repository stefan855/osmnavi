#pragma once

#include <fstream>
#include <optional>
#include <queue>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "algos/edge_key.h"
#include "algos/routing_defs.h"
#include "algos/routing_metric.h"
#include "algos/tarjan.h"
#include "base/util.h"
#include "geometry/distance.h"
#include "graph/graph_def.h"

// A router that can run in Dijkstra or in AStar mode, and may use precomputed
// routes from clusters (see 'hybrid').
//
// The router is edge based, i.e. it can handle turn costs.
//
// In AStar mode, the router uses a heuristic to estimate a lower bound (see
// 'heuristic_to_target') that is needed to travel from the current node to the
// target.
class EdgeRouter {
 public:
  // This exists once for every edge.
  struct VisitedEdge {
    GEdgeKey key;
    std::uint32_t done : 1;            // 1 <=> edge has been finalized.
    std::uint32_t shortest_route : 1;  // 1 <=> edge is part of shortest route.
    std::uint32_t restricted_obsolete : 1;
    std::uint32_t prev_v_idx;  // Predecessor in visited_edges vector.
    std::uint32_t min_metric;  // The minimal metric seen so far.
    std::uint32_t heuristic_to_target : 30;
  };

  // This might exist multiple times for each 'v_idx', when a node gets
  // reinserted into the priority queue with a lower priority.
  struct QueuedEdge {
    std::uint32_t metric;
    std::uint32_t v_idx;  // Index into visited_edges vector.
  };

  EdgeRouter(const Graph& g, int verbosity = 1)
      : g_(g),
        pq_(&MetricCmp),
        target_visited_index_(INFU32),
        verbosity_(verbosity) {
    Clear();
  }

  std::string AlgoName(const RoutingOptions& opt) {
    return opt.use_astar_heuristic ? "e_astar" : "e_dijkstra";
  }

  std::string Name(const RoutingMetric& metric, const RoutingOptions& opt) {
    return absl::StrFormat("%s-%s-%s%s", AlgoName(opt), metric.Name(),
                           opt.backward_search ? "backward" : "forward",
                           opt.hybrid.on ? "-hybrid" : "");
  }

  RoutingResult Route(std::uint32_t start_idx, std::uint32_t target_idx,
                      const RoutingMetric& metric, const RoutingOptions& opt) {
    if (verbosity_ > 0) {
      LOG_S(INFO) << absl::StrFormat(
          "Start routing from %u to %u (%s)", g_.nodes.at(start_idx).node_id,
          g_.nodes.at(target_idx).node_id, Name(metric, opt));
    }
    Clear();
    if (start_idx == target_idx) {
      return {.found = true,
              .found_distance = 0,
              .num_shortest_route_nodes = 0,
              .num_visited = 0};
    }

    Context ctx = {.opt = opt,
                   .metric = metric,
                   .target_lat = g_.nodes.at(target_idx).lat,
                   .target_lon = g_.nodes.at(target_idx).lon};

    // Add all outgoing edges of the start node to the queue.
    ExpandForwardEdges(ctx, {.other_idx = start_idx,
                             .metric = 0,
                             .v_idx = INFU32,
                             .restricted = true,
                             .in_target_area = false});

    // Store the best edge seen so far that points to target_idx.
    uint32_t target_best_v_idx = INFU32;
    uint32_t target_best_metric = INFU32;
    while (!pq_.empty()) {
      const QueuedEdge qe = pq_.top();
      pq_.pop();
      VisitedEdge& ve = visited_edges_.at(qe.v_idx);
      if (ve.min_metric >= target_best_metric && target_best_v_idx != INFU32) {
        // We have found a shortest way.
        break;
      }
      if (ve.done == 1) {
        continue;  // "old" entry in priority queue.
      }
      ve.done = 1;
      if (verbosity_ >= 3) {
        LOG_S(INFO) << "======";
        LOG_S(INFO) << "POP   " << VisitedEdgeToString(ve);
      }

      const uint32_t other_node_idx = ve.key.GetToIdx(g_, ctr_deduper_);
      if (other_node_idx == target_idx) {
        // This edge ends at the target node. Remember it if it is the fastest.
        if (ve.min_metric < target_best_metric) {
          target_best_metric = ve.min_metric;
          target_best_v_idx = qe.v_idx;
        }
      }

      if (!ctx.opt.backward_search) {
        ExpandForwardEdges(ctx, {.other_idx = other_node_idx,
                                 .metric = ve.min_metric,
                                 .v_idx = qe.v_idx,
                                 .restricted = ve.restricted_obsolete != 0,
                                 .in_target_area = ve.key.GetBit()});
      } else {
        CHECK_S(false);
        // ExpandNeighboursBackward(qedge, ctx, vedge);
      }
    }

    // Shortest route found?
    if (target_best_v_idx != INFU32) {
      return CreateResult(target_best_v_idx, target_best_metric);
    } else {
      if (verbosity_ > 0) {
        LOG_S(INFO) << "Route not found";
      }
      return {.num_visited = (uint32_t)visited_edges_.size()};
    }
  }

  void SaveSpanningTreeSegments(const std::string& filename) {
    if (verbosity_ > 0) {
      LOG_S(INFO) << "Write route to " << filename;
    }
    std::ofstream myfile;
    myfile.open(filename, std::ios::trunc | std::ios::binary | std::ios::out);
    // Draw the shortest route in red over the spanning tree in black.
    for (int output_shortest = 0; output_shortest <= 1; output_shortest++) {
      for (const VisitedEdge& ve : visited_edges_) {
        if (ve.shortest_route == (output_shortest > 0)) {
          const GNode a = ve.key.FromNode(g_, ctr_deduper_);
          const GNode b = ve.key.ToNode(g_, ctr_deduper_);
          myfile << absl::StrFormat("line,%s,%d,%d,%d,%d\n",
                                    ve.shortest_route ? "red" : "black", a.lat,
                                    a.lon, b.lat, b.lon);
        }
      }
    }
    myfile.close();
  }

  const VisitedEdge& GetVEdge(uint32_t v_idx) const {
    return visited_edges_.at(v_idx);
  }

  // Ugly but currently needed when accessing data in an edge key.
  const CTRDeDuper& GetCTRDeDuper() const { return ctr_deduper_; }

  std::vector<uint32_t> GetShortestPathNodeIndexes(const RoutingResult& res) {
    std::vector<uint32_t> v;
    if (!res.route_v_idx.empty()) {
      v.reserve(res.route_v_idx.size() + 1);
      v.push_back(visited_edges_.at(res.route_v_idx.front())
                      .key.GetFromIdx(g_, ctr_deduper_));
      for (uint32_t v_idx : res.route_v_idx) {
        v.push_back(visited_edges_.at(v_idx).key.GetToIdx(g_, ctr_deduper_));
      }
    }
    return v;
  }

  void TurnRestrictionSpecialStats(uint32_t* num_len_gt_1, uint32_t* max_len) {
    *num_len_gt_1 = 0;
    *max_len = 0;
    for (uint32_t i = 0; i < ctr_deduper_.num_unique(); ++i) {
      const ActiveCtrs& a = ctr_deduper_.GetObj(i);
      *num_len_gt_1 += a.size() > 1;
      *max_len = std::max(*max_len, static_cast<uint32_t>(a.size()));
    }
  }

 private:
  friend void TestClusterRouteDoubleEdge();

  struct Context {
    const RoutingOptions opt;
    const RoutingMetric& metric;
    const int32_t target_lat;
    const int32_t target_lon;
    // RestrictedAccessArea target_restricted_access;
  };

  //
  struct PrevEdgeData {
    const uint32_t other_idx;
    const uint32_t metric;
    const uint32_t v_idx;
    const bool restricted;
    const bool in_target_area;
  };

  static bool MetricCmp(const QueuedEdge& left, const QueuedEdge& right) {
    return left.metric > right.metric;
  }

  std::string VisitedEdgeToString(const VisitedEdge& ve) {
    std::string tr_str;
    if (ve.key.GetType() == GEdgeKey::TURN_RESTRICTION) {
      tr_str = ActiveCtrsToDebugString(
          g_, ctr_deduper_.GetObj(ve.key.GetCtrConfigId()));
    }

    return absl::StrFormat(
        "Edge from:%10u to:%10u metric:%10lld type:%s cluster:%u->%u "
        "t-area:%d done:%d %s",
        ve.key.FromNode(g_, ctr_deduper_).node_id,
        ve.key.ToNode(g_, ctr_deduper_).node_id,
        (ve.min_metric == INFU32) ? -1ll : (int64_t)ve.min_metric,
        ve.key.IsClusterEdge() ? "clus"
        : ve.key.GetEdge(g_, ctr_deduper_).car_label == GEdge::LABEL_FREE
            ? "free"
            : "rest",
        ve.key.FromNode(g_, ctr_deduper_).cluster_id,
        ve.key.ToNode(g_, ctr_deduper_).cluster_id, ve.key.GetBit(), ve.done,
        tr_str);
  }

  RoutingResult CreateResult(uint32_t target_v_idx, uint32_t target_metric) {
    RoutingResult result = {
        .found = true,
        .found_distance = target_metric,
        .num_shortest_route_nodes = 1,
        .num_visited = (uint32_t)visited_edges_.size(),
        .num_complex_turn_restriction_keys = ctr_deduper_.num_unique(),
        .complex_turn_restriction_keys_reduction_factor =
            static_cast<float>(ctr_deduper_.num_unique()) /
            std::max(1u, ctr_deduper_.num_added())};

    auto current_idx = target_v_idx;
    while (current_idx != INFU32) {
      result.route_v_idx.push_back(current_idx);
      VisitedEdge& ve = visited_edges_.at(current_idx);
      ve.shortest_route = 1;
      current_idx = ve.prev_v_idx;
    }
    result.num_shortest_route_nodes = result.route_v_idx.size() + 1;
    std::reverse(result.route_v_idx.begin(), result.route_v_idx.end());
    if (verbosity_ >= 1) {
      if (verbosity_ >= 2) {
        uint32_t count = 0;
        LOG_S(INFO) << "====== Result ======";
        for (uint32_t ve_idx : result.route_v_idx) {
          const VisitedEdge& ve = visited_edges_.at(ve_idx);
          LOG_S(INFO) << absl::StrFormat("%5u %s", ++count,
                                         VisitedEdgeToString(ve));
        }
      }
      LOG_S(INFO) << absl::StrFormat("Route found, visited edges:%u metric:%u",
                                     visited_edges_.size(),
                                     result.found_distance);
    }
    return result;
  }

  std::uint32_t FindOrAddVisitedEdge(GEdgeKey key, bool use_astar_heuristic) {
    // Prevent doing two lookups by following
    // https://stackoverflow.com/questions/1409454.
    const auto iter = edgekey_to_v_idx_.insert(EdgeIdMap::value_type(
        key.UInt64Key(g_, ctr_deduper_), visited_edges_.size()));
    if (iter.second) {
      // Key didn't exist and was inserted, so add it to visited_edges_ too.
      visited_edges_.push_back(
          {.key = key,
           .done = 0,
           .shortest_route = 0,
           .restricted_obsolete = 0,
           .prev_v_idx = INFU32,
           .min_metric = INFU32,
           .heuristic_to_target = use_astar_heuristic ? INFU30 : 0});
    }
    return iter.first->second;
  }

  // Compute the metric to get from 'node' to the target node, on a
  // hypothetical straight street which allows maximum speed.
  static inline std::uint32_t ComputeHeuristicToTarget(const GNode& node,
                                                       const Context& ctx) {
    // TODO: use country specific maxspeed.
    // TODO: support mexspeed for different vehicles.
    // SOLUTION: Should use per (country, vehicle) maxspeed. If target
    // is in other country then use mix.
    static const WaySharedAttrs g_wsa =
        WaySharedAttrs::Create({.dir = 1, .access = ACC_YES, .maxspeed = 120});
    return ctx.metric.Compute(
        g_wsa, ctx.opt.vt, DIR_FORWARD,
        {.distance_cm = static_cast<uint64_t>(
             1.00 * calculate_distance(node.lat, node.lon, ctx.target_lat,
                                       ctx.target_lon)),
         .contra_way = 0});
  }

  std::optional<GEdgeKey> CreateNextEdgeKey(const Context& ctx,
                                            const PrevEdgeData prev,
                                            const GEdge& next_edge,
                                            bool next_in_target_area) {
    const bool active_key =
        (prev.v_idx != INFU32 && visited_edges_.at(prev.v_idx).key.GetType() ==
                                     GEdgeKey::TURN_RESTRICTION);

    if (!active_key && !next_edge.complex_turn_restriction_trigger) {
      // Common case, no turn restriction active, no turn restriction triggered.
      return GEdgeKey::CreateGraphEdge(g_, prev.other_idx, next_edge,
                                       next_in_target_area);
    }

    ActiveCtrs active_ctrs;
    if (active_key) {
      // We have active turn restrictions. Check if they forbid the next edge.
      active_ctrs = ctr_deduper_.GetObj(
          visited_edges_.at(prev.v_idx).key.GetCtrConfigId());
      if (!ActiveCtrsAddNextEdge(g_, next_edge, &active_ctrs)) {
        // Forbidden turn!
        return std::nullopt;
      }
    }

    if (next_edge.complex_turn_restriction_trigger) {
      // Find new triggering turn restrictions.
      TurnRestriction::TREdge key = {.from_node_idx = prev.other_idx,
                                     .way_idx = next_edge.way_idx,
                                     .to_node_idx = next_edge.other_node_idx};
      auto it = g_.complex_turn_restriction_map.find(key);
      if (it != g_.complex_turn_restriction_map.end()) {
        uint32_t ctr_idx = it->second;
        do {
          const TurnRestriction& tr = g_.complex_turn_restrictions.at(ctr_idx);
          if (tr.GetTriggerKey() != key) {
            // Check that we iterated at least once.
            CHECK_NE_S(ctr_idx, it->second);
            break;
          }
          active_ctrs.push_back({.ctr_idx = ctr_idx, .position = 0});
        } while (++ctr_idx < g_.complex_turn_restrictions.size());
      }
    }

    if (active_ctrs.empty()) {
      // No active ctrs exist. Return normal edge key.
      return GEdgeKey::CreateGraphEdge(g_, prev.other_idx, next_edge,
                                       next_in_target_area);
    } else {
      uint32_t id = ctr_deduper_.Add(active_ctrs);
      return GEdgeKey::CreateTurnRestrictionEdge(g_, id, 0,
                                                 next_in_target_area);
    }
  }

  // Expand the forward edges.
  void ExpandForwardEdges(const Context& ctx, const PrevEdgeData prev) {
    const GNode& expansion_node = g_.nodes.at(prev.other_idx);
    if (verbosity_ >= 3) {
      LOG_S(INFO) << "EXPA  node:" << expansion_node.node_id
                  << " cluster:" << expansion_node.cluster_id;
    }

    // The node with 'uturn_node_idx' represents a forbidden u-turn.
    uint32_t uturn_forbidden_node_idx = INFU32;
    // Compute a bitset that has bits for all the allowed edge offsets.
    uint32_t allowed_offset_bits = ~0;  // all '1'.

    if (prev.v_idx != INFU32) {
      const GEdgeKey& prev_key = visited_edges_.at(prev.v_idx).key;
      if (!prev_key.IsClusterEdge()) {
        if (expansion_node.simple_turn_restriction_via_node) {
          auto iter = g_.simple_turn_restriction_map.find(
              {.from_node_idx = prev_key.GetFromIdx(g_, ctr_deduper_),
               .way_idx = prev_key.GetEdge(g_, ctr_deduper_).way_idx,
               .to_node_idx = prev.other_idx});
          if (iter != g_.simple_turn_restriction_map.end()) {
            if (verbosity_ >= 3) {
              LOG_S(INFO) << "*** TR found relation_id="
                          << iter->second.osm_relation_id;
            }
            allowed_offset_bits = iter->second.allowed_edge_bits;
          }
        } else if (!prev_key.GetEdge(g_, ctr_deduper_).car_uturn_allowed) {
          // Store a uturn node idx that is forbidden. This is not needed when a
          // bitset from simple turn restrictions above is available - it
          // already contains the uturn information.
          //
          // TODO: Use way_idx in addition to uturn_forbidden_node_idx.
          uturn_forbidden_node_idx = prev_key.GetFromIdx(g_, ctr_deduper_);
        }
      }
    }

    // for (const GEdge& curr_ge : gnode_forward_edges(g_, prev.other_idx)) {
    for (uint64_t offset = 0; offset < expansion_node.num_edges_forward;
         ++offset) {
      const GEdge& curr_ge =
          g_.edges.at(expansion_node.edges_start_pos + offset);

      if (curr_ge.other_node_idx == uturn_forbidden_node_idx ||
          ((1u << offset) & allowed_offset_bits) == 0) {
        // TODO: allow u-turn iff this is the only way out, the node has only
        // this one outgoing edge.
        continue;
      }

      const WaySharedAttrs& wsa = GetWSA(g_, curr_ge.way_idx);
      if (RoutingRejectEdge(g_, ctx.opt, expansion_node, prev.other_idx,
                            curr_ge, wsa, EDGE_DIR(curr_ge))) {
        continue;
      }

      bool next_in_target_area = prev.in_target_area;
      if (prev.restricted != (curr_ge.car_label != GEdge::LABEL_FREE)) {
        if (!CheckRestrictedAccessTransition(ctx, prev, curr_ge,
                                             &next_in_target_area)) {
          continue;
        }
      }

      // Create the edge key for the current edge, also handles complex turn
      // restrictions.
      const std::optional<GEdgeKey> next_edge_key =
          CreateNextEdgeKey(ctx, prev, curr_ge, next_in_target_area);
      if (!next_edge_key.has_value()) {
        // ABORT_S() << "no edge key";
        continue;
      }

      // We want to expand this edge!
      std::uint32_t v_idx = FindOrAddVisitedEdge(next_edge_key.value(),
                                                 ctx.opt.use_astar_heuristic);
#if 0
      std::uint32_t v_idx = FindOrAddVisitedEdge(
          GEdgeKey::CreateGraphEdge(g_, prev.other_idx, curr_ge,
                                    next_in_target_area),
          ctx.opt.use_astar_heuristic);
#endif
      VisitedEdge& ve = visited_edges_.at(v_idx);
      if (verbosity_ >= 3) {
        LOG_S(INFO) << "CONS  " << VisitedEdgeToString(ve);
      }
      if (ve.done) {
        continue;
      }

      std::uint32_t new_metric =
          prev.metric +
          ctx.metric.Compute(wsa, ctx.opt.vt, EDGE_DIR(curr_ge), curr_ge);

      if (new_metric < ve.min_metric) {
        ve.min_metric = new_metric;
        ve.prev_v_idx = prev.v_idx;
        ve.restricted_obsolete = (curr_ge.car_label != GEdge::LABEL_FREE);

        // In A* mode, compute heuristic distance from new node to target.
        if (ve.heuristic_to_target == INFU30) {
          ve.heuristic_to_target = ComputeHeuristicToTarget(
              g_.nodes.at(curr_ge.other_node_idx), ctx);
          CHECK_LT_S(ve.heuristic_to_target, INFU30);
        }
        pq_.emplace(new_metric + ve.heuristic_to_target, v_idx);
        if (verbosity_ >= 3) {
          LOG_S(INFO) << "PUSH  " << VisitedEdgeToString(ve);
        }
      }
    }

    if (ctx.opt.hybrid.on && expansion_node.cluster_border_node &&
        expansion_node.cluster_id != ctx.opt.hybrid.start_cluster_id &&
        expansion_node.cluster_id != ctx.opt.hybrid.target_cluster_id) {
      if (prev.v_idx != INFU32 &&
          visited_edges_.at(prev.v_idx).key.IsClusterEdge()) {
        // We just traversed a cluster, don't enter the same cluster again.
        return;
      }

      // Expand the 'virtual' links within the cluster, using the precomputed
      // distances.
      const GCluster& cluster = g_.clusters.at(expansion_node.cluster_id);
      const std::vector<std::uint32_t>& v_dist =
          cluster.GetBorderNodeDistances(prev.other_idx);

      for (size_t i = 0; i < cluster.border_nodes.size(); ++i) {
        uint32_t other_idx = cluster.border_nodes.at(i);
        if (other_idx == prev.other_idx) {
          // Self link, so ignore.
          continue;
        }

        const std::uint32_t dist = v_dist.at(i);
        if (dist == INFU32) continue;  // node is not reachable.
        CHECK_LT_S(dist, INFU30);      // Not plausible if so big.

        // TODO: check for potential overflow.
        std::uint32_t v_idx = FindOrAddVisitedEdge(
            GEdgeKey::CreateClusterEdge(g_, prev.other_idx, i, /*bit=*/false),
            ctx.opt.use_astar_heuristic);

        VisitedEdge& ve = visited_edges_.at(v_idx);
        if (verbosity_ >= 3) {
          LOG_S(INFO) << "CONS* " << VisitedEdgeToString(ve);
        }
        std::uint32_t new_metric = prev.metric + dist;

        if (!ve.done && new_metric < ve.min_metric) {
          // During routing, cluster edges compete for the shortest time at the
          // target node, i.e. the start node and the edge are irrelevant.
          // Therefore, the UInt64Key() of a cluster edge contains (cluster_id,
          // offset), not (start_node, offset).
          // So here, we have to set again the current start node, because it
          // might be different from what is in an already existing edge.
          ve.key = GEdgeKey::CreateClusterEdge(g_, prev.other_idx, i,
                                               /*bit=*/false);
          ve.min_metric = new_metric;
          ve.prev_v_idx = prev.v_idx;
          // ve.restricted is already false.
          CHECK_S(!ve.restricted_obsolete);

          // Compute heuristic distance from new node to target.
          if (ve.heuristic_to_target == INFU30) {
            ve.heuristic_to_target =
                ComputeHeuristicToTarget(ve.key.ToNode(g_, ctr_deduper_), ctx);
            CHECK_LT_S(ve.heuristic_to_target, INFU30);
          }
          pq_.emplace(new_metric + ve.heuristic_to_target, v_idx);
          if (verbosity_ >= 3) {
            LOG_S(INFO) << "PUSH* " << VisitedEdgeToString(ve);
          }
        }
      }
    }
  }

#if 0
  // Expand the backward edges (for traveling backwards).
  // Note: It is important to pass vedge by value, because a call to
  // FindOrAddVisitedEdge() may invalidate a reference.
  void ExpandNeighboursBackward(const QueuedEdge& qedge, const Context& ctx,
                                const VisitedEdge vedge) {
    const GNode& node = g_.nodes.at(vedge.node_idx);

    for (const GEdge& edge : gnode_all_edges(g_, vedge.node_idx)) {
      // Skip edges that are forward only.
      if (!edge.inverted /*i < node.num_edges_out*/ && !edge.both_directions) {
        continue;
      }

      const WaySharedAttrs& wsa = GetWSA(g_, edge.way_idx);
      if (RoutingRejectEdge(g_, ctx.opt, node, vedge.node_idx, edge, wsa,
                            EDGE_INVERSE_DIR(edge))) {
        continue;
      }

      std::uint32_t v_idx = FindOrAddVisitedEdge(edge.other_node_idx,
                                                 ctx.opt.use_astar_heuristic);
      VisitedEdge& vother = visited_edges_.at(v_idx);
      if (vother.done) {
        continue;
      }
      std::uint32_t new_metric =
          vedge.min_metric +
          ctx.metric.Compute(wsa, ctx.opt.vt, EDGE_INVERSE_DIR(edge), edge);
      if (new_metric < vother.min_metric) {
        vother.min_metric = new_metric;
        vother.prev_v_idx = qedge.visited_node_idx;

        // Compute heuristic distance from new node to target.
        if (vother.heuristic_to_target == INFU30) {
          const uint32_t h =
              ComputeHeuristicToTarget(g_.nodes.at(edge.other_node_idx), ctx);
          CHECK_LT_S(h, INFU30);
          vother.heuristic_to_target = h;
        }
        pq_.emplace(new_metric + vother.heuristic_to_target, v_idx);
      }
    }
  }
#endif

  // Checks if a transition of restricted access is happening, such as when
  // driving into roads with access=destination. Allowed transitions are
  // "start-restricted"->"free" and "free"->"destination restricted". Returns
  // false if a transition is happening that is not allowed, and true in all
  // other cases (i.e. no transition and allowed transitions). Sets
  // next_in_target_area to true if the corresponding bit in the edge key
  // should be set. This happens when entering a restricted-access area from
  // free.
  // When entering a restricted area, 'next_in_target_area' will be set to
  // true and all following edges will have the corresponding bit in the edge
  // key set. Note that this bit can not be unset, since it is not allowed to
  // leave the target restricted area. Setting the bit avoids conflicting
  // metrics when the start and target area are the same, but the fastest path
  // leaves and re-enters it.
  inline bool CheckRestrictedAccessTransition(const Context& ctx,
                                              const PrevEdgeData& prev,
                                              const GEdge& curr_ge,
                                              bool* next_in_target_area) {
    *next_in_target_area = prev.in_target_area;
    if (curr_ge.car_label == GEdge::LABEL_FREE) {
      // New edge is free, which is ok if we are not in target area.
      return !prev.in_target_area;
    }
    // curr edge is restricted.
    if (!prev.restricted) {
      // We're entering restricted.
      CHECK_S(!prev.in_target_area);  // We can't be in restricted because
                                      // we're entering it.
      *next_in_target_area = true;
    }
    return true;
  }

  void Clear() {
    ctr_deduper_.clear();
    visited_edges_.clear();
    edgekey_to_v_idx_.clear();
    CHECK_S(pq_.empty());  // No clear() method, should be empty anyways.
    target_visited_index_ = INFU32;
  }

  const Graph& g_;
  CTRDeDuper ctr_deduper_;
  std::vector<VisitedEdge> visited_edges_;

  typedef absl::flat_hash_map<uint64_t, uint32_t> EdgeIdMap;
  EdgeIdMap edgekey_to_v_idx_;

  std::priority_queue<QueuedEdge, std::vector<QueuedEdge>, decltype(&MetricCmp)>
      pq_;
  std::uint32_t target_visited_index_;
  const int verbosity_;
};
