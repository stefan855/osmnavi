#pragma once

#include "base/util.h"
#include "graph/graph_def.h"

// Check fail if n1 and n2 are different in any of the attributes.
#define CHECK_NODES_EQUAL(n1, n2)                             \
  CHECK_EQ_S(n1.node_id, n2.node_id);                         \
  CHECK_EQ_S(n1.cluster_id, n2.cluster_id);                   \
  CHECK_EQ_S(n1.edges_start_pos, n2.edges_start_pos);         \
  CHECK_EQ_S(n1.num_forward_edges, n2.num_forward_edges);     \
  CHECK_EQ_S(n1.ncc, n2.ncc);                                 \
  CHECK_EQ_S(n1.lat, n2.lat);                                 \
  CHECK_EQ_S(n1.lon, n2.lon);                                 \
  CHECK_EQ_S(n1.large_component, n2.large_component);         \
  CHECK_EQ_S(n1.cluster_border_node, n2.cluster_border_node); \
  CHECK_EQ_S(n1.dead_end, n2.dead_end);                       \
  CHECK_EQ_S(n1.is_pedestrian_crossing, n2.is_pedestrian_crossing);

// Check fail if e1 and e1 are different in any of the attributes.
#define CHECK_EDGES_EQUAL(e1, e2)                             \
  CHECK_EQ_S(e1.target_idx, e2.target_idx);                   \
  CHECK_EQ_S(e1.way_idx, e2.way_idx);                         \
  CHECK_EQ_S(e1.distance_cm, e2.distance_cm);                 \
  CHECK_EQ_S(e1.turn_cost_idx, e2.turn_cost_idx);             \
  CHECK_EQ_S(e1.unique_target, e2.unique_target);             \
  CHECK_EQ_S(e1.to_bridge, e2.to_bridge);                     \
  CHECK_EQ_S(e1.contra_way, e2.contra_way);                   \
  CHECK_EQ_S(e1.cross_country, e2.cross_country);             \
  CHECK_EQ_S(e1.inverted, e2.inverted);                       \
  CHECK_EQ_S(e1.both_directions, e2.both_directions);         \
  CHECK_EQ_S(e1.car_label, e2.car_label);                     \
  CHECK_EQ_S(e1.car_label_strange, e2.car_label_strange);     \
  CHECK_EQ_S(e1.complex_turn_restriction_trigger,             \
             e2.complex_turn_restriction_trigger);            \
  CHECK_EQ_S(e1.stop_sign, e2.stop_sign);                     \
  CHECK_EQ_S(e1.traffic_signal, e2.traffic_signal);           \
  CHECK_EQ_S(e1.road_priority, e2.road_priority);             \
  CHECK_EQ_S(e1.bridge, e2.bridge);                           \
  CHECK_EQ_S(e1.cluster_border_edge, e2.cluster_border_edge); \
  CHECK_EQ_S(e1.dead_end, e2.dead_end);

#define CHECK_WAYS_EQUAL(w1, w2)                                    \
  CHECK_EQ_S(w1.id, w2.id);                                         \
  CHECK_EQ_S(w1.highway_label, w2.highway_label);                   \
  CHECK_EQ_S(w1.uniform_country, w2.uniform_country);               \
  CHECK_EQ_S(w1.closed_way, w2.closed_way);                         \
  CHECK_EQ_S(w1.area, w2.area);                                     \
  CHECK_EQ_S(w1.roundabout, w2.roundabout);                         \
  CHECK_EQ_S(w1.has_ref, w2.has_ref);                               \
  CHECK_EQ_S(w1.priority_road_forward, w2.priority_road_forward);   \
  CHECK_EQ_S(w1.priority_road_backward, w2.priority_road_backward); \
  CHECK_EQ_S(w1.more_than_two_lanes, w2.more_than_two_lanes);       \
  CHECK_EQ_S(w1.ncc, w2.ncc);                                       \
  CHECK_EQ_S(w1.wsa_id, w2.wsa_id);                                 \
  CHECK_EQ_S(w1.streetname_idx, w2.streetname_idx);

inline bool operator==(const GCluster::EdgeDescriptor& a,
                       const GCluster::EdgeDescriptor& b) {
  return a.g_from_idx == b.g_from_idx && a.g_edge_idx == b.g_edge_idx &&
         a.c_from_idx == b.c_from_idx && a.c_edge_idx == b.c_edge_idx &&
         a.pos == b.pos;
}

#define CHECK_CLUSTERS_EQUAL(c1, c2)                    \
  CHECK_EQ_S(c1.cluster_id, c2.cluster_id);             \
  CHECK_EQ_S(c1.num_nodes, c2.num_nodes);               \
  CHECK_EQ_S(c1.num_border_nodes, c2.num_border_nodes); \
  CHECK_EQ_S(c1.num_inner_edges, c2.num_inner_edges);   \
  CHECK_EQ_S(c1.num_outer_edges, c2.num_outer_edges);   \
  CHECK_S(c1.border_nodes == c2.border_nodes);          \
  CHECK_S(c1.border_in_edges == c2.border_in_edges);    \
  CHECK_S(c1.border_out_edges == c2.border_out_edges);  \
  CHECK_S(c1.distances == c2.distances);                \
  CHECK_S(c1.edge_distances == c2.edge_distances);

#define CHECK_TURN_COST_DATA_EQUAL(tcd1, tcd2) \
  CHECK_S(tcd1.turn_costs == tcd2.turn_costs);

#define CHECK_WAY_SHARED_ATTRS_EQUAL(wsa1, wsa2)        \
  for (size_t i = 0; i < WaySharedAttrs::RA_MAX; ++i) { \
    const RoutingAttrs& ra1 = wsa1.ra[i];               \
    const RoutingAttrs& ra2 = wsa2.ra[i];               \
    CHECK_EQ_S(ra1.dir, ra2.dir);                       \
    CHECK_EQ_S(ra1.access, ra2.access);                 \
    CHECK_EQ_S(ra1.maxspeed, ra2.maxspeed);             \
    CHECK_EQ_S(ra1.lit, ra2.lit);                       \
    CHECK_EQ_S(ra1.toll, ra2.toll);                     \
    CHECK_EQ_S(ra1.surface, ra2.surface);               \
    CHECK_EQ_S(ra1.tracktype, ra2.tracktype);           \
    CHECK_EQ_S(ra1.smoothness, ra2.smoothness);         \
    CHECK_EQ_S(ra1.left_side, ra2.left_side);           \
    CHECK_EQ_S(ra1.right_side, ra2.right_side);         \
    CHECK_EQ_S(ra1.width_dm, ra2.width_dm);             \
  }

#define CHECK_TURN_RESTRICTION_EQUAL(tr1, tr2)  \
  CHECK_EQ_S(tr1.relation_id, tr2.relation_id); \
  CHECK_EQ_S(tr1.from_way_id, tr2.from_way_id); \
  CHECK_S(tr1.via_ids == tr2.via_ids);          \
  CHECK_EQ_S(tr1.to_way_id, tr2.to_way_id);     \
  CHECK_EQ_S(tr1.via_is_node, tr2.via_is_node); \
  CHECK_EQ_S(tr1.forbidden, tr2.forbidden);     \
  CHECK_S(tr1.direction == tr2.direction);      \
  CHECK_S(tr1.path == tr2.path);

#define CHECK_COMPONENT_EQUAL(c1, c2)       \
  CHECK_EQ_S(c1.start_node, c2.start_node); \
  CHECK_EQ_S(c1.size, c2.size);
