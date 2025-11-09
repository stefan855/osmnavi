#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

#include <memory>
#include <random>

#include "base/argli.h"
#include "base/country_code.h"
#include "base/util.h"
#include "graph/graph_def.h"
#include "graph/graph_serialize.h"

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
#define CHECK_EDGES_EQUAL(e1, e2)                         \
  CHECK_EQ_S(e1.target_idx, e2.target_idx);               \
  CHECK_EQ_S(e1.way_idx, e2.way_idx);                     \
  CHECK_EQ_S(e1.distance_cm, e2.distance_cm);             \
  CHECK_EQ_S(e1.turn_cost_idx, e2.turn_cost_idx);         \
  CHECK_EQ_S(e1.unique_target, e2.unique_target);         \
  CHECK_EQ_S(e1.to_bridge, e2.to_bridge);                 \
  CHECK_EQ_S(e1.contra_way, e2.contra_way);               \
  CHECK_EQ_S(e1.cross_country, e2.cross_country);         \
  CHECK_EQ_S(e1.inverted, e2.inverted);                   \
  CHECK_EQ_S(e1.both_directions, e2.both_directions);     \
  CHECK_EQ_S(e1.car_label, e2.car_label);                 \
  CHECK_EQ_S(e1.car_label_strange, e2.car_label_strange); \
  CHECK_EQ_S(e1.complex_turn_restriction_trigger,         \
             e2.complex_turn_restriction_trigger);        \
  CHECK_EQ_S(e1.stop_sign, e2.stop_sign);                 \
  CHECK_EQ_S(e1.traffic_signal, e2.traffic_signal);       \
  CHECK_EQ_S(e1.road_priority, e2.road_priority);         \
  CHECK_EQ_S(e1.type, e2.type);

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
  // TODO: streetname, node_ids

inline bool operator==(const GCluster::EdgeDescriptor& a,
                       const GCluster::EdgeDescriptor& b) {
  return memcmp(&a, &b, sizeof(GCluster::EdgeDescriptor)) == 0;
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

GNode RandomGNode(uint64_t seed, uint8_t fill = 0) {
  std::mt19937 myrand(seed);
  std::uniform_int_distribution<uint64_t> dist(
      std::numeric_limits<uint64_t>::min(),
      std::numeric_limits<uint64_t>::max());

  GNode n;
  memset((void*)&n, fill, sizeof(GNode));

  // TODO: handle sign?
  n.node_id = dist(myrand) % (1llu << GNODE_ID_BITS);
  n.cluster_id = dist(myrand) % (1llu << NUM_CLUSTER_BITS);
  n.edges_start_pos = dist(myrand) % (1llu << GNODE_EDGE_START_BITS);
  n.num_forward_edges = dist(myrand) % (1llu << NUM_EDGES_OUT_BITS);
  n.ncc = dist(myrand) % (1llu << NUM_CC_BITS);
  n.lat = (int32_t)(dist(myrand) % (1llu << 32));
  n.lon = (int32_t)(dist(myrand) % (1llu << 32));
  n.large_component = dist(myrand) % 2;
  n.cluster_border_node = dist(myrand) % 2;
  n.dead_end = dist(myrand) % 2;
  n.is_pedestrian_crossing = dist(myrand) % 2;

  return n;
}

GEdge RandomGEdge(uint64_t seed, uint8_t fill = 0) {
  std::mt19937 myrand(seed);
  std::uniform_int_distribution<uint64_t> dist(
      std::numeric_limits<uint64_t>::min(),
      std::numeric_limits<uint64_t>::max());

  GEdge e;
  memset((void*)&e, fill, sizeof(GEdge));

  e.target_idx = (uint32_t)(dist(myrand) % (1llu << 32));
  e.way_idx = (uint32_t)(dist(myrand) % (1llu << 32));
  e.distance_cm = (uint32_t)(dist(myrand) % (1llu << 32));
  e.turn_cost_idx = dist(myrand) % (1llu << MAX_TURN_COST_IDX_BITS);
  e.unique_target = dist(myrand) % 2;
  e.to_bridge = dist(myrand) % 2;
  e.contra_way = dist(myrand) % 2;
  e.cross_country = dist(myrand) % 2;
  e.inverted = dist(myrand) % 2;
  e.both_directions = dist(myrand) % 2;
  e.car_label = static_cast<GEdge::RESTRICTION>(
      dist(myrand) % (1llu << NUM_GEDGE_RESTRICTION_BITS));
  e.car_label_strange = dist(myrand) % 2;
  e.complex_turn_restriction_trigger = dist(myrand) % 2;
  e.stop_sign = dist(myrand) % 2;
  e.traffic_signal = dist(myrand) % 2;
  e.road_priority = static_cast<GEdge::ROAD_PRIORITY>(
      dist(myrand) % (1llu << NUM_GEDGE_ROAD_PRIORITY_BITS));
  e.type =
      static_cast<GEdge::TYPE>(dist(myrand) % (1llu << NUM_GEDGE_TYPE_BITS));
  return e;
}

GWay RandomWay(uint64_t seed, uint8_t fill = 0) {
  std::mt19937 myrand(seed);
  std::uniform_int_distribution<uint64_t> dist(
      std::numeric_limits<uint64_t>::min(),
      std::numeric_limits<uint64_t>::max());

  GWay w;
  memset((void*)&w, fill, sizeof(GWay));

  // handle sign?
  w.id = dist(myrand) % (1llu << GWAY_ID_BITS);
  w.highway_label = static_cast<HIGHWAY_LABEL>(
      dist(myrand) % (1llu << NUM_HIGHWAY_LABEL_BITS));
  w.uniform_country = dist(myrand) % 2;
  w.closed_way = dist(myrand) % 2;
  w.area = dist(myrand) % 2;
  w.roundabout = dist(myrand) % 2;
  w.has_ref = dist(myrand) % 2;
  w.priority_road_forward = dist(myrand) % 2;
  w.priority_road_backward = dist(myrand) % 2;
  w.more_than_two_lanes = dist(myrand) % 2;
  w.ncc = dist(myrand) % (1llu << NUM_CC_BITS);
  w.wsa_id = dist(myrand) % (1llu << 32);
  // TODO:
  w.streetname = nullptr;
  w.node_ids = nullptr;

  return w;
}

template <typename T>
std::vector<T> RandomVec(uint32_t num) {
  std::vector<T> v;
  while (num > 0) {
    // Don't need random numbers here, these are good enough for testing.
    v.push_back(3 * num);
    num--;
  }
  return v;
}

GCluster::EdgeDescriptor RandomEdgeDescriptor(uint64_t seed) {
  GCluster::EdgeDescriptor ed;
  ed.g_from_idx = seed % 12345678;
  ed.g_edge_idx = seed % 23456789;
  ed.c_from_idx = seed % 34567890;
  ed.c_edge_idx = seed % 45678901;
  ed.pos = seed % 56789012;
  return ed;
}

GCluster RandomGCluster(uint64_t seed) {
  std::mt19937 myrand(seed);
  std::uniform_int_distribution<uint64_t> dist(
      std::numeric_limits<uint64_t>::min(),
      std::numeric_limits<uint64_t>::max());

  GCluster cl = {0};

  cl.cluster_id = dist(myrand) % 32;
  cl.num_nodes = dist(myrand) % 32;
  cl.num_border_nodes = dist(myrand) % 32;
  cl.num_inner_edges = dist(myrand) % 32;
  cl.num_outer_edges = dist(myrand) % 32;

  cl.border_nodes = RandomVec<uint32_t>(dist(myrand) % 17);

  auto start = dist(myrand) % 17;
  for (size_t i = start; i > 0; --i) {
    cl.border_in_edges.push_back(RandomEdgeDescriptor(dist(myrand)));
  }

  for (size_t i = dist(myrand) % 17; i > 0; --i) {
    cl.border_out_edges.push_back(RandomEdgeDescriptor(dist(myrand)));
  }

  for (size_t i = 0; i < cl.border_nodes.size(); ++i) {
    cl.distances.push_back(RandomVec<uint32_t>(cl.border_nodes.size()));
  }

  for (size_t i = 0; i < cl.border_in_edges.size(); ++i) {
    cl.edge_distances.push_back(RandomVec<uint32_t>(cl.border_out_edges.size()));
  }

  return cl;
}

TurnCostData RandomTurnCostData(uint64_t seed) {
  std::mt19937 myrand(seed);
  std::uniform_int_distribution<uint64_t> dist(
      std::numeric_limits<uint64_t>::min(),
      std::numeric_limits<uint64_t>::max());

  TurnCostData tcd(0, 0);
  tcd.turn_costs = RandomVec<uint8_t>(dist(myrand) % 17);
  return tcd;
}

#if 0
inline bool operator==(const GNode& n1, const GNode& n2) {
  return n1.node_id == n2.node_id && n1.cluster_id == n2.cluster_id &&
         n1.edges_start_pos == n2.edges_start_pos &&
         n1.num_forward_edges == n2.num_forward_edges && n1.ncc == n2.ncc &&
         n1.lat == n2.lat && n1.lon == n2.lon &&
         n1.large_component == n2.large_component &&
         n1.cluster_border_node == n2.cluster_border_node &&
         n1.dead_end == n2.dead_end &&
         n1.is_pedestrian_crossing == n2.is_pedestrian_crossing;
}
#endif

void TestGNode() {
  FUNC_TIMER();

  std::vector<GNode> nodes;
  for (uint32_t i = 0; i < 100; ++i) {
    nodes.push_back(RandomGNode(i));

    // Compare the just created node with a node that has a different pre-fill
    // character. This will find errors such as when we forget to set an
    // attribute here.
    GNode rn2 = RandomGNode(i, /*fill=*/255);
    CHECK_NODES_EQUAL(nodes.at(i), rn2);
  }

  WriteBuff wb;
  GNode prev = {0};
  for (uint32_t i = 0; i < 100; ++i) {
    EncodeGNode(prev, nodes.at(i), &wb);
    prev = nodes.at(i);
  }

  prev = {0};
  uint8_t* ptr = wb.base_ptr();
  for (uint32_t i = 0; i < 100; ++i) {
    GNode dec = {0};
    ptr += DecodeGNode(prev, ptr, &dec);
    CHECK_NODES_EQUAL(nodes.at(i), dec);
    prev = dec;
  }
  CHECK_EQ_S(ptr, wb.base_ptr() + wb.used());
}

void TestGEdge() {
  FUNC_TIMER();

  WriteBuff wb;
  for (uint32_t i = 0; i < 100; ++i) {
    GEdge rn1 = RandomGEdge(i);
    EncodeGEdge(rn1, &wb);

    GEdge rn2 = RandomGEdge(i, /*fill=*/255);
    CHECK_EDGES_EQUAL(rn1, rn2);
  }

  uint8_t* ptr = wb.base_ptr();
  for (uint32_t i = 0; i < 100; ++i) {
    GEdge dec = {0};
    ptr += DecodeGEdge(ptr, &dec);
    const GEdge re = RandomGEdge(i);
    CHECK_EDGES_EQUAL(re, dec);
  }
  CHECK_EQ_S(ptr, wb.base_ptr() + wb.used());
}

void TestGWay() {
  FUNC_TIMER();

  std::vector<GWay> ways;
  for (uint32_t i = 0; i < 100; ++i) {
    ways.push_back(RandomWay(i));

    // Compare the just created node with a node that has a different pre-fill
    // character. This will find errors such as when we forget to set an
    // attribute here.
    GWay rw2 = RandomWay(i, /*fill=*/255);
    CHECK_WAYS_EQUAL(ways.at(i), rw2);
  }

  WriteBuff wb;
  GWay prev = {0};
  for (uint32_t i = 0; i < 100; ++i) {
    EncodeGWay(prev, ways.at(i), &wb);
    prev = ways.at(i);
  }

  prev = {0};
  uint8_t* ptr = wb.base_ptr();
  for (uint32_t i = 0; i < 100; ++i) {
    GWay dec = {0};
    ptr += DecodeGWay(prev, ptr, &dec);
    CHECK_WAYS_EQUAL(ways.at(i), dec);
    prev = dec;
  }
  CHECK_EQ_S(ptr, wb.base_ptr() + wb.used());
}

void TestGCluster() {
  FUNC_TIMER();

  WriteBuff wb;
  for (uint32_t i = 0; i < 100; ++i) {
    const GCluster cl = RandomGCluster(i);
    EncodeGCluster(cl, &wb);
  }

  uint8_t* ptr = wb.base_ptr();
  for (uint32_t i = 0; i < 100; ++i) {
    GCluster dec = {0};
    ptr += DecodeGCluster(ptr, &dec);
    const GCluster re = RandomGCluster(i);
    CHECK_CLUSTERS_EQUAL(re, dec);
  }
  CHECK_EQ_S(ptr, wb.base_ptr() + wb.used());
}

void TestTurnCostData() {
  FUNC_TIMER();

  WriteBuff wb;
  for (uint32_t i = 0; i < 100; ++i) {
    const TurnCostData cl = RandomTurnCostData(i);
    EncodeTurnCostData(cl, &wb);
  }

  uint8_t* ptr = wb.base_ptr();
  for (uint32_t i = 0; i < 100; ++i) {
    TurnCostData dec(0,0);
    ptr += DecodeTurnCostData(ptr, &dec);
    const TurnCostData re = RandomTurnCostData(i);
    CHECK_TURN_COST_DATA_EQUAL(re, dec);
  }
  CHECK_EQ_S(ptr, wb.base_ptr() + wb.used());
}

void TestRead() {
  FUNC_TIMER();
  Graph g =
      ReadSerializedGraph("/home/stefan/src/osm/osmnavi/release/graph.ser");
}

int main(int argc, char* argv[]) {
  InitLogging(argc, argv);
  if (argc != 1) {
    ABORT_S() << absl::StrFormat("usage: %s", argv[0]);
  }

  TestGNode();
  TestGEdge();
  TestGWay();
  TestGCluster();
  TestTurnCostData();

  // TestRead();

  LOG_S(INFO)
      << "\n\033[1;32m*****************************\nTesting successfully "
         "finished\n*****************************\033[0m";
  return 0;
}
