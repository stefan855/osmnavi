#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

#include <memory>
#include <random>

#include "base/argli.h"
#include "base/country_code.h"
#include "base/util.h"
#include "test/equal_checks.h"
#include "graph/graph_def.h"
#include "graph/graph_serialize.h"

GNode RandomGNode(uint64_t seed) {
  std::mt19937 myrand(seed);
  std::uniform_int_distribution<uint64_t> dist(
      std::numeric_limits<uint64_t>::min(),
      std::numeric_limits<uint64_t>::max());

  GNode n = {0};

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

GEdge RandomGEdge(uint64_t seed) {
  std::mt19937 myrand(seed);
  std::uniform_int_distribution<uint64_t> dist(
      std::numeric_limits<uint64_t>::min(),
      std::numeric_limits<uint64_t>::max());

  GEdge e = {0};

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

GWay RandomWay(uint64_t seed) {
  std::mt19937 myrand(seed);
  std::uniform_int_distribution<uint64_t> dist(
      std::numeric_limits<uint64_t>::min(),
      std::numeric_limits<uint64_t>::max());

  GWay w = {0};

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
  w.wsa_id = (uint32_t)dist(myrand);
  w.streetname_idx = (uint32_t)dist(myrand);
  // TODO:
  // w.node_ids_buff = nullptr;

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

  cl.cluster_id = (uint32_t)dist(myrand);
  cl.num_nodes = (uint32_t)dist(myrand);
  cl.num_border_nodes = (uint32_t)dist(myrand);
  cl.num_inner_edges = (uint32_t)dist(myrand);
  cl.num_outer_edges = (uint32_t)dist(myrand);

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
    cl.edge_distances.push_back(
        RandomVec<uint32_t>(cl.border_out_edges.size()));
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

WaySharedAttrs RandomWaySharedAttrs(uint64_t seed) {
  std::mt19937 myrand(seed);
  std::uniform_int_distribution<uint64_t> dist(
      std::numeric_limits<uint64_t>::min(),
      std::numeric_limits<uint64_t>::max());

  WaySharedAttrs wsa = {0};
  for (RoutingAttrs& ra : wsa.ra) {
    ra.dir = dist(myrand) % (1llu << 1);
    ra.access = static_cast<ACCESS>(dist(myrand) % (1llu << 4));
    ra.maxspeed = dist(myrand) % (1llu << 10);
    ra.lit = dist(myrand) % (1llu << 1);
    ra.toll = dist(myrand) % (1llu << 1);
    ra.surface = static_cast<SURFACE>(dist(myrand) % (1llu << 6));
    ra.tracktype = static_cast<TRACKTYPE>(dist(myrand) % (1llu << 3));
    ra.smoothness = static_cast<SMOOTHNESS>(dist(myrand) % (1llu << 4));
    ra.left_side = dist(myrand) % (1llu << 1);
    ra.right_side = dist(myrand) % (1llu << 1);
    ra.width_dm = dist(myrand) % (1llu << 8);
  }
  return wsa;
}

TurnRestriction RandomTurnRestriction(uint64_t seed) {
  std::mt19937 myrand(seed);
  std::uniform_int_distribution<uint64_t> dist(
      std::numeric_limits<uint64_t>::min(),
      std::numeric_limits<uint64_t>::max());

  TurnRestriction tr = {0};
  tr.relation_id = dist(myrand);
  tr.from_way_id = dist(myrand);
  tr.via_ids = RandomVec<int64_t>(dist(myrand) % 5);
  tr.to_way_id = dist(myrand);
  tr.via_is_node = (dist(myrand) % 2) != 0;
  tr.forbidden = (dist(myrand) % 2) != 0;
  tr.direction = static_cast<TurnDirection>(
      dist(myrand) % static_cast<uint64_t>(TurnDirection::Max));

  uint64_t v_size = dist(myrand) % 5;
  for (size_t i = 0; i < v_size; ++i) {
    tr.path.push_back({.from_node_idx = (uint32_t)dist(myrand),
                       .way_idx = (uint32_t)dist(myrand),
                       .to_node_idx = (uint32_t)dist(myrand),
                       .edge_idx = (uint32_t)dist(myrand)});
  }

  return tr;
}

Graph::Component RandomComponent(uint64_t seed) {
  std::mt19937 myrand(seed);
  std::uniform_int_distribution<uint64_t> dist(
      std::numeric_limits<uint64_t>::min(),
      std::numeric_limits<uint64_t>::max());

  Graph::Component c = {0};
  c.start_node = dist(myrand);
  c.size = dist(myrand);
  return c;
}

void TestGNode() {
  FUNC_TIMER();

  std::vector<GNode> nodes;
  for (uint32_t i = 0; i < 100; ++i) {
    nodes.push_back(RandomGNode(i));
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
    TurnCostData dec(0, 0);
    ptr += DecodeTurnCostData(ptr, &dec);
    const TurnCostData re = RandomTurnCostData(i);
    CHECK_TURN_COST_DATA_EQUAL(re, dec);
  }
  CHECK_EQ_S(ptr, wb.base_ptr() + wb.used());
}

void TestWaySharedAttrs() {
  FUNC_TIMER();

  WriteBuff wb;
  for (uint32_t i = 0; i < 100; ++i) {
    const WaySharedAttrs cl = RandomWaySharedAttrs(i);
    EncodeWaySharedAttrs(cl, &wb);
  }

  uint8_t* ptr = wb.base_ptr();
  for (uint32_t i = 0; i < 100; ++i) {
    WaySharedAttrs dec = {0};
    ptr += DecodeWaySharedAttrs(ptr, &dec);
    const WaySharedAttrs re = RandomWaySharedAttrs(i);
    CHECK_WAY_SHARED_ATTRS_EQUAL(re, dec);
  }
  CHECK_EQ_S(ptr, wb.base_ptr() + wb.used());
}

void TestTurnRestrictions() {
  FUNC_TIMER();

  WriteBuff wb;
  for (uint32_t i = 0; i < 1; ++i) {
    const TurnRestriction cl = RandomTurnRestriction(i);
    EncodeTurnRestriction(cl, &wb);
  }

  uint8_t* ptr = wb.base_ptr();
  for (uint32_t i = 0; i < 1; ++i) {
    TurnRestriction dec = {0};
    ptr += DecodeTurnRestriction(ptr, &dec);
    const TurnRestriction re = RandomTurnRestriction(i);
    CHECK_TURN_RESTRICTION_EQUAL(re, dec);
  }
  CHECK_EQ_S(ptr, wb.base_ptr() + wb.used());
}

void TestComponents() {
  FUNC_TIMER();

  WriteBuff wb;
  for (uint32_t i = 0; i < 1; ++i) {
    const Graph::Component cl = RandomComponent(i);
    EncodeComponent(cl, &wb);
  }

  uint8_t* ptr = wb.base_ptr();
  for (uint32_t i = 0; i < 1; ++i) {
    Graph::Component dec = {0};
    ptr += DecodeComponent(ptr, &dec);
    const Graph::Component re = RandomComponent(i);
    CHECK_COMPONENT_EQUAL(re, dec);
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
  TestWaySharedAttrs();
  TestTurnRestrictions();
  TestComponents();

  // TestRead();

  LOG_S(INFO)
      << "\n\033[1;32m*****************************\nTesting successfully "
         "finished\n*****************************\033[0m";
  return 0;
}
