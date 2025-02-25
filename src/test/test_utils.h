#include <osmpbf/osmpbf.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

#include <memory>

#include "base/argli.h"
#include "base/util.h"
#include "graph/build_graph.h"
#include "graph/graph_def.h"
#include "osm/access.h"
#include "osm/osm_helpers.h"

class OsmWrapper {
 public:
  OsmWrapper() : tagh(t) {}

  OSMPBF::StringTable t;
  OSMTagHelper tagh;

  template <typename T>
  void AddKeyVal(std::string_view key, std::string_view val, T* obj) {
    obj->mutable_keys()->Add(AddStringToTable(key));
    obj->mutable_vals()->Add(AddStringToTable(val));
  }

  // For instance AddMember("from", "way", 12345, &rel);
  void AddMember(std::string_view role, std::string_view type, int64_t id,
                 OSMPBF::Relation* rel) {
    rel->mutable_roles_sid()->Add(AddStringToTable(role));
    OSMPBF::Relation::MemberType member_type;
    if (type == "node") {
      member_type = OSMPBF::Relation::NODE;
    } else if (type == "way") {
      member_type = OSMPBF::Relation::WAY;
    } else {
      ABORT_S() << "Unsupported member type " << type;
    }
    rel->mutable_types()->Add(member_type);
    const int64_t last_id = GetLastDeltaEncoded(rel->memids());
    rel->mutable_memids()->Add(id - last_id);
  }

 private:
  // Find or add a string in string table 't' and return its position.
  int AddStringToTable(std::string_view str) {
    t.mutable_s()->Add(std::string(str));
    return t.s().size() - 1;
  }

  int64_t GetLastDeltaEncoded(
      const google::protobuf::RepeatedField<int64_t>& sequence) {
    int64_t running_id = 0;
    for (int64_t val : sequence) {
      running_id += val;
    }
    return running_id;
  }
};

void AddCluster(Graph& g, uint32_t cluster_id, GCluster c) {
  CHECK_EQ_S(cluster_id, g.clusters.size());
  g.clusters.push_back(c);
}

void AddNode(Graph& g, uint32_t idx, uint32_t cluster_id = 0) {
  CHECK_EQ_S(idx, g.nodes.size());
  g.nodes.push_back({.node_id = 100 + idx,
                     .large_component = 1,
                     .cluster_id = cluster_id,
                     .cluster_border_node = 0,
                     .edges_start_pos = 0,
                     .dead_end = 0,
                     .ncc = NCC_CH,
                     .lat = 100 + (int32_t)idx,
                     .lon = 100 + (int32_t)idx});
}

using TEdge = std::tuple<uint32_t, uint32_t, uint32_t, GEdge::RESTRICTION>;
void AddEdge(uint32_t from, uint32_t to, uint32_t w, GEdge::RESTRICTION label,
             bool both_dirs, std::vector<TEdge>* edges) {
  edges->push_back({from, to, w, label});
  if (both_dirs) {
    edges->push_back({to, from, w, label});
  }
}

void StoreEdges(std::vector<TEdge> edges, Graph* g) {
  std::sort(edges.begin(), edges.end());
  for (const TEdge& e : edges) {
    const uint32_t from_idx = std::get<0>(e);
    const uint32_t to_idx = std::get<1>(e);
    g->nodes.at(from_idx).edges_start_pos += 1;  // Hack: use as counter.
    g->edges.push_back({.other_node_idx = to_idx,
                        .way_idx = 0,
                        .distance_cm = std::get<2>(e),
                        .unique_other = 1,
                        .bridge = 0,
                        .to_bridge = 0,
                        .contra_way = 0,
                        .cross_country = 0,
                        .inverted = 0,
                        .both_directions = 0,
                        .car_label = std::get<3>(e),
                        .car_label_strange = 0});
    // If this edge crosses clusters, then mark the nodes accordingly.
    if (g->nodes.at(from_idx).cluster_id != g->nodes.at(to_idx).cluster_id) {
      g->nodes.at(from_idx).cluster_border_node = 1;
      g->nodes.at(to_idx).cluster_border_node = 1;
    }
  }
  uint32_t curr_pos = 0;
  for (GNode& n : g->nodes) {
    uint32_t count = n.edges_start_pos;
    n.edges_start_pos = curr_pos;
    curr_pos += count;
  }
}
