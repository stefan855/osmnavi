#include <osmpbf/osmpbf.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

#include <memory>

#include "base/argli.h"
#include "base/util.h"
#include "base/varbyte.h"
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

inline void AddCluster(Graph& g, uint32_t cluster_id, GCluster c) {
  CHECK_EQ_S(cluster_id, g.clusters.size());
  g.clusters.push_back(c);
}

inline void AddDefaultWSA(Graph& g,
                          RoutingAttrs dflt = {
                              .dir = 1, .access = ACC_YES, .maxspeed = 50}) {
  g.way_shared_attrs.push_back(WaySharedAttrs::Create(dflt));
}

inline void AddWay(Graph& g, uint32_t way_idx,
                   HIGHWAY_LABEL highway_label = HW_TERTIARY,
                   uint32_t wsa_id = 0,
                   std::vector<uint32_t> node_indexes = {}) {
    CHECK_EQ_S(way_idx, g.ways.size());
    CHECK_LT_S(wsa_id, g.way_shared_attrs.size());
    uint8_t* node_ids_buff = nullptr;
    if (!node_indexes.empty()) {
      std::vector<uint64_t> node_ids;
      for (uint32_t idx : node_indexes) {
        node_ids.push_back(g.nodes.at(idx).node_id);
      }
      WriteBuff buff;
      EncodeUInt(node_ids.size(), &buff);
      EncodeNodeIds(node_ids, &buff);
      node_ids_buff = g.unaligned_pool_.AllocBytes(buff.used());
      memcpy(node_ids_buff, buff.base_ptr(), buff.used());
    }

    g.ways.push_back({.id = way_idx * 1000,
                      .highway_label = highway_label,
                      .uniform_country = 1,
                      .ncc = NCC_CH,
                      .wsa_id = wsa_id,
                      .node_ids = node_ids_buff});
}

inline void AddNode(Graph& g, uint32_t idx, uint32_t cluster_id = 0) {
    CHECK_EQ_S(idx, g.nodes.size());
    g.nodes.push_back({.node_id = 'A' + idx,
                       .large_component = 1,
                       .cluster_id = cluster_id,
                       .cluster_border_node = 0,
                       .edges_start_pos = 0,
                       .num_edges_forward = 0,
                       .dead_end = 0,
                       .ncc = NCC_CH,
                       .lat = 100 + (int32_t)idx,
                       .lon = 100 + (int32_t)idx});
}

// Contains (from, to, dist, restriction-label, way_idx, contra_way).
using TEdge = std::tuple<uint32_t, uint32_t, uint32_t, GEdge::RESTRICTION,
                         uint32_t, bool>;

inline void AddEdge(uint32_t from, uint32_t to, uint32_t dist,
                    GEdge::RESTRICTION label, uint32_t way_idx, bool both_dirs,
                    std::vector<TEdge>* edges) {
    edges->push_back({from, to, dist, label, way_idx, false});
    if (both_dirs) {
      edges->push_back({to, from, dist, label, way_idx, true});
    }
}

// Same as above, but omitting way_idx.
inline void AddEdge(uint32_t from, uint32_t to, uint32_t dist,
                    GEdge::RESTRICTION label, bool both_dirs,
                    std::vector<TEdge>* edges) {
    AddEdge(from, to, dist, label, /*way_idx=0*/ 0, both_dirs, edges);
}

inline void StoreEdges(std::vector<TEdge> edges, Graph* g) {
    std::sort(edges.begin(), edges.end());
    for (const TEdge& e : edges) {
      const uint32_t from_idx = std::get<0>(e);
      const uint32_t to_idx = std::get<1>(e);
      g->nodes.at(from_idx).edges_start_pos += 1;    // Hack: use as counter.
      g->nodes.at(from_idx).num_edges_forward += 1;  // Hack: use as counter.
      g->edges.push_back({.other_node_idx = to_idx,
                          .way_idx = std::get<4>(e),
                          .distance_cm = std::get<2>(e),
                          .unique_other = 1,
                          .bridge = 0,
                          .to_bridge = 0,
                          .contra_way = std::get<5>(e),
                          .cross_country = 0,
                          .inverted = 0,
                          .both_directions = 0,
                          .car_label = std::get<3>(e),
                          .car_label_strange = 0,
                          .car_uturn_allowed = 0});
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

inline OSMPBF::Relation CreateTRRelation(const Graph& g, OsmWrapper* w,
                                         uint64_t id, uint32_t from_way_idx,
                                         uint32_t via_node_idx,
                                         uint32_t to_way_idx,
                                         std::string_view restr) {
    OSMPBF::Relation rel;
    rel.set_id(id);
    w->AddKeyVal("type", "restriction", &rel);
    w->AddKeyVal("restriction", restr, &rel);
    w->AddMember("from", "way", g.ways.at(from_way_idx).id, &rel);
    w->AddMember("via", "node", g.nodes.at(via_node_idx).node_id, &rel);
    w->AddMember("to", "way", g.ways.at(to_way_idx).id, &rel);
    return rel;
}

/*
 * Find the shortest way from [b] to [e].
 *
 * Given the forbidden turn w0-[d]-w2, it is not possible to go [b][d][e].
 * The (wrong) shortest path found by standard Dijkstra is [b][d][f][e].
 *
 * Edge based Dijkstra finds the correct shortest path [b][a][c][d][e].
 *
 *
 *                      [f]
 *                     /   \
 *                    /     \
 *               5w3 /       \ 5w3
 *                  /         \
 *    [c] ------- [d] ------- [e]
 *     |    1w1    |#   1w2
 *     |           |
 * 1w1 |           | 1w0
 *     |           |
 *     |    1w1    |
 *    [a] ------- [b]
 *
 *       Notation: '5w3': Labels an edge of length 5 and on way 3.
 *                 '#':   Turn restriction from way0 (w0) to way2 (w2).
 */
inline Graph CreateStandardTurnRestrictionGraph(bool both_dirs) {
    enum : uint32_t { A = 0, B, C, D, E, F };  // Node indexes.
    enum : uint32_t { Way0 = 0, Way1, Way2, Way3 };
    Graph g;
    AddDefaultWSA(g);

    AddNode(g, A);
    AddNode(g, B);
    AddNode(g, C);
    AddNode(g, D);
    AddNode(g, E);
    AddNode(g, F);

    AddWay(g, /*way_idx=*/Way0, HW_TERTIARY, /*wsa_id=*/0, {B, D});
    AddWay(g, /*way_idx=*/Way1, HW_TERTIARY, /*wsa_id=*/0, {A, B, C, D});
    AddWay(g, /*way_idx=*/Way2, HW_TERTIARY, /*wsa_id=*/0, {D, E});
    AddWay(g, /*way_idx=*/Way3, HW_TERTIARY, /*wsa_id=*/0, {D, F, E});

    std::vector<TEdge> edges;
    AddEdge(B, A, 1000, GEdge::LABEL_FREE, Way1, both_dirs, &edges);
    AddEdge(B, D, 1000, GEdge::LABEL_FREE, Way0, both_dirs, &edges);
    AddEdge(A, C, 1000, GEdge::LABEL_FREE, Way1, both_dirs, &edges);
    AddEdge(C, D, 1000, GEdge::LABEL_FREE, Way1, both_dirs, &edges);
    AddEdge(D, E, 1000, GEdge::LABEL_FREE, Way2, both_dirs, &edges);
    AddEdge(D, F, 5000, GEdge::LABEL_FREE, Way3, both_dirs, &edges);
    AddEdge(F, E, 5000, GEdge::LABEL_FREE, Way3, both_dirs, &edges);
    StoreEdges(edges, &g);

    return g;
}

// Helper function to iterate over all allowed edges in a condensed turn
// restriction.
inline std::vector<uint64_t> CondensedEdgeIndexes(
    const Graph& g, const CondensedTurnRestrictionKey& key,
    const CondensedTurnRestrictionData& data) {
    const GNode& via = g.nodes.at(key.via_node_idx);
    std::vector<uint64_t> v;
    uint32_t bits = data.allowed_edge_bits;
    while (bits != 0) {
      uint32_t offset = __builtin_ctz(bits);
      bits = bits - (1u << offset);
      v.push_back(via.edges_start_pos + offset);
    }
    return v;
}

// Helper function to iterate over all allowed edges in a condensed turn
// restriction.
inline std::vector<uint64_t> CondensedToNodeIds(
    const Graph& g, const CondensedTurnRestrictionKey& key,
    const CondensedTurnRestrictionData& data) {
    std::vector<uint64_t> v;
    for (auto e_idx : CondensedEdgeIndexes(g, key, data)) {
      const GEdge& e = g.edges.at(e_idx);
      v.push_back(GetGNodeId(g, e.other_node_idx));
    }
    return v;
}

inline std::string CondensedTurnRestrictionDebugString(
    const Graph& g, const CondensedTurnRestrictionKey& key,
    const CondensedTurnRestrictionData& data) {
    std::string tmp;
    for (auto id : CondensedToNodeIds(g, key, data)) {
      tmp += absl::StrFormat(",%c", id);
    }
    return absl::StrFormat(
        "from=%c via=%c to=[%s] rel=%lld", GetGNodeId(g, key.from_node_idx),
        GetGNodeId(g, key.via_node_idx), tmp.substr(1), data.osm_relation_id);
}
