#pragma once

#include "graph/graph_def.h"
#include "graph/routing_attrs.h"

// An edge that is specified using the index of the start node and the offset
// of the edge within the list of edges of this node.
// Storing an edge this way uses more space and is a little bit slower for most
// operations. But it allows access to the start node of the edge much more
// easily. Note that accessing the start node is almost impossible when only the
// edge index is stored.
class FullEdge final {
 public:
  FullEdge() : valid_(0) {}
  FullEdge(const FullEdge& e)
      : start_idx_(e.start_idx_), offset_(e.offset_), valid_(e.valid_) {}
  FullEdge(uint32_t start_idx, uint32_t offset)
      : start_idx_(start_idx), offset_(offset), valid_(1) {}

  inline const uint32_t start_idx() const { return start_idx_; }
  inline const uint32_t offset() const { return offset_; }
  inline const GNode& start_node(const Graph& g) const {
    return g.nodes.at(start_idx_);
  }
  inline const uint32_t target_idx(const Graph& g) const {
    return gedge(g).target_idx;
  }
  inline const GNode& target_node(const Graph& g) const {
    return g.nodes.at(target_idx(g));
  }
  inline const GEdge& gedge(const Graph& g) const {
    return g.edges.at(start_node(g).edges_start_pos + offset_);
  }
  // Second version that returns non-const GEdge reference.
  inline GEdge& gedge(Graph& g) const {
    return g.edges.at(start_node(g).edges_start_pos + offset_);
  }
  inline uint32_t gedge_idx(const Graph& g) const {
    return start_node(g).edges_start_pos + offset_;
  }
  bool operator==(const FullEdge& other) const {
    return start_idx_ == other.start_idx_ && offset_ == other.offset_;
    ;
  }
  bool valid() const { return valid_; }

 private:
  uint32_t start_idx_;
  uint32_t offset_ : NUM_EDGES_OUT_BITS;
  uint32_t valid_ : 1;
};

inline FullEdge gnode_find_full_edge(const Graph& g, uint32_t from_node_idx,
                                     uint32_t to_node_idx, uint32_t way_idx) {
  FullEdge fe;
  uint32_t e_start = gnode_edges_start(g, from_node_idx);
  uint32_t num = gnode_edges_stop(g, from_node_idx) - e_start;
  for (uint32_t off = 0; off < num; ++off) {
    if (g.edges.at(e_start + off).target_idx == to_node_idx &&
        g.edges.at(e_start + off).way_idx == way_idx) {
      return FullEdge(from_node_idx, off);
    }
  }
  ABORT_S() << absl::StrFormat(
      "Node %lld has no forward edge to node %lld with way %lld",
      GetGNodeIdSafe(g, from_node_idx), GetGNodeIdSafe(g, to_node_idx),
      g.ways.at(way_idx).id);
}

// Find all unique (deduped by 'e.target_idx') forward edges starting at
// 'node_idx'. e.target_idx==ignore_node_idx is ignored.
inline std::vector<FullEdge> gnode_unique_forward_edges(
    const Graph& g, uint32_t node_idx, bool ignore_loops,
    uint32_t ignore_node_idx = INFU32) {
  std::vector<FullEdge> res;

  const GNode& node = g.nodes.at(node_idx);
  for (uint32_t offset = 0; offset < node.num_forward_edges; ++offset) {
    const GEdge& e = g.edges.at(node.edges_start_pos + offset);
    if (!e.unique_target || e.target_idx == ignore_node_idx ||
        (ignore_loops && e.target_idx == node_idx)) {
      continue;
    }
    uint32_t found_pos;
    for (found_pos = 0; found_pos < res.size(); ++found_pos) {
      if (res.at(found_pos).target_idx(g) == e.target_idx) {
        break;
      }
    }
    if (found_pos >= res.size()) {
      res.emplace_back(node_idx, offset);
    }
  }
  return res;
}

// Note, this returns a new vector, not a span.
inline std::vector<FullEdge> gnode_incoming_edges(const Graph& g,
                                                  uint32_t node_idx) {
  std::vector<FullEdge> res;
  for (const GEdge& out : gnode_all_edges(g, node_idx)) {
    if (!out.unique_target || out.target_idx == node_idx) {
      continue;
    }

    const GNode& other = g.nodes.at(out.target_idx);
    for (uint32_t offset = 0; offset < other.num_forward_edges; ++offset) {
      const GEdge& incoming = g.edges.at(other.edges_start_pos + offset);
      if (incoming.target_idx == node_idx) {
        res.emplace_back(out.target_idx, offset);
      }
    }
  }
  return res;
}

// Statistics of in/outgoing edges at a node for the best (==lowest) highway
// label value.
struct BestHighwayAtNode {
  HIGHWAY_LABEL highway_label : 5 = HW_MAX;
  uint16_t num_incoming = 0;
  uint16_t num_outgoing = 0;
};

inline BestHighwayAtNode GetBestHighwayAtNode(const Graph& g, VEHICLE vt,
                                              uint32_t node_idx) {
  BestHighwayAtNode best;
  for (const GEdge& e : gnode_all_edges(g, node_idx)) {
    const GWay& w = g.ways.at(e.way_idx);
    if (w.highway_label <= best.highway_label) {
      if (w.highway_label < best.highway_label) {
        // Found new best highway.
        best.highway_label = w.highway_label;
        best.num_incoming = 0;
        best.num_outgoing = 0;
      }
      if (e.inverted) {
        best.num_incoming++;
      } else {
        best.num_outgoing++;
        if (e.both_directions) {
          best.num_incoming++;
        }
      }
    }
  }
  return best;
}

// Starting at 'start', find the first connected edge leading to a crossing
// within 'max_distance_cm'. If nothing is found, an invalid edge is
// returned.
inline FullEdge FollowEdgeToCrossing(const Graph& g, VEHICLE vt,
                                     const FullEdge start,
                                     uint32_t max_distance_cm = 20 * 100) {
  uint32_t dist_cm = 0;
  FullEdge curr = start;
  while (dist_cm <= max_distance_cm) {
    uint32_t target_idx = curr.target_idx(g);
    const GNode& target = g.nodes.at(target_idx);

#if 0
    // TODO: Should use this for some street types (depending on type of
    // start.way?).
    if (target.is_pedestrian_crossing) {
      // Marked as crossing with node tags. For instance a street crossing a
      // footway, one of which might be missing.
      return curr;
    }
#endif

    const uint32_t num_unique =
        gnode_num_unique_edges(g, target_idx, /*ignore_loops=*/true);
    // 'num_unique' counts the nodes connected to 'target_idx' by edges with
    // arbitrary direction.
    if (num_unique < 2) {
      // Should be one, because otherwise we wouldn't have gotten here.
      CHECK_EQ_S(num_unique, 1) << target.node_id;
      // Seems to be the end of a street.
      break;
    }

    if (num_unique >= 3) {
      // There are three different nodes connected somehow. Let's assume this is
      // a crossing, although it could be something different, for instance a
      // road fork in only one direction.
      return curr;
    }

    // Edges leading to other nodes except to the node we're coming from.
    std::vector<FullEdge> unique_forward =
        gnode_unique_forward_edges(g, target_idx, /*ignore_loops=*/true,
                                   /*ignore_node_idx=*/curr.start_idx());

    if (unique_forward.size() == 1 && num_unique == 2) {
      // We can continue on the way, it has no branches.
      curr = unique_forward.front();
      dist_cm += curr.gedge(g).distance_cm;
      continue;
    }

    // No crossing, but we can't continue.
    LOG_S(INFO) << absl::StrFormat(
        "No crossing but can't continue. edge %lld -> %lld",
        curr.start_node(g).node_id, target.node_id);
    break;
  }

  // Return invalid edge.
  return FullEdge();
}

// Describe a path connecting 3 nodes, using two edges
// The first edge starts at 'node0_idx' with edge offset 'edge0_off'.
// The second edge starts at 'node1_idx' with edge offset 'edge1_off'.
struct N3Path final {
  uint32_t node0_idx = INFU32;
  uint32_t node1_idx = INFU32;
  uint32_t node2_idx = INFU32;
  uint32_t edge0_off : NUM_EDGES_OUT_BITS = 0;
  uint32_t edge1_off : NUM_EDGES_OUT_BITS = 0;
  bool valid = false;

  const GNode& node0(const Graph& g) const { return g.nodes.at(node0_idx); }
  const GNode& node1(const Graph& g) const { return g.nodes.at(node1_idx); }
  const GNode& node2(const Graph& g) const { return g.nodes.at(node2_idx); }
  const FullEdge full_edge0() const {
    // return {.start_idx = node0_idx, .offset = edge0_off};
    return FullEdge(node0_idx, edge0_off);
  }
  const FullEdge full_edge1() const {
    // return {.start_idx = node1_idx, .offset = edge1_off};
    return FullEdge(node1_idx, edge1_off);
  }
  const GEdge& edge0(const Graph& g) const {
    return g.edges.at(node0(g).edges_start_pos + edge0_off);
  }
  const GEdge& edge1(const Graph& g) const {
    return g.edges.at(node1(g).edges_start_pos + edge1_off);
  }
  const uint32_t way0_idx(const Graph& g) const { return edge0(g).way_idx; }
  const uint32_t way1_idx(const Graph& g) const { return edge1(g).way_idx; }

  // The compressed turn cost between the first and the second edge.
  uint32_t get_compressed_turn_cost_0to1(const Graph& g) const {
    return g.turn_costs.at(edge0(g).turn_cost_idx).turn_costs.at(edge1_off);
  }

  static N3Path Create(const Graph& g, const FullEdge& fe0,
                       const FullEdge& fe1) {
    CHECK_EQ_S(fe0.target_idx(g), fe1.start_idx());
    return {.node0_idx = fe0.start_idx(),
            .node1_idx = fe1.start_idx(),
            .node2_idx = fe1.target_idx(g),
            .edge0_off = fe0.offset(),
            .edge1_off = fe1.offset(),
            .valid = true};
  }

  std::string DebugStr(const Graph& g) const {
    return absl::StrFormat(
        "%lld(%s) -> %lld(%s) -> %lld(%s)", GetGNodeIdSafe(g, node0_idx),
        node0(g).dead_end ? "d-e" : "-", GetGNodeIdSafe(g, node1_idx),
        node1(g).dead_end ? "d-e" : "-", GetGNodeIdSafe(g, node2_idx),
        node2(g).dead_end ? "d-e" : "-");
  }
};

// Find a path of length 2 (nodes and edges), given the three nodes on the
// path.
// If a path was found, return the path with 'valid' set to true.
// If the path can't be found, return 'valid' set to false.
inline N3Path FindN3Path(const Graph& g, uint32_t node0_idx, uint32_t node1_idx,
                         uint32_t node2_idx) {
  const GNode& n0 = g.nodes.at(node0_idx);
  for (uint32_t off0 = 0; off0 < n0.num_forward_edges; ++off0) {
    if (g.edges.at(n0.edges_start_pos + off0).target_idx == node1_idx) {
      const GNode& n1 = g.nodes.at(node1_idx);
      for (uint32_t off1 = 0; off1 < n1.num_forward_edges; ++off1) {
        if (g.edges.at(n1.edges_start_pos + off1).target_idx == node2_idx) {
          // Found a path
          return {
              .node0_idx = node0_idx,
              .node1_idx = node1_idx,
              .node2_idx = node2_idx,
              .edge0_off = off0,
              .edge1_off = off1,
              .valid = true,
          };
        }
      }
    }
  }
  return {.valid = false};
}

// Return a vector containing all paths of two edges starting with the edge
// 'fe'.
inline std::vector<N3Path> GetN3Paths(const Graph& g, FullEdge fe) {
  std::vector<N3Path> result;
  const GNode& target_node = fe.target_node(g);
  for (uint32_t off = 0; off < target_node.num_forward_edges; ++off) {
    result.push_back({
        .node0_idx = fe.start_idx(),
        .node1_idx = fe.target_idx(g),
        .node2_idx = g.edges.at(target_node.edges_start_pos + off).target_idx,
        .edge0_off = fe.offset(),
        .edge1_off = off,
        .valid = true,
    });
  }
  return result;
}
