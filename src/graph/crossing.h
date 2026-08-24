#pragma once

#include <vector>

#include "base/uint_types.h"
#include "geometry/distance.h"
#include "graph/graph_def.h"
#include "graph/graph_def_utils.h"

// Determine the conflicts at a crossing with other paths when travelling along
// the path given by parameter 'n3p'.
class CrossingHelper {
 public:
  // Note that the value of the enum correspond to the position in conns().
  enum Dir { Incoming = 0, Outgoing = 1 };
  struct Connection {
    uint32_t node_idx;  // The external node.
    Dir dir : 1;
    // Highway label of the underlying edge.
    HIGHWAY_LABEL hw : NUM_HIGHWAY_LABEL_BITS;
    // ROAD_PRIORITY of the underlying edge.
    GEdge::ROAD_PRIORITY road_priority : NUM_GEDGE_ROAD_PRIORITY_BITS;
    // The bearing for the edge.
    uint16_t bearing;  // [0..359] degrees.
    uint16_t rank;

    uint16_t out_bearing() {
      return (dir == Outgoing) ? bearing : invert_bearing(bearing);
    }

    std::string DebugStr(const Graph& g) const {
      return absl::StrFormat(
          "Connection node:%lu %s hw:%s prio:%u bearing:%u rank:%u",
          GetGNodeIdSafe(g, node_idx), dir == Incoming ? "IN" : "OUT",
          hw == HW_MAX ? "HW_MAX" : HighwayLabelToString(hw), road_priority,
          bearing, rank);
    }
  };

  // Merge means that the out leg merges with the out leg of the original
  // path.
  enum Type { Merge, Cross };
  enum Side { None, Left, Right };

  // Describes a conflict between the given path 'n3p' and another path
  // <in_conn_pos> -> <out_conn_pos> through the crossing.
  struct ConflictPath {
    // Arriving from original direction is never considered a conflict, i.e.
    // this is never 0.
    uint16_t in_conn_pos;
    // For type == Merge this is 1.
    uint16_t out_conn_pos;
    Type type : 1;
    Side from_side : 2;        // The incoming edge is coming from this side.
    uint16_t higher_prio : 1;  // 1 if this path has higher priority, 0 if not.
  };

  CrossingHelper(const Graph& g, const N3Path& n3p) : g(g), n3p(n3p) {
    AddConnections();
  }
  CrossingHelper() = delete;

  const Connection& conn(size_t pos) const { return conns_.at(pos); }
  const std::vector<Connection>& conns() const { return conns_; }

  // In- and out-leg of the path 'n3p'.
  const Connection& in() const { return conns_.at(Incoming); }
  const Connection& out() const { return conns_.at(Outgoing); }

  void FindConflictPaths(std::vector<ConflictPath>* conflicts) const {
    CHECK_S(conflicts->empty());

    // node 13640468744 is part of two overlapping ways.
    if (GetGNodeIdSafe(g, n3p.node1_idx) == 13640468744) {
      LOG_S(INFO) << "Debug node 13640468744: All edges of node "
                  << GetGNodeIdSafe(g, n3p.node1_idx);
      for (const GEdge& e : gnode_all_edges(g, n3p.node1_idx)) {
        LOG_S(INFO) << "Debug node 13640468744: " << debug_str(g, e);
      }
      for (const auto& conn : conns_) {
        LOG_S(INFO) << "Debug node 13640468744: " << conn.DebugStr(g);
      }
    }

    // Ignore the first two connections.
    for (uint16_t in_pos = 2; in_pos < conns_.size(); ++in_pos) {
      Connection in_leg = conns_.at(in_pos);
      if (in_leg.dir != Incoming) {
        continue;
      }
      if (in_leg.bearing == in().bearing) {
        // Separate node at same angle sends incoming edge. Probably a mapping
        // error.
        LOG_S(INFO) << "Additional IN conn with same angle node "
                    << n3p.node1(g).node_id;
        for (const GEdge& e : gnode_all_edges(g, n3p.node1_idx)) {
          LOG_S(INFO) << "  Edge node " << n3p.node1(g).node_id << ":"
                      << debug_str(g, e);
        }
        for (const auto& conn : conns_) {
          LOG_S(INFO) << "  Connection node " << n3p.node1(g).node_id << ":"
                      << conn.DebugStr(g);
        }
        continue;
      }
      for (uint16_t out_pos = 1; out_pos < conns_.size(); ++out_pos) {
        Connection out_leg = conns_.at(out_pos);
        if (out_leg.dir != Outgoing) {
          continue;
        }
        // Check for conflict.
        if (out_leg.bearing == out().bearing) {
          if (out_pos != Outgoing) {
            // Separate node at same angle as outgoing. Probably a mapping
            // error.
            LOG_S(INFO) << "Additional OUT conn with same angle node "
                        << n3p.node1(g).node_id;
            for (const GEdge& e : gnode_all_edges(g, n3p.node1_idx)) {
              LOG_S(INFO) << "  Edge node " << n3p.node1(g).node_id << ":"
                          << debug_str(g, e);
            }
            for (const auto& conn : conns_) {
              LOG_S(INFO) << "  Connection node " << n3p.node1(g).node_id << ":"
                          << conn.DebugStr(g);
            }
          }
          // Merge.
          conflicts->push_back({.in_conn_pos = in_pos,
                                .out_conn_pos = out_pos,
                                .type = Merge,
                                .from_side = GetSide(in_leg)});
          conflicts->back().higher_prio = HasHigherPriority(conflicts->back());
        } else {
          // Cross.
          const Side in_side = GetSide(in_leg);
          const Side out_side = GetSide(out_leg);

          // if (in_side == None || out_side == None) continue;  // TODO!!!!
          CHECK_NE_S(in_side, None) << n3p.DebugStr(g);
          CHECK_NE_S(out_side, None) << n3p.DebugStr(g);
          if (in_side != out_side) {
            conflicts->push_back({.in_conn_pos = in_pos,
                                  .out_conn_pos = out_pos,
                                  .type = Cross,
                                  .from_side = in_side});
            conflicts->back().higher_prio =
                HasHigherPriority(conflicts->back());
          }
        }
      }
    }
  }

  const Graph& g;
  const N3Path& n3p;
  // TODO: lhs countries. See config/left_traffic_countries.cfg.
  const bool right_hand_side_driving = true;

 private:
  // The incoming edge is stored at pos 0, the outgoing edge at pos 1. The rest
  // is all other edges at the middle node.
  std::vector<Connection> conns_;

  // Return true if this path has higher priority than the input path.
  bool HasHigherPriority(const ConflictPath& confl) const {
    Connection confl_in = conn(confl.in_conn_pos);

    if (in().road_priority == GEdge::PRIO_SIGNALS) {
      // The waiting time has been accounted for at the node with the singal,
      // so We assume that we can just drive now.
      // TODO: Handle turns that still need some additional waiting despite
      // having a green light.
      return false;
    } else if (confl_in.road_priority == GEdge::PRIO_SIGNALS) {
      // Doesn't really make sense, one side has signals, the other not.
      // TODO: Investigate cases that end up here.
      return false;
    }

    // PRECOND: both incoming connections don't have signals.
    if (in().road_priority != confl_in.road_priority) {
      if (in().road_priority == GEdge::PRIO_LOW) {
        return true;
      } else if (confl_in.road_priority == GEdge::PRIO_LOW) {
        return false;
      } else {
        return confl_in.road_priority < in().road_priority;
      }
    }
    CHECK_EQ_S(in().road_priority, confl_in.road_priority);

    if (in().hw == confl_in.hw ||
        // All unimportant/unclear highways are treated as having the same very
        // low priority.
        (in().hw > HW_RESIDENTIAL && confl_in.hw > HW_RESIDENTIAL)) {
      // Same highway type, so right/left makes the winner.
      return right_hand_side_driving == (confl.from_side == Right);
    } else {
      return confl_in.hw < in().hw;
    }
  }

  Side GetSide(const Connection& c) const {
    if (OnLeftSide(c)) {
      return Left;
    } else if (OnRightSide(c)) {
      return Right;
    }
    return None;
  }

  // Returns true if c is strictly on the left side of in/out edge.
  bool OnLeftSide(const Connection& c) const {
    if (out().rank > in().rank) {
      return c.rank > in().rank && c.rank < out().rank;
    } else {
      // out().rank has wrapped.
      return c.rank > in().rank || c.rank < out().rank;
    }
  }

  // Returns true if c is strictly on the right side of in/out edge.
  bool OnRightSide(const Connection& c) const {
    if (in().rank > out().rank) {
      return c.rank > out().rank && c.rank < in().rank;
    } else {
      // in().rank has wrapped.
      return c.rank > out().rank || c.rank < in().rank;
    }
  }

  // Returns the edge_idx with the best highway tag in all edges
  // from_idx->to_idx.
  //
  // Returns INFU32 if the connection doesn't exist.
  inline uint32_t GetBestHighwayEdge(uint32_t from_idx, uint32_t to_idx) {
    HIGHWAY_LABEL hw = HW_MAX;
    uint32_t idx = INFU32;
    for (const GEdge& e : gnode_forward_edges(g, from_idx)) {
      if (e.target_idx == to_idx && g.ways.at(e.way_idx).highway_label < hw) {
        hw = g.ways.at(e.way_idx).highway_label;
        idx = gnode_edge_idx(g, e);
      }
    }
    return idx;
  }

  bool ConnExists(Dir dir, uint32_t node_idx) {
    for (const auto& c : conns_) {
      if (c.dir == dir && c.node_idx == node_idx) {
        return true;
      }
    }
    return false;
  }

  // Rank is using bearing * 10 as basis and adds 1 depending on rhs and
  // incoming/outgoing edge type.
  uint16_t ComputeRank(Dir dir, uint16_t bearing) {
    // We use the same bearing for incoming and outgoing edges.
    uint16_t out_bearing =
        (dir == Outgoing) ? bearing : invert_bearing(bearing);
    // Add 1 if
    //   right_hand_side_driving is true and direction is outgoing,
    // or
    //   right_hand_side_driving is false and direction is incoming.
    return out_bearing * 10 +
           ((right_hand_side_driving == (dir == Outgoing)) ? 1 : 0);
  }

  // Add an incoming connection if it exists.
  void MayAddInConn(uint32_t node_idx) {
    if (ConnExists(Incoming, node_idx)) return;
    const uint32_t e_idx = GetBestHighwayEdge(node_idx, n3p.node1_idx);
    if (e_idx != INFU32) {
      const GEdge& e = g.edges.at(e_idx);
      /*
      LOG_S(INFO) << absl::StrFormat(
          "ZZ1 IN connection %lu->%lu start_bearing:%u target_bearing:%u",
          GetGNodeIdSafe(g, node_idx), GetGNodeIdSafe(g, n3p.node1_idx),
          e.start_bearing, e.target_bearing);
      */
      conns_.push_back({.node_idx = node_idx,
                        .dir = Incoming,
                        .hw = g.ways.at(e.way_idx).highway_label,
                        .road_priority = e.road_priority,
                        .bearing = e.target_bearing,
                        .rank = ComputeRank(Incoming, e.target_bearing)});
    }
  }

  // Add an outgoing connection if it exists.
  void MayAddOutConn(uint32_t node_idx) {
    if (ConnExists(Outgoing, node_idx)) return;
    const uint32_t e_idx = GetBestHighwayEdge(n3p.node1_idx, node_idx);
    if (e_idx != INFU32) {
      const GEdge& e = g.edges.at(e_idx);
      /*
      LOG_S(INFO) << absl::StrFormat(
          "ZZ2 OUT connection %lu->%lu start_bearing:%u target_bearing:%u",
          GetGNodeIdSafe(g, n3p.node1_idx), GetGNodeIdSafe(g, node_idx),
          e.start_bearing, e.target_bearing);
      */
      conns_.push_back({.node_idx = node_idx,
                        .dir = Outgoing,
                        .hw = g.ways.at(e.way_idx).highway_label,
                        .road_priority = e.road_priority,
                        .bearing = e.start_bearing,
                        .rank = ComputeRank(Outgoing, e.start_bearing)});
    }
  }

  // Add all connections of the middle node of n3p, which represents the
  // center of the crossing. The incoming/outgoing edges are put at positions
  // 0/1.
  void AddConnections() {
    MayAddInConn(n3p.node0_idx);   // Incoming edge is at position 0.
    MayAddOutConn(n3p.node2_idx);  // Outgoing edge is at position 1.
    CHECK_EQ_S(conns_.size(), 2) << n3p.DebugStr(g);

    // Add all other connections.
    const uint32_t middle_idx = n3p.node1_idx;
    for (const GEdge& e : gnode_all_edges(g, middle_idx)) {
      if (e.target_idx != middle_idx) {
        MayAddInConn(e.target_idx);
        MayAddOutConn(e.target_idx);
      }
    }
  }
};

inline DurationMS CrossingCost(const Graph& g, VEHICLE vt, const N3Path& n3p,
                               bool debug) {
  DurationMS cost(0u);

  if (debug) {
    LOG_S(INFO) << "Compute crossing cost for " << n3p.DebugStr(g);
  }
  // crossing.
  if (gnode_num_unique_edges(g, n3p.node1_idx) <= 2) {
    // Most simple case: a street that just continues, i.e. no real crossing.
    if (debug) {
      LOG_S(INFO) << "Return cost " << cost;
    }
    return cost;
  }

  const CrossingHelper hlp(g, n3p);
  std::vector<CrossingHelper::ConflictPath> conflicts;
  hlp.FindConflictPaths(&conflicts);
  if (debug) {
    LOG_S(INFO) << "Incoming: " << hlp.conn(0).DebugStr(g);
    LOG_S(INFO) << "Outgoing: " << hlp.conn(1).DebugStr(g);
  }

  for (const CrossingHelper::ConflictPath& confl : conflicts) {
    if (debug) {
      LOG_S(INFO) << "Conflict Incoming: "
                  << hlp.conn(confl.in_conn_pos).DebugStr(g);
      LOG_S(INFO) << "Conflict Outgoing: "
                  << hlp.conn(confl.out_conn_pos).DebugStr(g);
    }
    uint32_t new_cost = 0;
    if (confl.higher_prio) {
      new_cost = (confl.type == CrossingHelper::Merge) ? 3000 : 6000;
    } else {
      new_cost = 100;
    }
    // Weight the penalty by traffic on both incoming and outgoing leg.
    const double fraction =
        (HighwayToTrafficFraction(hlp.conn(confl.in_conn_pos).hw) +
         HighwayToTrafficFraction(hlp.conn(confl.out_conn_pos).hw)) /
        2.0;
    new_cost = new_cost * fraction;
    cost += new_cost;

    if (debug) {
      LOG_S(INFO) << "Increase cost by " << new_cost << " to " << cost;
    }
  }

  if (debug) {
    LOG_S(INFO) << absl::StrFormat(
        "Crossing connections:%lu conflicts:%lu cost:%u", hlp.conns().size(),
        conflicts.size(), cost.ms());
  }

  return cost;
}
