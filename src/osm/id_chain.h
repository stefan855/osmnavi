#pragma once

// OSM contains Relations with lists of ways/nodes. These lists sometimes are
// connected by node_ids. In this module, we call such a list a IdChain.
//
// Examples are country polygons (a list of ways forming a closed polygon) or
// turn restrictions (a list of ways and/or nodes forming a poly line).
//
// When building an IdChain from such a relation, two problems have to be
// solved. A way may need to be reversed to properly connect to the previous way
// or node in the chain, and when the chain can't be connected properly, then an
// error has to be issued.

#include <cstdint>
#include <vector>

#include "absl/strings/str_format.h"
#include "graph/graph_def.h"

class IdChain {
 public:
  // IdPair Represents a way or a node. For ways, node_idx1 is the first node_id
  // in the way and node_idx2 is the last node_id in the way, for nodes both ids
  // are identical.
  struct IdPair {
    // 'free' means that an element in a chain can be reversed if needed. Note
    // that the first element in a chain is initially added as 'free'. This is a
    // special case, because the first element may be reversed when adding the
    // second element. All elements after the first element are added as needed
    // in state 'Normal' or 'Reversed', unless there is an error.
    enum Code { Free, Normal, Reversed, Error };
    // Only set if created from a way.
    uint32_t way_idx = INFU32;
    uint32_t node_idx1 = INFU32;
    uint32_t node_idx2 = INFU32;
    Code code = Free;

    // Set new code. Check fails if the current code is not 'free'. Swaps node
    // ids if the new code is 'Reversed'.
    void SetCode(Code new_code) {
      CHECK_NE_S(new_code, Free);
      code = new_code;
      if (new_code == Reversed) {
        std::swap(node_idx1, node_idx2);
      }
    }
  };
  IdChain(int64_t relation_id) : relation_id_(relation_id), success_(true) {}

  // Create an IdPair from a way. Returns true on success, or false if the way
  // wasn't found.
  static bool CreateIdPairFromWay(const Graph& g, int64_t way_id,
                                  IdPair* pair) {
    pair->way_idx = g.FindWayIndex(way_id);
    // LOG_S(INFO) << "BB0 way_idx:" << pair->way_idx << " way_id:" << way_id;
    if (pair->way_idx >= g.ways.size()) return false;
    GetFrontAndBackNodeId(g, g.ways.at(pair->way_idx), &pair->node_idx1,
                          &pair->node_idx2);
   //  LOG_S(INFO) << "BB1";
    return true;
  }

  // Add a new pair to the end of an existing chain. Return true if the new
  // element could be added without error, false if there was an error.
  bool AddIdPair(const IdPair& p) {
    if (chain_.empty() || chain_.back().code == IdPair::Error) {
      chain_.push_back(p);  // Leave in state 'Free'.
      return true;
    }
    IdPair::Code code = TryAddIdPair(chain_.back().node_idx2, p);
    if (chain_.back().code == IdPair::Free) {
      chain_.back().SetCode(IdPair::Normal);
      if (code == IdPair::Error) {
        code = TryAddIdPair(chain_.back().node_idx1, p);
        if (code != IdPair::Error) {
          chain_.back().SetCode(IdPair::Reversed);
        }
      }
    }
    chain_.push_back(p);
    chain_.back().SetCode(code);
    success_ &= (code != IdPair::Error);
    return code != IdPair::Error;
  }

  std::string GetChainCodeString() const {
    std::string res;
    for (const IdPair& p : chain_) {
      // 'Normal' and 'Free' are both output as "N".
      absl::StrAppend(&res, p.code == IdPair::Error      ? "x"
                            : p.code == IdPair::Reversed ? "R"
                                                         : "N");
    }
    return res;
  }

  const std::vector<IdPair>& get_chain() const { return chain_; }
  int64_t relation_id() const { return relation_id_; }
  bool success() const { return success_; }

 private:
  std::vector<IdPair> chain_;
  int64_t relation_id_;
  bool success_;

  static void GetFrontAndBackNodeId(const Graph& g, const GWay& w,
                                    uint32_t* front_idx, uint32_t* back_idx) {
    std::vector<uint64_t> ids = Graph::GetGWayNodeIds(w);
    CHECK_GE_S(ids.size(), 2);
    *front_idx = g.FindNodeIndex(ids.front());
    *back_idx = g.FindNodeIndex(ids.back());
    CHECK_S(*front_idx < g.nodes.size()) << absl::StrFormat(
        "Front node %lld of way %lld not found in graph!", ids.front(), w.id);
    CHECK_S(*back_idx < g.nodes.size()) << absl::StrFormat(
        "Back node %lld of way %lld not found in graph!", ids.back(), w.id);
  }

  // Find out if pair can be chained to predecessor_id.
  // Return IdPair::Normal or IdPair::Reversed if pair can be connected, or
  // IdPair::Error if it can't be connected.
  static IdPair::Code TryAddIdPair(uint32_t predecessor_idx,
                                   const IdPair& pair) {
    CHECK_S(predecessor_idx != INFU32);
    CHECK_EQ_S(pair.code, IdPair::Free);
    if (pair.node_idx1 == predecessor_idx) {
      return IdPair::Normal;
    } else if (pair.node_idx2 == predecessor_idx) {
      return IdPair::Reversed;
    }
    return IdPair::Error;
  }
};
