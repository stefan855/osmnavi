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
  // Represents a way or a node. For ways, id1 is the first node_id in the way
  // and id2 is the last node_id in the way, for nodes both ids are identical.
  struct IdPair {
    // 'free' means that an element in a chain can be reversed if needed. Note
    // that the first element in a chain is initially added as 'free'. This is a
    // special case, because the first element may be reversed when adding the
    // second element. All elements after the first element are added as needed
    // in state 'Normal' or 'Reversed', unless there is an error.
    enum Code { Free, Normal, Reversed, Error };
    int64_t id1;
    int64_t id2;
    Code code = Free;
    void SetCode(Code new_code) {
      CHECK_NE_S(new_code, Free);
      code = new_code;
      if (new_code == Reversed) {
        std::swap(id1, id2);
      }
    }
  };
  IdChain() : success_(true) {}

  // Create an IdPair from a way. Returns true on success, or false if the way
  // doesn't exist.
  static bool CreateIdPair(const Graph& g, int64_t way_id, IdPair* pair) {
    const GWay* way = g.FindWay(way_id);
    if (way == nullptr) return false;
    GetFrontAndBackNodeId(*way, &pair->id1, &pair->id2);
    return true;
  }

  // Add a new pair to the end of an existing chain. Return true if the new
  // element could be added without error, false if there was an error.
  bool AddIdPair(const IdPair& p) {
    if (chain_.empty() || chain_.back().code == IdPair::Error) {
      chain_.push_back(p);  // Leave in state 'Free'.
      return true;
    }
    IdPair::Code code = TryAddIdPair(chain_.back().id2, p);
    if (chain_.back().code == IdPair::Free) {
      chain_.back().SetCode(IdPair::Normal);
      if (code == IdPair::Error) {
        code = TryAddIdPair(chain_.back().id1, p);
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

  const std::vector<IdPair>& get_chain() { return chain_; }
  const bool success() { return success_; }

 private:
  std::vector<IdPair> chain_;
  bool success_;

  static void GetFrontAndBackNodeId(const GWay& w, int64_t* front_id,
                                    int64_t* back_id) {
    std::vector<uint64_t> ids = Graph::GetGWayNodeIds(w);
    CHECK_GE_S(ids.size(), 2);
    *front_id = ids.front();
    *back_id = ids.back();
  }

  // Find out if pair can be chained to predecessor_id.
  // Return IdPair::Normal or IdPair::Reversed if pair can be connected, or
  // IdPair::Error if it can't be connected.
  static IdPair::Code TryAddIdPair(int64_t predecessor_id, const IdPair& pair) {
    CHECK_EQ_S(pair.code, IdPair::Free);
    if (pair.id1 == predecessor_id) {
      return IdPair::Normal;
    } else if (pair.id2 == predecessor_id) {
      return IdPair::Reversed;
    }
    return IdPair::Error;
  }
};
