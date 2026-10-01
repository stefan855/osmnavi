#pragma once

#include <deque>
#include <queue>
#include <vector>

#include "base/constants.h"
#include "base/util.h"

// ThinGraph contains the absolute necessary graph information for a directed
// graph.
//
// Nodes are numbered [0..num_nodes-1]. There is no additional data stored for
// nodes.
//
// The outgoing edges (i.e. the target nodes)  of node k are stored in targets,
// at indices starts[k]..starts[k+1] - 1.
//
// Note that you may add more data by subclassing the struct.
struct ThinGraph {
  // Number of nodes, index [0..num_nodes-1].
  uint32_t num_nodes = 0;
  // Edges are stored in two vectors. The target nodes of of node i are stored
  // in targets at positions [starts[i]..starts[i+1]-1].
  std::vector<uint32_t> starts;  // Dim num_nodes + 1.
  std::vector<uint32_t> targets;

  // Add an edge to the graph. This makes two assumptions:
  //   1) Nodes have numbers 0..max(seen value for from/to node). So every time
  //   an edge is added, 'num_nodes' might be updated depending on the maximum
  //   from_node and to_node values seen.
  //
  //   2) Edges need to be added in non-decreasing order by from_node.
  void AddEdge(uint32_t from_node, uint32_t to_node, bool verbose = false) {
    if (verbose) {
      LOG_S(INFO) << "\nAdd Tarjan edge\n" + DebugStr(from_node) + "\n" +
                         DebugStr(to_node);
    }
    // Check ordering.
    CHECK_GE_S(from_node + 1, starts.size());
    // Add missing start entries.
    while (from_node >= starts.size()) {
      starts.push_back(targets.size());
    }
    targets.push_back(to_node);
    num_nodes = std::max({num_nodes, from_node + 1, to_node + 1});
  }

  // Call this after the last call to AddEdge().
  void AddSentinel() {
    while (starts.size() < num_nodes + 1) {
      starts.push_back(targets.size());
    }
  }

  virtual std::string DebugStr(uint32_t node_idx) const {
    return std::to_string(node_idx);
  }
};

// Find strongly connected components (SCCs) in a directed graph.
class TarjanSCC {
 public:
  enum RecursiveMode { Recursive, Iterative };
  struct SCC {
    uint32_t scc_no;
    std::vector<uint32_t> nodes;
  };

  TarjanSCC(const ThinGraph& g)
      : g_(g),
        num_(g_.num_nodes, 0),
        lownum_(g_.num_nodes, 0),
        visited_(g_.num_nodes, false),
        is_pending_(g_.num_nodes, false),
        cur_num_(1),
        scc_no_(0) {}

  void DFS(RecursiveMode recursive_mode) {
    CHECK_EQ_S(g_.starts.size(), g_.num_nodes + 1);
    LOG_S(INFO) << "Start TarjanSCC #nodes:" << g_.num_nodes
                << " #edges:" << g_.targets.size();
    if (recursive_mode == Recursive) {
      for (uint32_t v = 0; v < g_.num_nodes; ++v) {
        if (!visited_.at(v)) {
          DFSRecursive(v);
        }
      }
    } else {
      for (uint32_t v = 0; v < g_.num_nodes; ++v) {
        if (!visited_.at(v)) {
          DFSIterative(v);
        }
      }
    }
  }

  const std::vector<SCC>& GetSCCs() { return sccs_; }

 private:
  const ThinGraph& g_;
  std::vector<uint32_t> num_;
  std::vector<uint32_t> lownum_;
  std::vector<bool> visited_;  // bit packed.
  std::vector<bool> is_pending_;  // bit packed.
  // Nodes visited but not yet processed, used like a stack.
  std::vector<uint32_t> pending_;
  std::vector<SCC> sccs_;

  uint32_t cur_num_;
  uint32_t scc_no_;

  void AddSCC(uint32_t v) {
    CHECK_EQ_S(lownum_.at(v), num_.at(v));

    // Found SCC.
    sccs_.push_back({.scc_no = scc_no_++});
    uint32_t k;
    do {
      k = pending_.back();
      pending_.pop_back();
      is_pending_.at(k) = false;
      sccs_.back().nodes.push_back(k);
    } while (k != v);
    LOG_S(INFO) << "Component:" << sccs_.back().scc_no
                << " #nodes:" << sccs_.back().nodes.size();
    for (size_t i = 0; i < std::min(4lu, sccs_.back().nodes.size()); ++i) {
      LOG_S(INFO) << "  " << i << ": " << g_.DebugStr(sccs_.back().nodes.at(i));
    }
  }

  void DFSRecursive(uint32_t v) {
    // When creating DFSIterative, all comments below with a number (N) are
    // entry points into the loop of DFSIterative.

    // Have entered new recursion (1).
    num_.at(v) = cur_num_;
    lownum_.at(v) = cur_num_;
    cur_num_++;
    visited_.at(v) = true;
    pending_.push_back(v);
    is_pending_.at(v) = true;
    LOG_S(INFO) << "Enter node " << v;

    // Setup target looping.
    for (uint32_t target_idx = g_.starts.at(v);
         target_idx < g_.starts.at(v + 1); ++target_idx) {
      LOG_S(INFO) << absl::StrFormat("Loop   node %u num=%u lownum=%u", v,
                                     num_.at(v), lownum_.at(v));
      // Loop Next target (2).
      uint32_t target = g_.targets.at(target_idx);

      if (!visited_.at(target)) {
        // Enter recursion.
        DFSRecursive(target);
        // Returned from recursion (3).
        lownum_.at(v) = std::min(lownum_.at(v), lownum_.at(target));
      } else if (is_pending_.at(target)) {
        lownum_.at(v) = std::min(lownum_.at(v), num_.at(target));
      }
    }

    // Finalize node. (4)
    if (lownum_.at(v) == num_.at(v)) {
      AddSCC(v);
    }
    // Return from recursion.
    LOG_S(INFO) << absl::StrFormat("Return node %u num=%u lownum=%u", v,
                                   num_.at(v), lownum_.at(v));
  }

  // Iterative version of the recursive function above, allows running on much
  // larger graphs.
  //
  // This version was handcrafted by me. Out of curiosity I created two more
  // version using AI tools, for comparison. See DFSIterative2 and
  // DFSIterative3 below.
  void DFSIterative(uint32_t start_v) {
    enum Action {
      EnterRecursion,
      LoopNextTarget,
      ReturnFromRecursion,
      FinalizeNode
    };
    struct StackFrame {
      // Parameters.
      const uint32_t v;
      // Variables on stack.
      uint32_t target_idx;
    };

    LOG_S(INFO) << "DFSIterative called with node " << g_.DebugStr(start_v);
    // Replacement for the stack that we have when doing real recursion.
    std::vector<StackFrame> stack;
    stack.push_back({.v = start_v, .target_idx = MAXU32});
    Action action = EnterRecursion;

    while (true) {
      StackFrame& f = stack.back();  // Invalidated when stack is changed!
      switch (action) {
        case EnterRecursion:
          // Start recursion.
          num_.at(f.v) = cur_num_;
          lownum_.at(f.v) = cur_num_;
          cur_num_++;
          visited_.at(f.v) = true;
          pending_.push_back(f.v);
          is_pending_.at(f.v) = true;
          // Setup target looping.
          f.target_idx = g_.starts.at(f.v);
          /* FALL THROUGH */
        case LoopNextTarget:
          if (f.target_idx >= g_.starts.at(f.v + 1)) {
            // Stop loop
            action = FinalizeNode;
          } else {
            const uint32_t target = g_.targets.at(f.target_idx);
            if (!visited_.at(target)) {
              // Enter recursion.
              stack.push_back({.v = target, .target_idx = MAXU32});
              action = EnterRecursion;
            } else {
              if (is_pending_.at(target)) {
                lownum_.at(f.v) = std::min(lownum_.at(f.v), num_.at(target));
              }
              f.target_idx++;
              action = LoopNextTarget;
            }
          }
          break;
        case ReturnFromRecursion: {
          // Refetch target, we had it before entering recursion and it
          // shouldn't have changed.
          const uint32_t target = g_.targets.at(f.target_idx);
          lownum_.at(f.v) = std::min(lownum_.at(f.v), lownum_.at(target));
          f.target_idx++;
          action = LoopNextTarget;
          break;
        }
        case FinalizeNode: {
          if (lownum_.at(f.v) == num_.at(f.v)) {
            AddSCC(f.v);
          }
          stack.pop_back();
          if (stack.empty()) {
            return;  // We're done.
          }
          action = ReturnFromRecursion;
          break;
        }
        default:
          CHECK_S(0) << action;
      }
    }
  }
};
