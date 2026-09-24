#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

#include "algos/tarjan_scc.h"
#include "base/util.h"

void TestTarjanSCC() {
  FUNC_TIMER();

  struct MyThinGraph : ThinGraph {
    // Print nice names "A", "B" etc.
    virtual std::string DebugStr(uint32_t node_idx) const {
      return absl::StrFormat("%c", 'A' + node_idx);
    }
  };
  enum NodesEnum : uint8_t { A = 0, B, C, D, E, F, G, H, I, J, MAX };

  MyThinGraph g;
  g.AddEdge(A, B);
  g.AddEdge(B, C);
  g.AddEdge(B, D);
  g.AddEdge(C, A);
  g.AddEdge(D, E);
  g.AddEdge(E, F);
  g.AddEdge(F, E);
  g.AddEdge(H, E);  // Node G is not mentioned, but should exist after this.
  g.AddSentinel();

  TarjanSCC t(g);

  t.DFS(TarjanSCC::Iterative);

  const std::vector<TarjanSCC::SCC>& sccs = t.GetSCCs();
  // We expect sccs {A,B,C}, {D}, {E,F}, {G}, {H}
  CHECK_EQ_S(sccs.size(), 5);
  // The order of the clusters is given by the way Tarjan visits the nodes. We
  // expect this fixed order of SCCs and also of nodes in an SCC.
  compare_check_vectors("", sccs.at(0).nodes, std::vector<uint32_t>({F, E}));
  compare_check_vectors("", sccs.at(1).nodes, std::vector<uint32_t>({D}));
  compare_check_vectors("", sccs.at(2).nodes, std::vector<uint32_t>({C, B, A}));
  compare_check_vectors("", sccs.at(3).nodes, std::vector<uint32_t>({G}));
  compare_check_vectors("", sccs.at(4).nodes, std::vector<uint32_t>({H}));
}

int main(int argc, char* argv[]) {
  InitLogging(argc, argv);
  if (argc != 1) {
    ABORT_S() << absl::StrFormat("usage: %s", argv[0]);
  }

  TestTarjanSCC();

  LOG_S(INFO)
      << "\n\033[1;32m*****************************\nTesting successfully "
         "finished\n*****************************\033[0m";
  return 0;
}
