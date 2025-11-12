#include <osmpbf/osmpbf.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

#include <memory>

#include "algos/louvain.h"
#include "algos/louvain_precluster.h"
#include "base/argli.h"
#include "base/util.h"
#include "logging/loguru.h"

namespace louvain {

int32_t FindWeight(const LouvainGraph& g, const LouvainNode& n,
                   uint32_t other_pos) {
  for (uint32_t p = n.edge_start; p < n.edge_start + n.num_edges; ++p) {
    if (g.edges.at(p).other_node_pos == other_pos) {
      return g.edges.at(p).weight;
    }
  }
  return 0;
}

double ComputeModularity(const LouvainGraph& g) {
  // Follows the formula in the original paper:
  // Q = 1/2m sum((Aij - ki*kj/2m) * sigma_ij)
  // sigma_ij is 1 if node i and j are in the same cluster, 0 if not.
  double two_m = 0.0;
  for (const auto& e : g.edges) {
    two_m += e.weight;
  }
  LOG_S(INFO) << "2m is " << two_m;

  double sum_loop = 0.0;
  for (size_t i = 0; i < g.nodes.size(); ++i) {
    const auto& ni = g.nodes.at(i);
    for (size_t j = 0; j < g.nodes.size(); ++j) {
      const auto& nj = g.nodes.at(j);
      if (i != j && (ni.cluster_pos != nj.cluster_pos)) {
        continue;
      }
      double w = FindWeight(g, ni, j);
      sum_loop += (w - g.resolution_ * (ni.w_tot * nj.w_tot) / two_m);
    }
  }
  return sum_loop / two_m;
}

void TestLouvainTriangleGrpah() {
  FUNC_TIMER();
  // These values where manually verified.
  constexpr double kExpectedModularityStart = -0.173469387755;
  constexpr double kExpectedModularityEnd = 0.357142857143;
  constexpr double kQMaxDiff = 0.00000001;

  // Two triangles (0,1,2) and (3,4,5) interconnected at 2 and 5.
  LouvainGraph g;
  g.resolution_ = 1.0;

  g.AddNodeAndEdges(0, {1, 2, 3});
  g.AddNodeAndEdges(1, {0, 4, 5});
  g.AddNodeAndEdges(2, {0, 3});
  g.AddNodeAndEdges(3, {0, 2});
  g.AddNodeAndEdges(4, {1, 5});
  g.AddNodeAndEdges(5, {1, 4});
  g.Validate();

  // Check initial quality.
  const double tot_q_start = g.TotalQuality();
  const double tot_q_start_computed = ComputeModularity(g);
  CHECK_S(std::abs(tot_q_start - kExpectedModularityStart) < kQMaxDiff)
      << tot_q_start;
  CHECK_S(std::abs(tot_q_start_computed - kExpectedModularityStart) < kQMaxDiff)
      << tot_q_start_computed;

  // Iterate...
  g.DebugPrint();
  g.Step();
  g.DebugPrint();
  g.Step();
  g.DebugPrint();
  g.Validate();

  CHECK_EQ_S(g.CountClusters(), 2u);
  // CHECK_EQ_S(g.nodes.at(0).cluster_pos, g.nodes.at(1).cluster_pos);
  // CHECK_EQ_S(g.nodes.at(0).cluster_pos, g.nodes.at(2).cluster_pos);
  // CHECK_EQ_S(g.nodes.at(3).cluster_pos, g.nodes.at(4).cluster_pos);
  // CHECK_EQ_S(g.nodes.at(3).cluster_pos, g.nodes.at(5).cluster_pos);

  // Check final quality.
  const double tot_q_end = g.TotalQuality();
  const double tot_q_end_computed = ComputeModularity(g);
  CHECK_S(std::abs(tot_q_end - kExpectedModularityEnd) < kQMaxDiff)
      << tot_q_end;
  CHECK_S(std::abs(tot_q_end_computed - kExpectedModularityEnd) < kQMaxDiff)
      << tot_q_end_computed;

  // Test quality change when removing node 2 from it's cluster. Node 2 has two
  // edges and is connected to the first triangle.
  {
    LouvainNode n2 = g.nodes.at(2);
    LouvainCluster c = g.clusters.at(n2.cluster_pos);
    // Cluster c has w_in:6 w_tot:7.
    CHECK_EQ_S(c.w_inside_edges, 6u);
    CHECK_EQ_S(c.w_tot_edges, 7u);
    double exp_before = 6.0 / 14.0 - (7.0 * 7.0) / (14.0 * 14.0);
    double exp_after =
        2.0 / 14.0 - (5.0 * 5.0) / (14.0 * 14.0) - (2.0 * 2.0) / (14.0 * 14.0);
    double expected_delta = exp_after - exp_before;
    double computed_delta = g.DeltaQualityRemove(2);
    LOG_S(INFO) << "== Remove node 2 from "
                << g.DebugStringCluster(n2.cluster_pos);
    LOG_S(INFO) << "Expected Delta " << expected_delta;
    LOG_S(INFO) << "Computed Delta " << computed_delta;
    // CHECK_EQ_S(expected_delta, computed_delta);
    CHECK_DOUBLE_EQ_S(expected_delta, computed_delta, 0.000000001);
  }

  // Test quality change when adding node 1 (second triangle) to first triangle.
  {
    // Node 1 has 3 edges, one going into cluster of other first triangle.
    // Cluster has w_in:6 w_tot:7.
    double exp_before =
        6.0 / 14.0 - (7.0 * 7.0) / (14.0 * 14.0) - (3.0 * 3.0) / (14.0 * 14.0);
    double exp_after = 8.0 / 14.0 - (10.0 * 10.0) / (14.0 * 14.0);
    double expected_delta = exp_after - exp_before;
    double computed_dq = g.DeltaQualityAdd(1, 0);
    LOG_S(INFO) << "== Add node 1 to "
                << g.DebugStringCluster(g.nodes.at(0).cluster_pos);
    LOG_S(INFO) << "Exp Delta " << exp_after - exp_before;
    LOG_S(INFO) << "Delta " << computed_dq;
    // TODO: Write macro to check floats for approximate equality.
    // This started to fail, buit values are very close.
    // CHECK_EQ_S(computed_dq, expected_delta);
    LOG_S(INFO) << absl::StrFormat("%.20f %.20f", computed_dq, expected_delta);
    CHECK_S(std::abs(computed_dq - expected_delta) <
            (std::abs(computed_dq) + std::abs(expected_delta)) / 100000)
        << computed_dq << " <-> " << expected_delta;
  }

  // Cluster of node 0 is the first triangle.
  {
    LouvainNode n0 = g.nodes.at(0);
    double clusterq_mod_neg = g.ModifiedClusterQ(n0.cluster_pos, -2, -2);
    double clusterq_mod_pos = g.ModifiedClusterQ(n0.cluster_pos, 2, 2);

    // Alternative way to compute the same
    LouvainCluster& c = g.clusters.at(n0.cluster_pos);
    c.w_inside_edges += 2;
    c.w_tot_edges += 2;
    double clusterq_pos = g.ClusterQ(n0.cluster_pos);
    c.w_inside_edges -= 4;
    c.w_tot_edges -= 4;
    double clusterq_neg = g.ClusterQ(n0.cluster_pos);
    CHECK_EQ_S(clusterq_mod_neg, clusterq_neg);
    CHECK_EQ_S(clusterq_mod_pos, clusterq_pos);
  }
}

void TestLouvainSquareGraph() {
  FUNC_TIMER();
  constexpr double kQMaxDiff = 0.00000001;

  LouvainGraph g;
  // Square with additional edge + node at one corner..
  g.AddNodeAndEdges(0, {1, 3});
  g.AddNodeAndEdges(1, {0, 2});
  g.AddNodeAndEdges(2, {1, 3});
  g.AddNodeAndEdges(3, {2, 0});
  // g.AddNodeAndEdges(4, {3}, 1);

  g.DebugPrint();
  g.Validate();

  // Check initial quality.

  const double tot_q_start = g.TotalQuality();
  const double tot_q_start_computed = ComputeModularity(g);
  CHECK_S(std::abs(tot_q_start - tot_q_start_computed) < kQMaxDiff)
      << "Initial quality " << tot_q_start << " vs. " << tot_q_start_computed;

  // Iterate...
  g.DebugPrint();
  g.Step();
  g.DebugPrint();
  g.Step();
  g.DebugPrint();
  g.Validate();

  // Check final quality.
  const double tot_q_end = g.TotalQuality();
  const double tot_q_end_computed = ComputeModularity(g);
  CHECK_S(std::abs(tot_q_end - tot_q_end_computed) < kQMaxDiff)
      << "end quality " << tot_q_end << " vs. " << tot_q_end_computed;
}

void TestLouvainSquareClusterGraph() {
  FUNC_TIMER();

  std::vector<std::unique_ptr<LouvainGraph>> gvec;

  gvec.push_back(std::make_unique<LouvainGraph>());
  LouvainGraph& g = *gvec.back();
  // Square with additional edge + node at one corner..
  g.AddNodeAndEdges(0, {1, 3});
  g.AddNodeAndEdges(1, {0, 2});
  g.AddNodeAndEdges(2, {1, 3});
  g.AddNodeAndEdges(3, {2, 0, 4});
  g.AddNodeAndEdges(4, {5, 7, 3});
  g.AddNodeAndEdges(5, {4, 6});
  g.AddNodeAndEdges(6, {5, 7});
  g.AddNodeAndEdges(7, {6, 4});
  g.Validate();
  g.Step();
  g.Step();
  g.Validate();
  g.DebugPrint();
  LOG_S(INFO) << "Remove empty clusters...";
  RemoveEmptyClusters(&g);
  g.DebugPrint();

  gvec.push_back(std::make_unique<LouvainGraph>());
  LouvainGraph& cluster_g = *gvec.back();
  CreateClusterGraph(g, &cluster_g);
  cluster_g.DebugPrint();
  cluster_g.Step();
  cluster_g.DebugPrint();

  for (uint32_t pos = 0; pos < gvec.front()->nodes.size(); ++pos) {
    LOG_S(INFO) << "Node " << pos << " is in final cluster "
                << FindFinalCluster(gvec, pos);
  }
}

void TestNodeLineRemover() {
  FUNC_TIMER();
  LouvainGraph g;
  // A line of four nodes ending in a triangle.
  g.AddNodeAndEdges(0, {1});
  g.AddNodeAndEdges(1, {0, 2});
  g.AddNodeAndEdges(2, {1, 3});
  g.AddNodeAndEdges(3, {2, 4, 5});
  g.AddNodeAndEdges(4, {3, 5});
  g.AddNodeAndEdges(5, {4, 3});

  NodeLineRemover::NodeLine nl;
  CHECK_S(!NodeLineRemover::GetLineOfNodes(g, 0, &nl));
  CHECK_S(!NodeLineRemover::GetLineOfNodes(g, 3, &nl));

  CHECK_S(NodeLineRemover::GetLineOfNodes(g, 1, &nl));
  CHECK_S(!nl.connected_circle);
  CHECK_S(!nl.isolated_circle);
  CHECK_EQ_S(nl.vnodes.size(), 4u);
  CHECK_EQ_S(nl.vnodes.at(0), 0u);
  CHECK_EQ_S(nl.vnodes.at(1), 1u);
  CHECK_EQ_S(nl.vnodes.at(2), 2u);
  CHECK_EQ_S(nl.vnodes.at(3), 3u);

  CHECK_S(NodeLineRemover::GetLineOfNodes(g, 2, &nl));
  CHECK_S(!nl.connected_circle);
  CHECK_S(!nl.isolated_circle);
  CHECK_EQ_S(nl.vnodes.size(), 4u);
  CHECK_EQ_S(nl.vnodes.at(0), 0u);
  CHECK_EQ_S(nl.vnodes.at(1), 1u);
  CHECK_EQ_S(nl.vnodes.at(2), 2u);
  CHECK_EQ_S(nl.vnodes.at(3), 3u);

  CHECK_S(NodeLineRemover::GetLineOfNodes(g, 4, &nl));
  CHECK_S(nl.connected_circle);
  CHECK_S(!nl.isolated_circle);
  CHECK_EQ_S(nl.vnodes.size(), 4u);
  CHECK_EQ_S(nl.vnodes.at(0), 3u);
  CHECK_EQ_S(nl.vnodes.at(1), 4u);
  CHECK_EQ_S(nl.vnodes.at(2), 5u);
  CHECK_EQ_S(nl.vnodes.at(3), 3u);

  CHECK_S(NodeLineRemover::GetLineOfNodes(g, 5, &nl));
  CHECK_S(nl.connected_circle);
  CHECK_S(!nl.isolated_circle);
  CHECK_EQ_S(nl.vnodes.size(), 4u);
  CHECK_EQ_S(nl.vnodes.at(0), 3u);
  CHECK_EQ_S(nl.vnodes.at(1), 4u);
  CHECK_EQ_S(nl.vnodes.at(2), 5u);
  CHECK_EQ_S(nl.vnodes.at(3), 3u);
}

void TestNodeLineRemoverDisconnectedCircle() {
  FUNC_TIMER();
  LouvainGraph g;
  // A circle of five nodes.
  g.AddNodeAndEdges(0, {4, 1});
  g.AddNodeAndEdges(1, {0, 2});
  g.AddNodeAndEdges(2, {1, 3});
  g.AddNodeAndEdges(3, {2, 4});
  g.AddNodeAndEdges(4, {3, 0});

  NodeLineRemover::NodeLine nl;
  CHECK_S(NodeLineRemover::GetLineOfNodes(g, 0, &nl));
  CHECK_EQ_S(nl.vnodes.size(), 5u);
  CHECK_S(!nl.connected_circle);
  CHECK_S(nl.isolated_circle);
  CHECK_EQ_S(nl.vnodes.at(0), 0u);
  CHECK_EQ_S(nl.vnodes.at(1), 4u);
  CHECK_EQ_S(nl.vnodes.at(2), 3u);
  CHECK_EQ_S(nl.vnodes.at(3), 2u);
  CHECK_EQ_S(nl.vnodes.at(4), 1u);
}

}  // namespace louvain

int main(int argc, char* argv[]) {
  InitLogging(argc, argv);
  if (argc != 1) {
    ABORT_S() << absl::StrFormat("usage: %s", argv[0]);
  }

  louvain::TestLouvainTriangleGrpah();
  louvain::TestLouvainSquareGraph();
  louvain::TestLouvainSquareClusterGraph();
  louvain::TestNodeLineRemover();
  louvain::TestNodeLineRemoverDisconnectedCircle();

  LOG_S(INFO)
      << "\n\033[1;32m*****************************\nTesting successfully "
         "finished\n*****************************\033[0m";
  return 0;
}
