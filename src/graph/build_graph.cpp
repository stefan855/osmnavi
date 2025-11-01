#include <map>
#include <mutex>

#include "absl/strings/numbers.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include "algos/restricted_access_edges.h"
#include "base/huge_bitset.h"
#include "geometry/distance.h"
#include "geometry/polygon.h"
#include "graph/build_clusters.h"
#include "graph/build_graph.h"
#include "graph/data_block.h"
#include "graph/graph_def.h"
#include "graph/graph_def_utils.h"
#include "graph/node_tags.h"
#include "graph/routing_attrs.h"
#include "graph/routing_config.h"
#include "graph/turn_costs.h"
#include "osm/access.h"
#include "osm/key_bits.h"
#include "osm/maxspeed.h"
#include "osm/oneway.h"
#include "osm/osm_helpers.h"
#include "osm/read_osm_pbf.h"
#include "osm/surface.h"

namespace build_graph {
namespace {
struct ExtractedWayNode {
  int64_t id;
  uint16_t ncc;
};

// Data needed while constructing the way representation of type GWay.
struct WayContext {
  GWay way;
  const OSMPBF::Way& osm_way;
  const ParsedTagInfo pti;
  PerCountryConfig::ConfigValue config_forw;
  PerCountryConfig::ConfigValue config_backw;
};

void ValidateGraph(const Graph& g) {
  FUNC_TIMER();
  CHECK_LT_S(g.ways.size(), 1ull << WAY_IDX_BITS);
  CHECK_LT_S(g.nodes.size(), 1ull << 32);
  CHECK_LT_S(g.edges.size(), 1ull << 32);
  CHECK_LE_S(g.clusters.size(), MAX_CLUSTER_ID);

  // Check that we don't have edges duplicating (from, to, way_idx).
  // Check that nodes are sorted by increasing OSM node_id.
  for (uint32_t node_idx = 0; node_idx < g.nodes.size() - 1; ++node_idx) {
    const GNode& node = g.nodes.at(node_idx);
    if (node_idx > 0) {
      // Nodes are sorted by increasing OSM node_id.
      CHECK_GT_S(node.node_id, g.nodes.at(node_idx - 1).node_id) << node_idx;
    }
    // No dup edges.
    const uint32_t num_all_edges = gnode_num_all_edges(g, node_idx);
    for (uint32_t off1 = 0; off1 < num_all_edges; ++off1) {
      const GEdge& e1 = g.edges.at(node.edges_start_pos + off1);
      for (uint32_t off2 = off1 + 1; off2 < num_all_edges; ++off2) {
        const GEdge& e2 = g.edges.at(node.edges_start_pos + off2);
        if (e1.target_idx == e2.target_idx && e1.way_idx == e2.way_idx) {
          LOG_S(INFO) << absl::StrFormat(
              "Duplicate edge %lld to %lld way_id:%lld", node.node_id,
              GetGNodeIdSafe(g, e1.target_idx), GetGWayIdSafe(g, e1.way_idx));
          for (const GEdge& e : gnode_forward_edges(g, node_idx)) {
            LOG_S(INFO) << absl::StrFormat(
                "edge %lld to %lld way_id:%lld", node.node_id,
                GetGNodeIdSafe(g, e.target_idx), GetGWayIdSafe(g, e.way_idx));
          }
          ABORT_S();
        }
      }
    }
  }
}
}  // namespace

void ConsumeWayStoreSeenNodesWorker(const OSMTagHelper& tagh,
                                    const OSMPBF::Way& osm_way, std::mutex& mut,
                                    HugeBitset* node_ids,
                                    BuildGraphStats* stats) {
  if (HighwayLabelToEnum(tagh.GetValue(osm_way, "highway")) == HW_MAX) {
    return;
  }

  stats->num_ways_with_highway_tag++;
  const size_t ref_size = osm_way.refs().size();
  if (ref_size > 0) {
    stats->num_edges_with_highway_tag += (osm_way.refs().size() - 1);
    stats->num_noderefs_with_highway_tag += osm_way.refs().size();
  }

  {
    std::unique_lock<std::mutex> l(mut);
    std::int64_t running_id = 0;
    for (int ref_idx = 0; ref_idx < osm_way.refs().size(); ++ref_idx) {
      running_id += osm_way.refs(ref_idx);
      node_ids->SetBit(running_id, true);
    }
  }
}

void ConsumeNodeBlob(VEHICLE vt, const OSMTagHelper& tagh,
                     const OSMPBF::PrimitiveBlock& prim_block, std::mutex& mut,
                     const HugeBitset& touched_nodes_ids,
                     DataBlockTable* node_table, GraphMetaData* meta) {
  NodeBuilder builder;

  for (const OSMPBF::PrimitiveGroup& pg : prim_block.primitivegroup()) {
    const auto& keys_vals = pg.dense().keys_vals();
    // NodeBuilder::VNode node = {.id = 0, .lat = 0, .lon = 0};
    OsmPbfReader::NodeWithTags node;

    // kv_start points to terminating 0-element of previous node. Before the
    // loop it is therefore on position -1.
    int kv_start = -1;
    for (int i = 0; i < pg.dense().id_size(); ++i) {
      node.id_ += pg.dense().id(i);
      node.lat_ += pg.dense().lat(i);
      node.lon_ += pg.dense().lon(i);
      const int kv_stop =
          OsmPbfReader::NodeWithTags::AdvanceKVWindow(keys_vals, &kv_start);

      if (touched_nodes_ids.GetBit(node.id_)) {
        if (kv_start < kv_stop) {
          node.AddKeyVals(pg.dense().keys_vals(), kv_start, kv_stop);
          const ParsedTagInfo pti = ParseTags(tagh, node);
          NodeTags node_tags = ParseOSMNodeTags(vt, pti, node.id_);
          if (!node_tags.empty()) {
            // Store node_tags for this node.
            std::unique_lock<std::mutex> l(mut);
            meta->graph.node_tags_sorted.push_back(node_tags);
          }
          node.keys_.clear();
          node.vals_.clear();
        }

        CHECK_GT_S(node.id_, 0);
        builder.AddNode(
            {.id = (uint64_t)node.id_, .lat = node.lat_, .lon = node.lon_});
        if (builder.pending_nodes() >= 128) {
          std::unique_lock<std::mutex> l(mut);
          builder.AddBlockToTable(node_table);
        }
      }
      kv_start = kv_stop;
    }
  }
  if (builder.pending_nodes() > 0) {
    std::unique_lock<std::mutex> l(mut);
    builder.AddBlockToTable(node_table);
  }
}

WayTaggedZones ExtractWayZones(const OSMTagHelper& tagh,
                               const std::vector<ParsedTag>& ptags) {
  constexpr KeySet selector_bits = KeySet(
      {KEY_BIT_MAXSPEED, KEY_BIT_ZONE, KEY_BIT_TRAFFIC, KEY_BIT_MOTORROAD});
  constexpr KeySet modifier_bits =
      KeySet({KEY_BIT_FORWARD, KEY_BIT_BACKWARD, KEY_BIT_BOTH_WAYS});

  WayTaggedZones info;
  for (const ParsedTag& pt : ptags) {
    if ((pt.bits & selector_bits).none()) {
      continue;
    }

    const KeySet ks = pt.bits & ~modifier_bits;
    if (ks == KeySet({KEY_BIT_MAXSPEED}) ||
        ks == KeySet({KEY_BIT_MAXSPEED, KEY_BIT_SOURCE}) ||
        ks == KeySet({KEY_BIT_MAXSPEED, KEY_BIT_TYPE}) ||
        ks == KeySet({KEY_BIT_MAXSPEED, KEY_BIT_ZONE}) ||
        ks == KeySet({KEY_BIT_ZONE, KEY_BIT_TRAFFIC})) {
      uint16_t ncc = INVALID_NCC;
      ENVIRONMENT_TYPE et = ET_ANY;
      std::string_view val = tagh.ToString(pt.val_st_idx);
      if (ParseCountrySpeedParts(val, &ncc, &et)) {
        if (et == ET_RURAL || et == ET_URBAN) {
          if (pt.bits.test(KEY_BIT_FORWARD)) {
            info.et_forw = et;
          } else if (pt.bits.test(KEY_BIT_BACKWARD)) {
            info.et_backw = et;
          } else {
            info.et_forw = et;
            info.et_backw = et;
          }
        }
        if (ncc != INVALID_NCC) {
          info.ncc = ncc;
        }
      }
    }

    if (ks == KeySet({KEY_BIT_MOTORROAD})) {
      IS_MOTORROAD im = tagh.ToString(pt.val_st_idx) == "yes" ? IM_YES : IM_NO;
      if (pt.bits.test(KEY_BIT_FORWARD)) {
        info.im_forw = im;
      } else if (pt.bits.test(KEY_BIT_BACKWARD)) {
        info.im_backw = im;
      } else {
        info.im_forw = im;
        info.im_backw = im;
      }
    }
  }
  return info;
}

namespace {
// String that contains the tags relevant for routing in readable form. This
// is used to log tag-signatures and frequencies, for instance to find
// interesting cases of usage of tags.
std::string CreateWayTagSignature(const OSMTagHelper& tagh,
                                  const OSMPBF::Way& osm_way) {
  std::vector<std::string> tagvals;
  for (int pos = 0; pos < osm_way.keys().size(); ++pos) {
    std::string_view key = tagh.ToString(osm_way.keys().at(pos));
    std::string_view val = tagh.ToString(osm_way.vals().at(pos));
    if (absl::StrContains(key, "lane") || absl::StrContains(key, "zone") ||
        absl::StrContains(key, "motorroad") || absl::StrContains(key, "car") ||
        absl::StrContains(key, "oneway") ||
        absl::StrContains(key, "junction") ||
        absl::StrContains(key, "tracktype") ||
        absl::StrContains(key, "carriageway") /* "Fahrbahn" ??*/ ||
        absl::StrContains(key, "maxspeed") || absl::StrContains(key, "cycle") ||
        absl::StrContains(key, "overtaking") ||  // yes, no, caution
        absl::StrContains(key, "sidewalk") || absl::StrContains(key, "turn") ||
        absl::StrContains(key, "smoothness") ||
        absl::StrContains(key, "vehicle") || absl::StrContains(key, "access") ||
        absl::StrContains(key, "surface") || absl::StrContains(key, "busway") ||
        val == "lane" || val == "psv" || val == "bus" || val == "taxi" ||
        absl::StrContains(val, "cycle") || absl::StrContains(val, "vehicle") ||
        absl::StrContains(key, "psv") || absl::StrContains(key, "taxi") ||
        absl::StrContains(key, "horse") || absl::StrContains(key, "forward") ||
        absl::StrContains(key, "backward") || key == "highway") {
      if (!absl::StrContains(key, "destination") &&
          !absl::StrContains(key, "cs_dir:") &&
          !absl::StrContains(key, "fixme") &&
          !absl::StrContains(key, "placement") &&
          !absl::StrContains(key, "description") &&
          !absl::StrContains(key, ":note") &&
          !absl::StrContains(key, "note:") &&
          !absl::StrContains(key, "check_date")) {
        tagvals.push_back(
            absl::StrCat(key, "=", tagh.ToString(osm_way.vals().at(pos))));
      }
    }
  }
  std::string res;
  if (tagvals.size() > 0) {
    std::sort(tagvals.begin(), tagvals.end());
    for (const std::string& s : tagvals) {
      absl::StrAppend(&res, s, " ## ");
    }
  }
  return res;
}

// Given a way, return a list of all way nodes with their countries.
std::vector<ExtractedWayNode> ExtractWayNodes(const GraphMetaData& meta,
                                              const OSMPBF::Way& osm_way,
                                              bool* missing_nodes,
                                              uint32_t* dup_segments) {
  *missing_nodes = false;
  std::vector<ExtractedWayNode> way_nodes;
  std::int64_t running_id = 0;
  for (int ref_idx = 0; ref_idx < osm_way.refs().size(); ++ref_idx) {
    running_id += osm_way.refs(ref_idx);
    //
    // Check for repeated nodes/segments. If found, remove them.
    if (way_nodes.size() > 0 && way_nodes.back().id == running_id) {
      // Repeated node. This is an error that causes headaches, for instance
      // it create edges of length 0.
      (*dup_segments) += 1;
      continue;
    }

    if (way_nodes.size() > 1) {
      bool dup_segment = false;
      for (uint32_t i = 0; i < way_nodes.size() - 1; ++i) {
        if (way_nodes.at(i).id == running_id) {
          // The current node was seen before in the same way.
          // Check if adding running_id would create a duplicate segment.
          // The segment that is added is
          //     way_nodes.back().id ----> running_id.
          if (way_nodes.at(i + 1).id == way_nodes.back().id) {
            dup_segment = true;
            break;
          }
          if (i > 0 && way_nodes.at(i - 1).id == way_nodes.back().id) {
            dup_segment = true;
            break;
          }
        }
      }
      if (dup_segment) {
        (*dup_segments) += 1;
        continue;
      }
    }

    NodeBuilder::VNode node;
    if (!NodeBuilder::FindNode(*meta.node_table, running_id, &node)) {
      // Node is referenced by osm_way, but the node was not loaded.
      // This happens often when a clipped country file does contain a way but
      // not all the nodes of the way.
      // LOG_S(INFO) << "Way " << osm_way.id() << " references non-existing"
      //                " node " << running_id;
      *missing_nodes = true;
      continue;
    }
    uint16_t ncc = meta.tiler->GetCountryNum(node.lon, node.lat);
    way_nodes.push_back({.id = running_id, .ncc = ncc});
  }
  if (*missing_nodes) {
    LOG_S(INFO) << "Way " << osm_way.id() << " has missing node(s) -- country:"
                << (way_nodes.empty()
                        ? "<empty>"
                        : CountryNumToString(way_nodes.front().ncc));
    return {};
  }
  return way_nodes;
}

// Extract the country of a way from "way_nodes" and store it in
// way.ncc and way.uniform_country. Note: way.ncc contains the
// country of the first way node and is always set (unless the first way node
// has no country).
void SetWayCountryCode(const std::vector<ExtractedWayNode>& way_nodes,
                       GWay* way) {
  CHECK_GT_S(way_nodes.size(), 1u);
  way->ncc = way_nodes.front().ncc;
  way->uniform_country = 1;
  for (size_t i = 1; i < way_nodes.size(); ++i) {
    if (way_nodes.at(i).ncc != way->ncc) {
      way->uniform_country = 0;
      /*
      if (way_nodes.at(i).ncc < way->ncc &&
          way_nodes.at(i).ncc != INVALID_NCC) {
        way->ncc = way_nodes.front().ncc;
      }
      */
      return;
    }
  }
}

// Should only be called when mutex-locked.
// Marks all nodes referenced by a way as "seen". If a node is already "seen"
// then it marks it also as "needed", because this means that the point is an
// intersection of two ways.
// Also marks the nodes of edges connecting different countries as "needed".
// This keeps edge distances short when we are not sure about speed limits,
// access rules etc.
void MarkSeenAndNeeded(GraphMetaData* meta,
                       const std::vector<ExtractedWayNode>& ncs,
                       const OSMPBF::Way& osm_way,
                       build_graph::BuildGraphStats* stats) {
  if (meta->opt.keep_all_nodes) {
    for (const ExtractedWayNode& nc : ncs) {
      meta->way_nodes_seen->SetBit(nc.id, true);
      meta->way_nodes_needed->SetBit(nc.id, true);
    }
    return;
  }

  // Mark nodes at country-crossing edges as 'needed' .
  for (size_t i = 0; i < ncs.size() - 1; ++i) {
    if (ncs.at(i).ncc != ncs.at(i + 1).ncc) {
      meta->way_nodes_needed->SetBit(ncs.at(i).id, true);
      meta->way_nodes_needed->SetBit(ncs.at(i + 1).id, true);
    }
  }

  for (const ExtractedWayNode& nc : ncs) {
    if (meta->way_nodes_seen->GetBit(nc.id)) {
      // Node was seen >= 2 times, Mark as 'needed'.
      meta->way_nodes_needed->SetBit(nc.id, true);
    } else {
      // Not seen before, so mark as 'seen'.
      meta->way_nodes_seen->SetBit(nc.id, true);
    }
  }
  // Start and end nodes are both 'needed'.
  meta->way_nodes_needed->SetBit(ncs.front().id, true);
  meta->way_nodes_needed->SetBit(ncs.back().id, true);
  if (ncs.front().id == ncs.back().id) {
    stats->num_ways_closed++;
  }

  // Find loops in the way nodes and mark all nodes in the loop as needed.
  for (size_t i = 0; i < ncs.size() - 1; ++i) {
    const ExtractedWayNode& nc1 = ncs.at(i);
    for (size_t j = i + 1; j < ncs.size(); ++j) {
      if (ncs.at(j).id == nc1.id) {
        // [i..j] is a loop.
        if (j - i <= 3) {
          LOG_S(INFO) << absl::StrFormat(
              "LOOP of length %llu in way %lld node %lld", j - i, osm_way.id(),
              nc1.id);
        }
        for (size_t k = i; k < j; ++k) {
          meta->way_nodes_needed->SetBit(ncs.at(k).id, true);
        }
      }
    }
  }
}

std::uint16_t LimitedMaxspeed(const PerCountryConfig::ConfigValue& cv,
                              std::uint16_t extracted_maxspeed) {
  std::uint16_t m = cv.dflt.maxspeed;
  if (extracted_maxspeed > 0) {
    m = extracted_maxspeed;
  }
  if (cv.speed_limit > 0 && cv.speed_limit < m) {
    m = cv.speed_limit;
  }
  return m;
}

}  // namespace

inline void ComputeCarWayRoutingData(const OSMTagHelper& tagh,
                                     const WayContext& wc,
                                     WaySharedAttrs* wsa) {
  RoutingAttrs ra_forw = wc.config_forw.dflt;
  RoutingAttrs ra_backw = wc.config_backw.dflt;

  const DIRECTION direction =
      CarRoadDirection(tagh, wc.way.highway_label, wc.way.id, wc.pti.tags());
  if (direction == DIR_MAX) {
    return;  // For instance wrong vehicle type or some tagging error.
  }
  ra_forw.dir = IsDirForward(direction);
  ra_backw.dir = IsDirBackward(direction);
  CarRoadSurface(tagh, wc.way.id, wc.pti.tags(), &ra_forw, &ra_backw);

  {
    // Set access.
    const AccessPerDirection apd =
        CarAccess(tagh, wc.way.id, wc.pti.tags(),
                  {.acc_forw = ra_forw.access, .acc_backw = ra_backw.access});
    ra_forw.access = apd.acc_forw;
    ra_backw.access = apd.acc_backw;
  }

  bool acc_forw = RoutableAccess(ra_forw.access);
  bool acc_backw = RoutableAccess(ra_backw.access);
  if ((!IsDirForward(direction) || !acc_forw) &&
      (!IsDirBackward(direction) || !acc_backw)) {
    if (acc_forw || acc_backw) {
      LOG_S(INFO) << absl::StrFormat(
          "way %lld has no valid direction for cars\n%s", wc.way.id,
          tagh.GetLoggingStr(wc.osm_way));
    }
    return;  // For instance when "private" or "no".
  }

  // Set maxspeed.
  std::uint16_t maxspeed_forw;
  std::uint16_t maxspeed_backw;
  CarMaxspeedFromWay(tagh, wc.way.id, wc.pti.tags(), &maxspeed_forw,
                     &maxspeed_backw);
  ra_forw.maxspeed = LimitedMaxspeed(wc.config_forw, maxspeed_forw);
  ra_backw.maxspeed = LimitedMaxspeed(wc.config_backw, maxspeed_backw);

  // Store final routing attributes.
  if (acc_forw && IsDirForward(direction) && ra_forw.maxspeed > 0) {
    wsa->ra[RAinWSAIndex(VH_MOTORCAR, DIR_FORWARD)] = ra_forw;
  }

  if (acc_backw && IsDirBackward(direction) && ra_backw.maxspeed > 0) {
    wsa->ra[RAinWSAIndex(VH_MOTORCAR, DIR_BACKWARD)] = ra_backw;
  }
}

inline void ComputeBicycleWayRoutingData(const OSMTagHelper& tagh,
                                         const WayContext& wc,
                                         WaySharedAttrs* wsa) {
  RoutingAttrs ra_forw = wc.config_forw.dflt;
  RoutingAttrs ra_backw = wc.config_backw.dflt;

  const DIRECTION direction = BicycleRoadDirection(tagh, wc.way.highway_label,
                                                   wc.way.id, wc.pti.tags());
  if (direction == DIR_MAX) {
    return;  // For instance wrong vehicle type or some tagging error.
  }
  ra_forw.dir = IsDirForward(direction);
  ra_backw.dir = IsDirBackward(direction);

  {
    // Set access.
    const AccessPerDirection apd = BicycleAccess(
        tagh, wc.way.id, wc.pti.tags(),
        {.acc_forw = ra_forw.access, .acc_backw = ra_backw.access});
    ra_forw.access = apd.acc_forw;
    ra_backw.access = apd.acc_backw;
  }

  bool acc_forw = RoutableAccess(ra_forw.access);
  bool acc_backw = RoutableAccess(ra_backw.access);
  if ((!IsDirForward(direction) || !acc_forw) &&
      (!IsDirBackward(direction) || !acc_backw)) {
    if (acc_forw || acc_backw) {
      LOG_S(INFO) << absl::StrFormat(
          "way %lld has no valid direction for bicycles\n%s", wc.way.id,
          tagh.GetLoggingStr(wc.osm_way));
    }
    return;  // For instance when "private" or "no".
  }

  // Set maxspeed.
  std::uint16_t maxspeed_forw;
  std::uint16_t maxspeed_backw;
  BicycleMaxspeedFromWay(tagh, wc.way.id, wc.pti.tags(), &maxspeed_forw,
                         &maxspeed_backw);
  ra_forw.maxspeed = LimitedMaxspeed(wc.config_forw, maxspeed_forw);
  ra_backw.maxspeed = LimitedMaxspeed(wc.config_backw, maxspeed_backw);

  // Store final routing attributes.
  if (acc_forw && IsDirForward(direction) && ra_forw.maxspeed > 0) {
    wsa->ra[RAinWSAIndex(VH_BICYCLE, DIR_FORWARD)] = ra_forw;
  }

  if (acc_backw && IsDirBackward(direction) && ra_backw.maxspeed > 0) {
    wsa->ra[RAinWSAIndex(VH_BICYCLE, DIR_BACKWARD)] = ra_backw;
  }
}

namespace {
void MayStoreWayTagStats(const OSMTagHelper& tagh, const OSMPBF::Way& osm_way,
                         build_graph::BuildGraphStats* stats) {
  if (!tagh.GetValue(osm_way, "highway").empty()) {
    std::string statkey = CreateWayTagSignature(tagh, osm_way);
    if (!statkey.empty()) {
      stats->way_tag_stats.Add(statkey, osm_way.id());
    }
  }
}

void LogCountryConflict(const WayTaggedZones& rural, const GWay way) {
  if (rural.ncc != INVALID_NCC && way.uniform_country && rural.ncc != way.ncc) {
    LOG_S(INFO) << absl::StrFormat(
        "Defined country different from tagged country %d:%d %s:%s in "
        "way:%lld",
        rural.ncc, way.ncc, CountryNumToString(rural.ncc),
        CountryNumToString(way.ncc), way.id);
  }
}

inline void EncodeNodeIds(const std::vector<ExtractedWayNode>& way_nodes,
                          WriteBuff* node_ids_buff) {
  std::vector<uint64_t> node_ids;
  for (const ExtractedWayNode& nc : way_nodes) {
    node_ids.push_back(nc.id);
  }
  EncodeUInt(node_ids.size(), node_ids_buff);
  EncodeNodeIds(node_ids, node_ids_buff);
}

// Get the number of lanes that are declared in the tags.
// Returns 0 if no tags were found.
int GetNumLanes(const OSMTagHelper& tagh, const std::vector<ParsedTag>& ptags) {
  long num_lanes = 0;
  long sum_lanes_with_dir = 0;
  for (const ParsedTag& pt : ptags) {
    if (pt.bits == KeySet({KEY_BIT_LANES})) {
      double val;
      if (absl::SimpleAtod(tagh.ToString(pt.val_st_idx), &val)) {
        num_lanes = std::roundl(val);
      }
    } else if (pt.bits == KeySet({KEY_BIT_LANES, KEY_BIT_FORWARD}) ||
               pt.bits == KeySet({KEY_BIT_LANES, KEY_BIT_BACKWARD})) {
      double val;
      if (absl::SimpleAtod(tagh.ToString(pt.val_st_idx), &val)) {
        sum_lanes_with_dir += std::roundl(val);
      }
    }
  }
  if (num_lanes > 0) {
    return num_lanes;
  } else {
    return sum_lanes_with_dir;
  }
}

std::pair<bool, bool> GetPriorityRoadSetting(
    const OSMTagHelper& tagh, const std::vector<ParsedTag>& ptags) {
  bool p_forward = false;
  bool p_backward = false;
  // static const std::string_view yes_prio[] = {"designated", "yes",
  //                                             "yes_unposted"};

  for (const ParsedTag& pt : ptags) {
    if (pt.bits.test(KEY_BIT_PRIORITY_ROAD)) {
      bool prio = StrSpanContains(tagh.ToString(pt.val_st_idx),
                                  {"designated", "yes", "yes_unposted"});
      if (pt.bits == KeySet({KEY_BIT_PRIORITY_ROAD})) {
        p_forward = p_backward = prio;
      } else if (pt.bits == KeySet({KEY_BIT_PRIORITY_ROAD, KEY_BIT_FORWARD})) {
        p_forward = prio;
      } else if (pt.bits == KeySet({KEY_BIT_PRIORITY_ROAD, KEY_BIT_BACKWARD})) {
        p_backward = prio;
      }
    }
  }

  return {p_forward, p_backward};
}
}  // namespace

// Check if osm_way is part of the routable network (routable by car etc.) and
// add a record to graph.ways. Also updates 'seen nodes' and 'needed nodes'
// bitsets.
void ConsumeWayWorker(const OSMTagHelper& tagh, const OSMPBF::Way& osm_way,
                      std::mutex& mut, DeDuperWithIds<WaySharedAttrs>* deduper,
                      GraphMetaData* meta, BuildGraphStats* stats) {
  if (osm_way.refs().size() <= 1) {
    LOG_S(INFO) << absl::StrFormat("Ignore way %lld of length %lld",
                                   osm_way.id(), osm_way.refs().size());
    stats->num_ways_too_short++;
    return;
  }

  if (meta->opt.log_way_tag_stats) {
    MayStoreWayTagStats(tagh, osm_way, stats);
  }

  const HIGHWAY_LABEL highway_label =
      HighwayLabelToEnum(tagh.GetValue(osm_way, "highway"));

  if (highway_label == HW_MAX) {
    return;  // Not interesting for routing, ignore.
  }

  WayContext wc = {.osm_way = osm_way, .pti = ParseTags(tagh, osm_way)};
  wc.way.id = osm_way.id();
  wc.way.highway_label = highway_label;
  wc.way.area = tagh.GetValue(osm_way, "area") == "yes";
  wc.way.roundabout = tagh.GetValue(osm_way, "junction") == "roundabout";
  wc.way.has_ref = !tagh.GetValue(osm_way, "ref").empty();
  const std::pair<bool, bool> prios =
      GetPriorityRoadSetting(tagh, wc.pti.tags());
  wc.way.priority_road_forward = prios.first;
  wc.way.priority_road_backward = prios.second;
  wc.way.more_than_two_lanes = GetNumLanes(tagh, wc.pti.tags()) > 2;

  CHECK_LT_S(wc.way.highway_label, HW_MAX);

  bool missing_nodes = false;
  uint32_t dup_segments = 0;
  const std::vector<ExtractedWayNode> way_nodes =
      ExtractWayNodes(*meta, osm_way, &missing_nodes, &dup_segments);
  if (missing_nodes || way_nodes.size() <= 1) {
    stats->num_ways_missing_nodes++;
    return;
  }
  // Sets way.ncc and way.uniform_country.
  SetWayCountryCode(way_nodes, &wc.way);
  wc.way.closed_way = (way_nodes.front().id == way_nodes.back().id);
  const WayTaggedZones rural = ExtractWayZones(tagh, wc.pti.tags());
  LogCountryConflict(rural, wc.way);

  WaySharedAttrs wsa;
  // Set all numbers to 0, enums to first value.
  memset(&wsa.ra, 0, sizeof(wsa.ra));

  // TODO: Use real country instead of always using CH (Switzerland).
  wc.config_forw = meta->per_country_config->GetDefault(
      /*wc.way.ncc*/ NCC_CH, wc.way.highway_label, meta->opt.vt, rural.et_forw,
      rural.im_forw);
  wc.config_backw = meta->per_country_config->GetDefault(
      /*wc.way.ncc*/ NCC_CH, wc.way.highway_label, meta->opt.vt, rural.et_backw,
      rural.im_backw);

  if (meta->opt.vt == VH_MOTORCAR) {
    ComputeCarWayRoutingData(tagh, wc, &wsa);
  } else if (meta->opt.vt == VH_BICYCLE) {
    ABORT_S() << "Unsupported vehicle type " << meta->opt.vt;
    // TODO
    // ComputeBicycleWayRoutingData(tagh, wc, &wsa);
  } else if (meta->opt.vt == VH_FOOT) {
    ABORT_S() << "Unsupported vehicle type " << meta->opt.vt;
    // TODO
  } else {
    ABORT_S() << "Invalid vehicle type " << meta->opt.vt;
  }

  if (!WSAVehicleAnyRoutable(wsa, meta->opt.vt)) return;

  WriteBuff node_ids_buff;
  EncodeNodeIds(way_nodes, &node_ids_buff);
  stats->num_ways_dup_segments += dup_segments;

  // Run modifications of global data behind a mutex.
  {
    std::unique_lock<std::mutex> l(mut);
    wc.way.node_ids =
        meta->graph.unaligned_pool_.AllocBytes(node_ids_buff.used());
    memcpy(wc.way.node_ids, node_ids_buff.base_ptr(), node_ids_buff.used());

    std::string_view streetname = tagh.GetValue(wc.osm_way, "name");
    if (!streetname.empty()) {
      // allocate a 0-terminated char*.
      wc.way.streetname =
          (char*)meta->graph.unaligned_pool_.AllocBytes(streetname.size() + 1);
      memcpy(wc.way.streetname, streetname.data(), streetname.size());
      wc.way.streetname[streetname.size()] = '\0';
    }

    MarkSeenAndNeeded(meta, way_nodes, osm_way, stats);

    wc.way.wsa_id = deduper->Add(wsa);
    meta->graph.ways.push_back(wc.way);
  }
}

namespace {
void AddEdge(Graph& g, const size_t start_idx, const size_t other_idx,
             const bool inverted, const bool contra_way,
             const bool both_directions, const size_t way_idx,
             const std::uint64_t distance_cm, bool car_restricted) {
  GNode& n = g.nodes.at(start_idx);
  const GNode& other = g.nodes.at(other_idx);
  CHECK_LE_S(distance_cm, MAX_EDGE_DISTANCE_CM)
      << absl::StrFormat("Node %lld->%lld way %lld", n.node_id, other.node_id,
                         GetGWayIdSafe(g, way_idx));
  const int64_t edge_start = n.edges_start_pos;
  const int64_t edges_stop = gnode_edges_stop(g, start_idx);
  int64_t ep;
  if (inverted) {
    for (ep = edges_stop - 1; ep >= edge_start; --ep) {
      if (g.edges.at(ep).target_idx == INFU32) break;
    }
  } else {
    for (ep = edge_start; ep < edges_stop; ++ep) {
      if (g.edges.at(ep).target_idx == INFU32) break;
    }
    CHECK_LT_S(n.num_forward_edges, MAX_NUM_EDGES_OUT) << n.node_id;
    n.num_forward_edges++;
  }
  CHECK_S(ep >= edge_start && ep < edges_stop);
  CHECK_S(other_idx != INFU32);
  GEdge& e = g.edges.at(ep);
  e.target_idx = other_idx;
  e.way_idx = way_idx;
  e.distance_cm = distance_cm;
  e.unique_target = 0;
  // e.bridge = 0;
  e.to_bridge = 0;
  e.contra_way = contra_way ? 1 : 0;
  e.cross_country = n.ncc != other.ncc;
  e.inverted = inverted ? 1 : 0;
  e.both_directions = both_directions ? 1 : 0;
  e.car_label = car_restricted ? GEdge::LABEL_RESTRICTED : GEdge::LABEL_UNSET;
  e.car_label_strange = 0;
  e.car_uturn_allowed = 0;
  e.complex_turn_restriction_trigger = 0;
  e.stop_sign = 0;
  e.traffic_signal = 0;
  e.road_priority = GEdge::PRIO_UNSET;
  e.type = GEdge::TYPE_UNKNOWN;
  // e.cluster_border_edge = 0;
  e.turn_cost_idx = INVALID_TURN_COST_IDX;

  const GWay& way = g.ways.at(way_idx);
  if ((way.priority_road_forward && !contra_way) ||
      (way.priority_road_backward && contra_way)) {
    e.road_priority = GEdge::PRIO_HIGH;
  }
}

void MarkUniqueOther(std::span<GEdge> edges) {
  for (size_t i = 0; i < edges.size(); ++i) {
    size_t k = 0;
    while (k < i) {
      // TODO: C++26 allows .at() with bounds checking.
      if (edges[i].target_idx == edges[k].target_idx) {
        break;
      }
      k++;
    }
    edges[i].unique_target = (i == k);
  }
}

void FindLargeComponents(Graph* g) {
  ComponentAnalyzer a(*g);
  g->large_components = a.FindLargeComponents();
  ComponentAnalyzer::MarkLargeComponents(g);
}

void ConsumeRelation(const OSMTagHelper& tagh, const OSMPBF::Relation& osm_rel,
                     GraphMetaData* meta, TRResult* result) {
  ParseTurnRestriction(meta->graph, tagh, osm_rel,
                       meta->opt.verb_turn_restrictions, result);
}

// Read the ways that might useful for routing, remember the nodes ids touched
// by these ways, then read the node coordinates and store them in
// 'node_table'.
void LoadNodeCoordsAndAttrributes(VEHICLE vt, OsmPbfReader* reader,
                                  DataBlockTable* node_table,
                                  GraphMetaData* meta) {
  FUNC_TIMER();
  HugeBitset touched_nodes_ids;
  // Read ways and remember the touched nodes in 'touched_nodes_ids'.
  reader->ReadWays([&touched_nodes_ids, meta](const OSMTagHelper& tagh,
                                              const OSMPBF::Way& way,
                                              int thread_idx, std::mutex& mut) {
    ConsumeWayStoreSeenNodesWorker(tagh, way, mut, &touched_nodes_ids,
                                   &meta->Stats(thread_idx));
  });

  // Read all the node coordinates for nodes in 'touched_nodes_ids'.
  reader->ReadBlobs(
      OsmPbfReader::ContentNodes,
      [vt, &touched_nodes_ids, node_table, meta](
          const OSMTagHelper& tagh, const OSMPBF::PrimitiveBlock& prim_block,
          int thread_idx, std::mutex& mut) {
        ConsumeNodeBlob(vt, tagh, prim_block, mut, touched_nodes_ids,
                        node_table, meta);
      });
  // Make node table searchable, so we can look up lat/lon by node_id.
  node_table->Sort();
  std::sort(meta->graph.node_tags_sorted.begin(),
            meta->graph.node_tags_sorted.end(),
            [](const auto& a, const auto& b) { return a.node_id < b.node_id; });
}

void LoadGWays(OsmPbfReader* reader, GraphMetaData* meta) {
  FUNC_TIMER();

  DeDuperWithIds<WaySharedAttrs> deduper;
  reader->ReadWays([&deduper, meta](const OSMTagHelper& tagh,
                                    const OSMPBF::Way& way, int thread_idx,
                                    std::mutex& mut) {
    ConsumeWayWorker(tagh, way, mut, &deduper, meta, &meta->Stats(thread_idx));
  });

  // Sort and build the vector with shared way attributes.
  deduper.SortByPopularity();
  meta->graph.way_shared_attrs = deduper.GetObjVector();
  const std::vector<uint32_t> mapping = deduper.GetSortMapping();
  for (GWay& w : meta->graph.ways) {
    w.wsa_id = mapping.at(w.wsa_id);
  }
  LOG_S(INFO) << absl::StrFormat(
      "Shared way attributes de-duping %u -> %u (%.2f%%)", deduper.num_added(),
      deduper.num_unique(),
      (100.0 * deduper.num_unique()) / std::max(1u, deduper.num_added()));
}

void SortGWays(GraphMetaData* meta) {
  FUNC_TIMER();
  // Sort by ascending way_id.
  std::sort(meta->graph.ways.begin(), meta->graph.ways.end(),
            [](const GWay& a, const GWay& b) { return a.id < b.id; });
}

void MarkNodesWithAttributesAsNeeded(GraphMetaData* meta) {
  FUNC_TIMER();
  for (const NodeTags& nt : meta->graph.node_tags_sorted) {
    if (meta->way_nodes_seen->GetBit(nt.node_id)) {
      meta->way_nodes_needed->SetBit(nt.node_id, true);
    }
  }
}

void AllocateGNodes(GraphMetaData* meta) {
  FUNC_TIMER();
  meta->graph.nodes.reserve(meta->way_nodes_needed->CountBits());
  NodeBuilder::GlobalNodeIter iter(*meta->node_table);
  const NodeBuilder::VNode* node;
  while ((node = iter.Next()) != nullptr) {
    if (meta->way_nodes_needed->GetBit(node->id)) {
      GNode n;
      n.node_id = node->id;
      n.large_component = 0;
      n.cluster_id = INVALID_CLUSTER_ID;
      n.cluster_border_node = 0;
      n.edges_start_pos = 0;
      n.num_forward_edges = 0;
      n.dead_end = 0;
      n.ncc = INVALID_NCC;
      n.simple_turn_restriction_via_node = 0;
      n.is_pedestrian_crossing = 0;
      n.lat = node->lat;
      n.lon = node->lon;
      meta->graph.nodes.push_back(n);
    }
  }
}

void SetCountryInGNodes(GraphMetaData* meta) {
  FUNC_TIMER();
  // TODO: run with thread pool.
  for (GNode& n : meta->graph.nodes) {
    n.ncc = meta->tiler->GetCountryNum(n.lon, n.lat);
  }
}

// Compute for each node how many edges it has. The temporary edge count is
// stored in n.edges_start_pos.
void ComputeEdgeCountsWorker(size_t start_pos, size_t stop_pos,
                             GraphMetaData* meta, std::mutex& mut) {
  Graph& graph = meta->graph;

  for (size_t way_idx = start_pos; way_idx < stop_pos; ++way_idx) {
    const GWay& way = graph.ways.at(way_idx);
    const WaySharedAttrs& wsa = GetWSA(graph, way);

    std::vector<uint32_t> node_idx =
        meta->graph.GetGWayNodeIndexes(*(meta->way_nodes_needed), way);
    {
      // Update edge counts.
      std::unique_lock<std::mutex> l(mut);
      int64_t prev_idx = -1;
      for (const size_t idx : node_idx) {
        if (prev_idx >= 0) {
          // All edges should be routable for at least one vehicle type.
          CHECK_S(WSAVehicleAnyRoutable(wsa, meta->opt.vt)) << way.id;

          // We "abuse" edges_start_pos as edge-counter while building the
          // graph.
          graph.nodes.at(prev_idx).edges_start_pos++;
          graph.nodes.at(idx).edges_start_pos++;

          // TODO: self edges?
          // CHECK_NE_S(idx1, idx2) << way.id;
        }
        prev_idx = idx;
      }
    }
  };
}

void ComputeEdgeCounts(GraphMetaData* meta) {
  FUNC_TIMER();
  std::mutex mut;
  ThreadPool pool;
  ArrayChunker<int> chunker(meta->graph.ways.size(), 1024 * 16, -1);
  for (const auto& chunk : chunker.chunks) {
    pool.AddWork([meta, &mut, &chunk](int thread_idx) {
      ComputeEdgeCountsWorker(chunk.start, chunk.stop, meta, mut);
    });
  }
  pool.Start(meta->opt.n_threads);
  pool.WaitAllFinished();
}

void AllocateEdgeArrays(GraphMetaData* meta) {
  FUNC_TIMER();
  size_t edge_start = 0;
  for (GNode& n : meta->graph.nodes) {
    size_t num_edges = n.edges_start_pos;  // Read temporary counter.
    n.edges_start_pos = edge_start;
    edge_start += num_edges;
  }
  CHECK_S(meta->graph.edges.empty());

  meta->graph.edges.reserve(edge_start);
  meta->graph.edges.resize(edge_start, {.target_idx = INFU32});
}

void PopulateEdgeArraysWorker(size_t start_pos, size_t stop_pos,
                              GraphMetaData* meta, std::mutex& mut) {
  Graph& graph = meta->graph;
  std::vector<uint64_t> ids;
  std::vector<size_t> node_idx;
  std::vector<uint64_t> dist_sums;

  // Compute distances between the nodes of the way and store
  for (size_t way_idx = start_pos; way_idx < stop_pos; ++way_idx) {
    const GWay& way = graph.ways.at(way_idx);
    ids.clear();
    node_idx.clear();
    dist_sums.clear();
    // Decode node_ids.
    std::uint64_t num_nodes;
    std::uint8_t* ptr = way.node_ids;
    ptr += DecodeUInt(ptr, &num_nodes);
    ptr += DecodeNodeIds(ptr, num_nodes, &ids);

    // Compute distances sum from start and store in distance vector.
    // Non-existing nodes add 0 to the distance.
    NodeBuilder::VNode prev_node = {.id = 0, .lat = 0, .lon = 0};
    int64_t sum = 0;
    for (const uint64_t id : ids) {
      if (meta->way_nodes_seen->GetBit(id)) {
        NodeBuilder::VNode node;
        if (!NodeBuilder::FindNode(*meta->node_table, id, &node)) {
          // Should not happen, all 'seen' nodes should exist.
          ABORT_S() << absl::StrFormat("Way:%llu has missing node %llu", way.id,
                                       id);
        }
        // Sum up distance so far.
        if (prev_node.id != 0) {
          sum += calculate_distance(prev_node.lat, prev_node.lon, node.lat,
                                    node.lon);
        }
        prev_node = node;
      }
      dist_sums.push_back(sum);
      if (meta->way_nodes_needed->GetBit(id)) {
        std::size_t idx = graph.FindNodeIndex(id);
        CHECK_S(idx >= 0 && idx < graph.nodes.size());
        node_idx.push_back(idx);
      } else {
        node_idx.push_back(std::numeric_limits<size_t>::max());
      }
    }
    CHECK_EQ_S(ids.size(), dist_sums.size());
    CHECK_EQ_S(ids.size(), node_idx.size());

    // Go through 'needed' nodes (skip the others) and output edges.
    {
      const WaySharedAttrs& wsa = GetWSA(graph, way);
      // TODO: use opt.vt instead of doing it for car.
      const ACCESS acc_car_f =
          GetRAFromWSA(wsa, VH_MOTORCAR, DIR_FORWARD).access;
      const ACCESS acc_car_b =
          GetRAFromWSA(wsa, VH_MOTORCAR, DIR_BACKWARD).access;
      const bool restr_car_f = RestrictedAccess(acc_car_f);
      const bool restr_car_b = RestrictedAccess(acc_car_b);

      const bool vt_forward =
          RoutableAccess(GetRAFromWSA(wsa, meta->opt.vt, DIR_FORWARD).access);
      const bool vt_backward =
          RoutableAccess(GetRAFromWSA(wsa, meta->opt.vt, DIR_BACKWARD).access);

      std::unique_lock<std::mutex> l(mut);
      int last_pos = -1;
      for (size_t pos = 0; pos < ids.size(); ++pos) {
        uint64_t id = ids.at(pos);
        if (meta->way_nodes_needed->GetBit(id)) {
          if (last_pos >= 0) {
            // Emit edge.
            std::size_t idx1 = node_idx.at(last_pos);
            std::size_t idx2 = node_idx.at(pos);
            uint64_t distance_cm = dist_sums.at(pos) - dist_sums.at(last_pos);
            // Store edges with the summed up distance.

            // if (WSAAnyRoutable(wsa, DIR_FORWARD) &&
            //     WSAAnyRoutable(wsa, DIR_BACKWARD)) {
            if (vt_forward && vt_backward) {
              AddEdge(graph, idx1, idx2, /*inverted=*/false,
                      /*contra_way=*/false,
                      /*both_directions=*/true, way_idx, distance_cm,
                      restr_car_f);
              AddEdge(graph, idx2, idx1, /*inverted=*/false,
                      /*contra_way=*/true,
                      /*both_directions=*/true, way_idx, distance_cm,
                      restr_car_b);
              // } else if (WSAAnyRoutable(wsa, DIR_FORWARD)) {
            } else if (vt_forward) {
              AddEdge(graph, idx1, idx2, /*inverted=*/false,
                      /*contra_way=*/false,
                      /*both_directions=*/false, way_idx, distance_cm,
                      restr_car_f);
              // Inverted edges should have the same contra way as the
              // non-inverted original edge. This way, using EDGE_DIR(e) when
              // querying the way information works the same for inverted and
              // non-inverted edges.
              AddEdge(graph, idx2, idx1, /*inverted=*/true,
                      /*contra_way=*/false,
                      /*both_directions=*/false, way_idx, distance_cm,
                      restr_car_f);
            } else {
              // CHECK_S(WSAAnyRoutable(wsa, DIR_BACKWARD)) << way.id;
              CHECK_S(vt_backward) << way.id;
              AddEdge(graph, idx2, idx1, /*inverted=*/false,
                      /*contra_way=*/true,
                      /*both_directions=*/false, way_idx, distance_cm,
                      restr_car_b);
              // Inverted edges should have the same contra way as the
              // non-inverted original edge. This way, using EDGE_DIR(e) when
              // querying the way information works the same for inverted and
              // non-inverted edges.
              AddEdge(graph, idx1, idx2, /*inverted=*/true,
                      /*contra_way=*/true,
                      /*both_directions=*/false, way_idx, distance_cm,
                      restr_car_b);
            }
          }
          last_pos = pos;
        }
      }
    }
  }
}

void PopulateEdgeArrays(GraphMetaData* meta) {
  FUNC_TIMER();
  std::mutex mut;
  ThreadPool pool;
  ArrayChunker chunker(meta->graph.ways.size(), 1024 * 16);
  for (const auto& chunk : chunker.chunks) {
    pool.AddWork([meta, &mut, &chunk](int thread_idx) {
      PopulateEdgeArraysWorker(chunk.start, chunk.stop, meta, mut);
    });
  }
  pool.Start(meta->opt.n_threads);
  pool.WaitAllFinished();
}

namespace {
// Sort the edges [start..stop) in g->edges by ascending (target_idx, way_idx).
void SortEdgeSpan(Graph* g, uint32_t start, uint32_t stop) {
  std::sort(
      g->edges.begin() + start, g->edges.begin() + stop,
      [](const GEdge& e0, const GEdge& e1) {
        return e0.target_idx < e1.target_idx ||
               (e0.target_idx == e1.target_idx && e0.way_idx < e1.way_idx);
      });
}
}  // namespace

void SortAllEdges(GraphMetaData* meta) {
  FUNC_TIMER();

  Graph& g = meta->graph;
  ThreadPool pool;
  ArrayChunker chunker(g.nodes.size(), 1024 * 32);
  for (const auto& chunk : chunker.chunks) {
    pool.AddWork([meta, &g, &chunk](int thread_idx) {
      for (uint32_t from_idx = chunk.start; from_idx < chunk.stop; ++from_idx) {
        const GNode& n = g.nodes.at(from_idx);
        // Sort forward edges.
        SortEdgeSpan(&g, n.edges_start_pos,
                     n.edges_start_pos + n.num_forward_edges);
        // Sort inverted edges.
        SortEdgeSpan(&g, n.edges_start_pos + n.num_forward_edges,
                     gnode_edges_stop(g, from_idx));
      }
    });
  }
  pool.Start(meta->opt.n_threads);
  pool.WaitAllFinished();
}

void MarkUniqueEdges(GraphMetaData* meta) {
  FUNC_TIMER();
  for (uint32_t i = 0; i < meta->graph.nodes.size(); ++i) {
    MarkUniqueOther(gnode_all_edges(meta->graph, i));
  }
}

void LoadTurnRestrictionsFromRelations(OsmPbfReader* reader,
                                       GraphMetaData* meta,
                                       BuildGraphStats* stats) {
  FUNC_TIMER();
  // Get data.
  std::vector<TRResult> results(reader->n_threads());
  reader->ReadRelations([&meta, &results](const OSMTagHelper& tagh,
                                          const OSMPBF::Relation& osm_rel,
                                          int thread_idx, std::mutex& mut) {
    ConsumeRelation(tagh, osm_rel, meta, &results.at(thread_idx));
  });
  for (const TRResult& res : results) {
    stats->num_turn_restriction_success += res.num_success;
    stats->max_turn_restriction_via_ways = std::max(
        stats->max_turn_restriction_via_ways, res.max_success_via_ways);
    stats->num_turn_restriction_error += res.num_error;
    stats->num_turn_restriction_error_connection += res.num_error_connection;
    for (const TurnRestriction& tr : res.trs) {
      if (tr.via_is_node) {
        CHECK_EQ_S(tr.path.size(), 2);
        meta->simple_turn_restrictions.push_back(tr);
      } else {
        CHECK_GT_S(tr.path.size(), 2);
        meta->graph.complex_turn_restrictions.push_back(tr);
      }
    }
  }

  // Store simple turn restrictions.
  SortTurnRestrictions(&(meta->simple_turn_restrictions));
  meta->graph.simple_turn_restriction_map = ComputeSimpleTurnRestrictionMap(
      meta->graph, meta->opt.verb_turn_restrictions,
      meta->simple_turn_restrictions);
  MarkSimpleViaNodes(&(meta->graph));

  // Store complex turn restrictions.
  SortTurnRestrictions(&(meta->graph.complex_turn_restrictions));
  meta->graph.complex_turn_restriction_map =
      ComputeTurnRestrictionMapToFirst(meta->graph.complex_turn_restrictions);
  MarkComplexTriggerEdges(&(meta->graph));
}

void ComputeShortestPathsInAllClusters(GraphMetaData* meta) {
  FUNC_TIMER();
  RoutingMetricTime metric;
  if (!meta->graph.clusters.empty()) {
    ThreadPool pool;
    for (GCluster& cluster : meta->graph.clusters) {
      pool.AddWork([meta, &metric, &cluster](int) {
        build_clusters::ComputeShortestClusterPaths(meta->graph, metric,
                                                    meta->opt.vt, &cluster);
      });
    }
    pool.Start(meta->opt.n_threads);
    pool.WaitAllFinished();
  }
}

void ComputeShortestEdgePathsInAllClusters(GraphMetaData* meta) {
  FUNC_TIMER();
  RoutingMetricTime metric;
  if (!meta->graph.clusters.empty()) {
    ThreadPool pool;
    for (GCluster& cluster : meta->graph.clusters) {
      pool.AddWork([meta, &metric, &cluster](int) {
        build_clusters::ComputeShortestClusterEdgePaths(meta->graph, metric,
                                                        meta->opt.vt, &cluster);
      });
    }
    pool.Start(meta->opt.n_threads);
    pool.WaitAllFinished();
  }
}

void ClusterGraph(const BuildGraphOptions& opt, GraphMetaData* meta) {
  FUNC_TIMER();
  build_clusters::ExecuteLouvain(opt.n_threads, &meta->graph);
  build_clusters::UpdateGraphClusterInformation(&meta->graph);

#if 0
  if (opt.align_clusters_to_ncc && opt.merge_tiny_clusters) {
    build_clusters::MergeTinyClusters(&(meta->graph));
    build_clusters::UpdateGraphClusterInformation(opt.align_clusters_to_ncc,
                                                  &meta->graph);
  }
#endif

  // build_clusters::StoreClusterInformation(gvec, &meta->graph);
  build_clusters::PrintClusterInformation(meta->graph);

  ComputeShortestPathsInAllClusters(meta);
  ComputeShortestEdgePathsInAllClusters(meta);

  if (meta->opt.check_shortest_cluster_paths) {
    // Check if astar and dijkstra find the same shortest paths.
    build_clusters::CheckShortestClusterPaths(meta->graph, meta->opt.n_threads);
  }
  build_clusters::AssignClusterColors(&(meta->graph));
}

void FillStats(const OsmPbfReader& reader, GraphMetaData* meta,
               BuildGraphStats* stats) {
  FUNC_TIMER();
  const Graph& g = meta->graph;

  for (const GCluster& c : g.clusters) {
    stats->num_cluster_border_in_edges += c.border_in_edges.size();
    stats->num_cluster_border_out_edges += c.border_out_edges.size();
  }

  stats->num_nodes_in_pbf = reader.CountEntries(OsmPbfReader::ContentNodes);
  stats->num_ways_in_pbf = reader.CountEntries(OsmPbfReader::ContentWays);
  stats->num_relations_in_pbf =
      reader.CountEntries(OsmPbfReader::ContentRelations);

  for (const GWay& w : g.ways) {
    const WaySharedAttrs& wsa = GetWSA(g, w);
    const RoutingAttrs ra_forw = GetRAFromWSA(wsa, VH_MOTORCAR, DIR_FORWARD);
    const RoutingAttrs ra_backw = GetRAFromWSA(wsa, VH_MOTORCAR, DIR_BACKWARD);

    if (RoutableAccess(ra_forw.access) && RoutableAccess(ra_backw.access) &&
        ra_forw.maxspeed != ra_backw.maxspeed) {
      stats->num_ways_diff_maxspeed += 1;
    }
    if ((RoutableAccess(ra_forw.access) && ra_forw.maxspeed == 0) ||
        (RoutableAccess(ra_backw.access) && ra_backw.maxspeed == 0)) {
      stats->num_ways_no_maxspeed += 1;
    }
    stats->num_ways_has_country += w.uniform_country;
    stats->num_ways_has_streetname += w.streetname == nullptr ? 0 : 1;
    stats->num_ways_oneway_car +=
        (wsa.ra[0].access == ACC_NO) != (wsa.ra[1].access == ACC_NO);
    if ((FreeAccess(wsa.ra[0].access) && RestrictedAccess(wsa.ra[1].access)) ||
        (RestrictedAccess(wsa.ra[0].access) && FreeAccess(wsa.ra[1].access))) {
      LOG_S(INFO) << "Way has mixed restrictions: " << w.id;
      stats->num_ways_mixed_restricted_car += 1;
    }
  }

  std::vector<uint64_t> num_forwards(MAX_NUM_EDGES_OUT + 1, 0);
  std::vector<int64_t> num_forwards_node_id(MAX_NUM_EDGES_OUT + 1, 0);
  for (size_t node_idx = 0; node_idx < g.nodes.size(); ++node_idx) {
    const GNode& n = g.nodes.at(node_idx);

    if (n.cluster_border_node) {
      stats->num_cluster_border_nodes++;
    } else if (n.cluster_id != INVALID_CLUSTER_ID) {
      stats->num_cluster_inside_nodes++;
    } else {
      stats->num_cluster_outside_nodes++;
    }

    if (n.large_component == 0) {
      stats->num_nodes_in_small_component++;
    }
    stats->num_nodes_no_country += (n.ncc == INVALID_NCC ? 1 : 0);
    stats->num_nodes_simple_tr_via += n.simple_turn_restriction_via_node;

    {
      uint32_t unique_edges = gnode_num_unique_edges(g, node_idx);
      uint32_t idx = (unique_edges >= stats->num_nodes_unique_edges_dim)
                         ? stats->num_nodes_unique_edges_dim - 1
                         : unique_edges;
      stats->num_nodes_unique_edges[idx]++;
    }

    stats->num_edges_at_simple_tr_via +=
        n.simple_turn_restriction_via_node ? n.num_forward_edges : 0;

    int64_t num_inverted_edges = 0;
    int64_t num_forward_edges = 0;
    num_forwards.at(n.num_forward_edges)++;
    if (absl::ToInt64Nanoseconds(absl::Now() - absl::Time()) %
            num_forwards.at(n.num_forward_edges) ==
        0) {
      num_forwards_node_id.at(n.num_forward_edges) = n.node_id;
    }

    for (const GEdge& e : gnode_all_edges(g, node_idx)) {
      const GNode& other = g.nodes.at(e.target_idx);
      // const bool edge_dead_end = n.dead_end || other.dead_end;
      if (!e.unique_target) {
        stats->num_edges_non_unique++;
      }
      num_inverted_edges += e.inverted;
      num_forward_edges += (e.inverted == 0);
      stats->num_edges_bridge += e.is_deadend_bridge();

      stats->num_edges_low_priority +=
          (!e.inverted && (e.road_priority == GEdge::PRIO_LOW));
      stats->num_edges_high_priority +=
          (!e.inverted && (e.road_priority == GEdge::PRIO_HIGH));

      if (!e.inverted && e.target_idx != node_idx) {
        stats->sum_edge_length_cm += e.distance_cm;
        if (e.distance_cm == 0) {
          LOG_S(INFO) << "Edge with length 0 from " << n.node_id << " to "
                      << other.node_id;
        }
        stats->min_edge_length_cm =
            std::min(stats->min_edge_length_cm, (int64_t)e.distance_cm);
        stats->max_edge_length_cm =
            std::max(stats->max_edge_length_cm, (int64_t)e.distance_cm);
        stats->num_edges_forward_car_restr_unset +=
            (e.car_label == GEdge::LABEL_UNSET);
        stats->num_edges_forward_car_restr_free +=
            (e.car_label == GEdge::LABEL_FREE);
        stats->num_edges_forward_car_restricted +=
            (e.car_label == GEdge::LABEL_RESTRICTED);
        stats->num_edges_forward_car_restricted2 +=
            (e.car_label == GEdge::LABEL_RESTRICTED_SECONDARY);
        stats->num_edges_forward_car_strange += e.car_label_strange;
        CHECK_S(e.car_label_strange == 0 ||
                e.car_label == GEdge::LABEL_RESTRICTED_SECONDARY);
        CHECK_NE_S(e.car_label, GEdge::LABEL_TEMPORARY);
        stats->num_edges_forward_car_forbidden +=
            !RoutableAccess(GetRAFromWSA(g, e, VH_MOTORCAR).access);
      }

      stats->num_cross_country_edges += (!e.inverted && e.cross_country);
      stats->num_cross_country_restricted +=
          (!e.inverted && e.cross_country && e.car_label != GEdge::LABEL_FREE);

      if (n.cluster_id == INVALID_CLUSTER_ID ||
          other.cluster_id == INVALID_CLUSTER_ID) {
        stats->num_cluster_outside_edges += 1;
      } else if (n.cluster_id != other.cluster_id) {
        stats->num_cluster_border_edges += 1;
        if (e.car_label != GEdge::LABEL_FREE) {
          stats->num_cluster_border_edges_restr += 1;
          LOG_S(INFO) << absl::StrFormat(
              "Cluster border cluster restricted edge %lld to %lld, label:%d "
              "way:%lld",
              n.node_id, other.node_id, (int)e.car_label,
              g.ways.at(e.way_idx).id);
        }
      } else {
        stats->num_cluster_inside_edges += 1;
      }

      // By construction, this is 0, because dead-ends are omitted from
      // clusters.
      // stats->num_cross_cluster_restricted_dead_end +=
      //     (e.car_label != GEdge::LABEL_FREE && edge_dead_end);
    }
    stats->max_edges =
        std::max(stats->max_edges, num_forward_edges + num_inverted_edges);
    stats->num_edges_forward += num_forward_edges;
    stats->num_edges_inverted += num_inverted_edges;
    stats->max_edges_out = std::max(stats->max_edges_out, num_forward_edges);
    stats->max_edges_inverted =
        std::max(stats->max_edges_inverted, num_inverted_edges);
  }
  for (size_t i = 0; i <= MAX_NUM_EDGES_OUT; ++i) {
    LOG_S(INFO) << absl::StrFormat(
        "Num nodes with %llu out edges: %llu (%2.2f%%) example_id:%lld", i,
        num_forwards.at(i),
        (100.0 * num_forwards.at(i)) / (g.nodes.size() + 0.01),
        num_forwards_node_id.at(i));
  }
}

void PrintStats(const GraphMetaData& meta, const BuildGraphStats& stats) {
  const Graph& g = meta.graph;

  LOG_S(INFO) << "=========== Pbf Stats ============";
  LOG_S(INFO) << absl::StrFormat("Nodes:              %12lld",
                                 stats.num_nodes_in_pbf);
  LOG_S(INFO) << absl::StrFormat("Ways:               %12lld",
                                 stats.num_ways_in_pbf);
  LOG_S(INFO) << absl::StrFormat("Relations:          %12lld",
                                 stats.num_relations_in_pbf);
  LOG_S(INFO) << absl::StrFormat("Ways with hw tag:   %12lld",
                                 stats.num_ways_with_highway_tag);
  LOG_S(INFO) << absl::StrFormat("Edges with hw tag:  %12lld",
                                 stats.num_edges_with_highway_tag);
  LOG_S(INFO) << absl::StrFormat("Noderefs with hw tag:%11lld",
                                 stats.num_noderefs_with_highway_tag);
  LOG_S(INFO) << absl::StrFormat("Ways too short:     %12lld",
                                 stats.num_ways_too_short);
  LOG_S(INFO) << absl::StrFormat("Ways with missing nodes:%8lld",
                                 stats.num_ways_missing_nodes);
  LOG_S(INFO) << absl::StrFormat("Ways dup segments:  %12lld",
                                 stats.num_ways_dup_segments);

  LOG_S(INFO) << "========= Various Stats ==========";
  LOG_S(INFO) << absl::StrFormat("Num var-nodes:      %12lld",
                                 meta.node_table->total_records());
  LOG_S(INFO) << absl::StrFormat(
      "  bytes/var-node:   %12.2f",
      static_cast<double>(meta.node_table->mem_allocated()) /
          meta.node_table->total_records());
  LOG_S(INFO) << absl::StrFormat("Num t-restr success: %11lld",
                                 stats.num_turn_restriction_success);
  LOG_S(INFO) << absl::StrFormat("Num t-restr errors:   %10lld",
                                 stats.num_turn_restriction_error);
  LOG_S(INFO) << absl::StrFormat("Num t-restr simple:  %11lld",
                                 meta.simple_turn_restrictions.size());
  LOG_S(INFO) << absl::StrFormat("Num t-restr complex: %11lld",
                                 g.complex_turn_restrictions.size());
  LOG_S(INFO) << absl::StrFormat("Num t-restr comb/simple:%8lld",
                                 g.simple_turn_restriction_map.size());
  LOG_S(INFO) << absl::StrFormat("Max t-restr via ways: %10llu",
                                 stats.max_turn_restriction_via_ways);
  LOG_S(INFO) << absl::StrFormat("Num t-restr errors conn:%8lld",
                                 stats.num_turn_restriction_error_connection);
  LOG_S(INFO) << absl::StrFormat("Num node attrs:      %11lld",
                                 g.node_tags_sorted.size());
  LOG_S(INFO) << absl::StrFormat("Num node barrier free:%10lld",
                                 stats.num_node_barrier_free);
  LOG_S(INFO) << absl::StrFormat("Num edge barrier block:%9lld",
                                 stats.num_edge_barrier_block);
  LOG_S(INFO) << absl::StrFormat("Num edge barrier merged:%8lld",
                                 stats.num_edge_barrier_merged);
  LOG_S(INFO) << absl::StrFormat("Num edge barrier no-uturn:%6lld",
                                 stats.num_edge_barrier_no_uturn);

  LOG_S(INFO) << "========= Graph Stats ============";
  std::int64_t way_bytes = g.ways.size() * sizeof(GWay);
  std::int64_t way_added_bytes = g.unaligned_pool_.MemAllocated();
  std::int64_t way_shared_attrs_bytes =
      g.way_shared_attrs.size() * sizeof(WaySharedAttrs);
  LOG_S(INFO) << absl::StrFormat("Ways selected:      %12lld", g.ways.size());

  LOG_S(INFO) << absl::StrFormat("  Nodes \"seen\":     %12lld",
                                 meta.way_nodes_seen->CountBits());

  LOG_S(INFO) << absl::StrFormat("  Nodes \"needed\":   %12lld",
                                 meta.way_nodes_needed->CountBits());

  LOG_S(INFO) << absl::StrFormat("  No maxspeed:      %12lld",
                                 stats.num_ways_no_maxspeed);
  LOG_S(INFO) << absl::StrFormat("  Diff maxspeed/dir:%12lld",
                                 stats.num_ways_diff_maxspeed);
  LOG_S(INFO) << absl::StrFormat("  Has country:      %12lld",
                                 stats.num_ways_has_country);
  LOG_S(INFO) << absl::StrFormat("  Has no country:   %12lld",
                                 g.ways.size() - stats.num_ways_has_country);
  LOG_S(INFO) << absl::StrFormat("  Has streetname:   %12lld",
                                 stats.num_ways_has_streetname);
  LOG_S(INFO) << absl::StrFormat("  Oneway for cars:  %12lld",
                                 stats.num_ways_oneway_car);
  LOG_S(INFO) << absl::StrFormat("  Mixed restr cars: %12lld",
                                 stats.num_ways_mixed_restricted_car);
  LOG_S(INFO) << absl::StrFormat("  Closed ways:      %12lld",
                                 stats.num_ways_closed);
  LOG_S(INFO) << absl::StrFormat("  Bytes per way     %12.2f",
                                 (double)way_bytes / g.ways.size());
  LOG_S(INFO) << absl::StrFormat("  Added per way     %12.2f",
                                 (double)way_added_bytes / g.ways.size());
  LOG_S(INFO) << absl::StrFormat("  Total Bytes:      %12lld",
                                 way_bytes + way_added_bytes);

  LOG_S(INFO) << absl::StrFormat("Nodes needed:       %12lld", g.nodes.size());
  LOG_S(INFO) << absl::StrFormat("  Deadend nodes:    %12lld",
                                 stats.num_dead_end_nodes);
  LOG_S(INFO) << absl::StrFormat("  Node in small comp:%11lld",
                                 stats.num_nodes_in_small_component);
  LOG_S(INFO) << absl::StrFormat("  Nodes no country: %12lld",
                                 stats.num_nodes_no_country);

  for (uint32_t idx = 0; idx < stats.num_nodes_unique_edges_dim; ++idx) {
    LOG_S(INFO) << absl::StrFormat("  Has %u unique edges: %10lld", idx,
                                   stats.num_nodes_unique_edges[idx]);
  }

  std::int64_t node_bytes = g.nodes.size() * sizeof(GNode);
  std::int64_t edge_memory = g.edges.size() * sizeof(GEdge);
  LOG_S(INFO) << absl::StrFormat("  Bytes per node    %12.2f",
                                 (double)node_bytes / g.nodes.size());
  LOG_S(INFO) << absl::StrFormat("  Edge Mem per node %12.2f",
                                 (double)edge_memory / g.nodes.size());
  LOG_S(INFO) << absl::StrFormat("  Total Bytes:      %12lld",
                                 node_bytes + edge_memory);

  LOG_S(INFO) << "Edges";
  LOG_S(INFO) << absl::StrFormat("  Num out:          %12lld",
                                 stats.num_edges_forward);
  LOG_S(INFO) << absl::StrFormat("  Num inverted:     %12lld",
                                 stats.num_edges_inverted);
  LOG_S(INFO) << absl::StrFormat("  Num non-unique:   %12lld",
                                 stats.num_edges_non_unique);
  LOG_S(INFO) << absl::StrFormat("  turn cost table size:%9lld",
                                 g.turn_costs.size());
  LOG_S(INFO) << absl::StrFormat("  Car restr unset:  %12lld",
                                 stats.num_edges_forward_car_restr_unset);
  LOG_S(INFO) << absl::StrFormat("  Car restr free:   %12lld",
                                 stats.num_edges_forward_car_restr_free);
  LOG_S(INFO) << absl::StrFormat("  Car restricted:   %12lld",
                                 stats.num_edges_forward_car_restricted);
  LOG_S(INFO) << absl::StrFormat("  Car restricted 2nd:%11lld",
                                 stats.num_edges_forward_car_restricted2);
  LOG_S(INFO) << absl::StrFormat("  Car strange:      %12lld",
                                 stats.num_edges_forward_car_strange);
  LOG_S(INFO) << absl::StrFormat("  Car forbidden:    %12lld",
                                 stats.num_edges_forward_car_forbidden);

  LOG_S(INFO) << absl::StrFormat("  Low priority:     %12lld",
                                 stats.num_edges_low_priority);
  LOG_S(INFO) << absl::StrFormat("  High priority:    %12lld",
                                 stats.num_edges_high_priority);
  LOG_S(INFO) << absl::StrFormat("  Bridge:           %12lld",
                                 stats.num_edges_bridge);

  LOG_S(INFO) << absl::StrFormat("  Min edge length:  %12lld",
                                 stats.min_edge_length_cm);
  LOG_S(INFO) << absl::StrFormat("  Max edge length:  %12lld",
                                 stats.max_edge_length_cm);
  LOG_S(INFO) << absl::StrFormat(
      "  Avg edge length   %12.0f",
      (double)stats.sum_edge_length_cm / stats.num_edges_forward);
  LOG_S(INFO) << absl::StrFormat(
      "  Num edges/node:   %12.2f",
      static_cast<double>(stats.num_edges_inverted + stats.num_edges_forward) /
          g.nodes.size());
  LOG_S(INFO) << absl::StrFormat("  Max edges:        %12lld", stats.max_edges);
  LOG_S(INFO) << absl::StrFormat("  Max edges out:    %12lld",
                                 stats.max_edges_out);
  LOG_S(INFO) << absl::StrFormat("  Max edges inverted: %10lld",
                                 stats.max_edges_inverted);

  LOG_S(INFO) << absl::StrFormat("  Cross country edges:%10lld",
                                 stats.num_cross_country_edges);
  LOG_S(INFO) << absl::StrFormat("  Cross country restr:%10lld",
                                 stats.num_cross_country_restricted);

  LOG_S(INFO) << absl::StrFormat("Clusters:           %12lld",
                                 g.clusters.size());
  LOG_S(INFO) << absl::StrFormat("  Num border nodes: %12lld",
                                 stats.num_cluster_border_nodes);
  LOG_S(INFO) << absl::StrFormat("  Num inside nodes: %12lld",
                                 stats.num_cluster_inside_nodes);
  LOG_S(INFO) << absl::StrFormat("  Num outside nodes:%12lld",
                                 stats.num_cluster_outside_nodes);

  LOG_S(INFO) << absl::StrFormat("  Num border edges: %12lld",
                                 stats.num_cluster_border_edges);
  LOG_S(INFO) << absl::StrFormat("  Num inside edges: %12lld",
                                 stats.num_cluster_inside_edges);
  LOG_S(INFO) << absl::StrFormat("  Num outside edges:%12lld",
                                 stats.num_cluster_outside_edges);

  LOG_S(INFO) << absl::StrFormat("  Num border in edges:%10lld",
                                 stats.num_cluster_border_in_edges);
  LOG_S(INFO) << absl::StrFormat("  Num border out edges:%9lld",
                                 stats.num_cluster_border_out_edges);

  LOG_S(INFO) << absl::StrFormat("  Num restr border edges:%7lld",
                                 stats.num_cluster_border_edges_restr);

  // LOG_S(INFO) << absl::StrFormat("  Cross clust restr+de:%9lld",
  //                           stats.num_cross_cluster_restricted_dead_end);

  LOG_S(INFO) << "========= Memory Stats ===========";
  LOG_S(INFO) << absl::StrFormat("Bitset memory:      %12.2f MB",
                                 (meta.way_nodes_seen->NumUsedBytes() +
                                  meta.way_nodes_needed->NumUsedBytes()) /
                                     1000000.0);
  LOG_S(INFO) << absl::StrFormat("Varnode memory:     %12.2f MB",
                                 meta.node_table->mem_allocated() / 1000000.0);
  LOG_S(INFO) << absl::StrFormat("Node graph memory:  %12.2f MB",
                                 (node_bytes) / 1000000.0);
  LOG_S(INFO) << absl::StrFormat("Edge graph memory:  %12.2f MB",
                                 (edge_memory) / 1000000.0);
  LOG_S(INFO) << absl::StrFormat("Way graph memory:   %12.2f MB",
                                 (way_bytes + way_added_bytes) / 1000000.0);
  LOG_S(INFO) << absl::StrFormat("Way shared attrs:   %12.2f MB",
                                 way_shared_attrs_bytes / 1000000.0);
  LOG_S(INFO) << absl::StrFormat("Total graph memory: %12.2f MB",
                                 (node_bytes + edge_memory + way_bytes +
                                  way_added_bytes + way_shared_attrs_bytes) /
                                     1000000.0);
}

void PrintWayTagStats(const Graph& g, const FrequencyTable& ft) {
  const std::vector<FrequencyTable::Entry> v = ft.GetSortedElements();
  for (size_t i = 0; i < v.size(); ++i) {
    const FrequencyTable::Entry& e = v.at(i);
    char cc[3] = "--";  // Way not found, can easily happen because many ways
                        // are not stored in the graph.
    {
      const GWay* way = g.FindWay(e.example_id);
      if (way != nullptr) {
        strcpy(cc, ">1");
        if (way->uniform_country) {
          CountryNumToTwoLetter(way->ncc, cc);
        }
      }
    }

    RAW_LOG_F(INFO, "%7lu %10ld id:%10ld %s %s", i, e.ref_count, e.example_id,
              cc, std::string(e.key).c_str());
  }
  LOG_S(INFO) << absl::StrFormat(
      "Number of different ways tag configurations: %ld, total %ld (%.2f%%)",
      ft.TotalUnique(), ft.Total(), (100.0 * ft.TotalUnique()) / ft.Total());
}
}  // namespace

// inline void MarkUTurnAllowedEdges_Obsolete(Graph* g) {
//   FUNC_TIMER();
//   for (uint32_t from_idx = 0; from_idx < g->nodes.size(); ++from_idx) {
//     for (GEdge& e : gnode_forward_edges(*g, from_idx)) {
//       e.car_uturn_allowed = IsUTurnAllowedEdgeOld(*g, from_idx, e);
//     }
//   }
// }

void MarkCrossingNodes(GraphMetaData* meta) {
  FUNC_TIMER();

  const Graph& g = meta->graph;
  ThreadPool pool;
  ArrayChunker chunker(meta->graph.node_tags_sorted.size(), 1024 * 16);
  for (const auto& chunk : chunker.chunks) {
    pool.AddWork([meta, &g, &chunk](int thread_idx) {
      for (size_t idx = chunk.start; idx < chunk.stop; ++idx) {
        const NodeTags& nt = meta->graph.node_tags_sorted.at(idx);
        if (nt.bit_crossing) {
          uint32_t node_idx = g.FindNodeIndex(nt.node_id);
          if (node_idx < g.nodes.size()) {
            meta->graph.nodes.at(node_idx).is_pedestrian_crossing = 1;
          }
        }
      }
    });
  }
  pool.Start(meta->opt.n_threads);
  pool.WaitAllFinished();
}

// Label the current edge and the edge at the next crossing according to some
// node tags. Note that at least on of nt.bit_traffic_signals, nt.bit_stop,
// nt.bit_give_way has to be true.
inline void LabelEdgeAndNextCrossing(Graph& g, const NodeTags& nt, FullEdge fe,
                                     FullEdge crossing_fe) {
  CHECK_S(fe.valid());
  if (nt.bit_traffic_signals) {
    if (!crossing_fe.valid()) {  // We didn't find a crossing.
      fe.gedge(g).traffic_signal = 1;
    } else if (fe == crossing_fe) {  // Signal is directly on crossing node.
      if (crossing_fe.gedge(g).road_priority != GEdge::PRIO_SIGNALS) {
        // The signal was not found farther away, so put it.
        fe.gedge(g).traffic_signal = 1;
        crossing_fe.gedge(g).road_priority = GEdge::PRIO_SIGNALS;
      }
    } else {
      // Start and crossing edges differ, signal not directly at crossing.
      fe.gedge(g).traffic_signal = 1;
      crossing_fe.gedge(g).road_priority = GEdge::PRIO_SIGNALS;
      // Delete the signal flag that might be directly at crossing, it is
      // replaced by the signal father away.
      crossing_fe.gedge(g).traffic_signal = 0;
    }
  } else if (nt.bit_stop || nt.bit_give_way) {
    if (nt.bit_stop) {
      fe.gedge(g).stop_sign = 1;
    }
    if (crossing_fe.valid() &&
        crossing_fe.gedge(g).road_priority != GEdge::PRIO_SIGNALS) {
      // Set priority of edge arriving at crossing, but only if is not
      // controlled by a traffic signal.
      crossing_fe.gedge(g).road_priority = GEdge::PRIO_LOW;
    }
  } else {
    CHECK_S(false);
  }
}

// Some nodes are tagged as "traffic_signals", "stop" or "give_way". They often
// come with a direction in the tags, but sometimes the direction has to be
// inferred by looking at the direction of the road or by finding the closest
// crossing.
//
// For these tags we try to find the next crossing and mark the edge arriving
// at the crossing as high-signal, high or low priority, depending on the tag.
//
// For "traffic_signals" and "stop", we additionally label the edge leading to
// the node, because this will add some time to the turn costs.
void LabelEdgesFromNodeTags(GraphMetaData* meta) {
  FUNC_TIMER();
  const Graph& g = meta->graph;
  const VEHICLE vt = meta->opt.vt;

  for (NodeTags& nt : meta->graph.node_tags_sorted) {
    const size_t node_idx = g.FindNodeIndex(nt.node_id);
    if (node_idx >= g.nodes.size()) {
      LOG_S(INFO) << "Node " << nt.node_id << " for stop not found";
      continue;
    }

    const std::vector<FullEdge> in_edges = gnode_incoming_edges(g, node_idx);
    if (in_edges.size() == 0) {
      continue;  // No edge that is affected.
    }

    if (nt.bit_stop || nt.bit_give_way || nt.bit_traffic_signals) {
      if (nt.direction == DIR_BOTH || nt.direction == DIR_MAX) {
        // ======================================
        // Missing direction or direction "both".
        // ======================================
        if (in_edges.size() == 1) {
          // We have only one incoming edge, so the direction is given. This
          // is probably a one way road. Label the edge.
          const FullEdge cr = FollowEdgeToCrossing(g, vt, in_edges.front());
          LabelEdgeAndNextCrossing(meta->graph, nt, in_edges.front(), cr);
          LOG_S(INFO) << absl::StrFormat(
              "Infer direction for 1-stop-edge %lld->%lld crossing node %lld",
              in_edges.front().start_node(g).node_id,
              in_edges.front().target_node(g).node_id,
              cr.valid() ? GetGNodeIdSafe(g, cr.gedge(g).target_idx) : -1);
        } else if (in_edges.size() == 2 &&
                   gnode_num_unique_edges(g, node_idx,
                                          /*ignore_loops=*/true) == 2) {
          // A single street with both directions allowed. Infer direction if
          // we find a crossing for one edge, but not for the other.
          if (nt.bit_traffic_signals ||
              (nt.bit_stop && nt.direction == DIR_BOTH)) {
            // Assume it is both ways and there is no crossing to look for.
            // TODO: Some cases might not be handled correctly, for instance a
            // bicycle way crossing a car-road might be invisible here because
            // we ignored the bicycle ways.
            LabelEdgeAndNextCrossing(meta->graph, nt, in_edges.at(0), {});
            LabelEdgeAndNextCrossing(meta->graph, nt, in_edges.at(1), {});
            LOG_S(INFO) << absl::StrFormat(
                "Assume both ways direction for 2-stop-edges at crossing "
                "%lld",
                GetGNodeIdSafe(g, node_idx));

          } else {
            // Try to infer direction from the next crossing.
            const FullEdge cr[2] = {
                FollowEdgeToCrossing(g, vt, in_edges.at(0)),
                FollowEdgeToCrossing(g, vt, in_edges.at(1))};
            if (cr[0].valid() != cr[1].valid()) {
              // Found a crossing in on direction, but not in the other, so we
              // can use the found crossing to infer direction.
              const uint32_t idx = cr[0].valid() ? 0 : 1;
              const FullEdge& in_edge = in_edges.at(idx);
              LabelEdgeAndNextCrossing(meta->graph, nt, in_edge, cr[idx]);
              LOG_S(INFO) << absl::StrFormat(
                  "Infer direction for 2-stop-edge %lld->%lld and crossing "
                  "%lld",
                  in_edge.start_node(g).node_id, in_edge.target_node(g).node_id,
                  GetGNodeIdSafe(g, cr[idx].gedge(g).target_idx));
            } else if (!cr[0].valid() &&
                       (nt.bit_stop || nt.bit_traffic_signals)) {
              // Didn't find a crossing in both directions.
              LabelEdgeAndNextCrossing(meta->graph, nt, in_edges.at(0), {});
              LabelEdgeAndNextCrossing(meta->graph, nt, in_edges.at(1), {});
              LOG_S(INFO) << absl::StrFormat(
                  "Can't infer direction, assume both ways direction at "
                  "crossing %lld",
                  GetGNodeIdSafe(g, node_idx));
            } else {
              LOG_S(INFO) << absl::StrFormat(
                  "Can not infer direction for 2-stop node %lld",
                  GetGNodeIdSafe(g, node_idx));
            }
          }
        } else {
          // Looks like a crossing. If this is a traffic signals, or an
          // all-way stop, then label all incoming edges.
          if (nt.bit_traffic_signals || (nt.bit_stop && nt.stop_all)) {
            for (const FullEdge& in : in_edges) {
              LabelEdgeAndNextCrossing(meta->graph, nt, in, in);
            }
          } else {
            // We don't really know what to do. We don't use the data.
            LOG_S(INFO) << absl::StrFormat(
                "Can not handle crossing with no dir and stop=%u all=%u "
                "give_way=%u traffic_signals=%u at node %lld",
                nt.bit_stop, nt.stop_all, nt.bit_give_way,
                nt.bit_traffic_signals, GetGNodeIdSafe(g, node_idx));
          }
        }
      } else {
        // ================
        // Valid direction.
        // ================
        const bool contra_way = (nt.direction == DIR_BACKWARD);
        if (in_edges.size() == 1) {
          // Check that the direction is correct.
          const FullEdge& in_edge = in_edges.front();
          if (contra_way == in_edge.gedge(g).contra_way) {
            const FullEdge cr = FollowEdgeToCrossing(g, vt, in_edge);
            LabelEdgeAndNextCrossing(meta->graph, nt, in_edge, cr);
            LOG_S(INFO) << absl::StrFormat(
                "Correct direction for single stop-edge %lld->%lld",
                in_edge.start_node(g).node_id, in_edge.target_node(g).node_id);
          } else {
            LOG_S(INFO) << absl::StrFormat(
                "Wrong direction for single stop-edge %lld->%lld",
                in_edge.start_node(g).node_id, in_edge.target_node(g).node_id);
          }
        } else if (in_edges.size() == 2 &&
                   gnode_num_unique_edges(g, node_idx,
                                          /*ignore_loops=*/true) == 2) {
          bool good_dir[2];
          good_dir[0] = (contra_way == in_edges.at(0).gedge(g).contra_way);
          good_dir[1] = (contra_way == in_edges.at(1).gedge(g).contra_way);
          if (good_dir[0] != good_dir[1]) {
            const uint32_t idx = good_dir[0] ? 0 : 1;
            const FullEdge& in_edge = in_edges.at(idx);
            const FullEdge cr = FollowEdgeToCrossing(g, vt, in_edge);
            LabelEdgeAndNextCrossing(meta->graph, nt, in_edge, cr);
            LOG_S(INFO) << absl::StrFormat(
                "Correct direction for double stop-edge %lld->%lld",
                in_edge.start_node(g).node_id, in_edge.target_node(g).node_id);
          } else {
            LOG_S(INFO) << absl::StrFormat(
                "Wrong direction for double stop node %lld",
                GetGNodeIdSafe(g, node_idx));
          }
        } else {
          LOG_S(INFO) << absl::StrFormat(
              "Can not handle crossing with dir and stop=%u all=%u "
              "give_way=%u traffic_signals=%u at %llu-stop node %lld",
              nt.bit_stop, nt.stop_all, nt.bit_give_way, nt.bit_traffic_signals,
              in_edges.size(), GetGNodeIdSafe(g, node_idx));
        }
      }
    }
  }
}

void ComputeAllTurnCosts(GraphMetaData* meta) {
  FUNC_TIMER();

  Graph& g = meta->graph;
  const IndexedTurnRestrictions indexed_trs = {
      .sorted_trs = meta->simple_turn_restrictions,
      .map_to_first =
          ComputeTurnRestrictionMapToFirst(meta->simple_turn_restrictions)};

  ThreadPool pool;
  std::vector<DeDuperWithIds<TurnCostData>> deduper_per_thread(
      meta->opt.n_threads);

  ArrayChunker<int16_t> chunker(g.nodes.size(), 1024 * 32, -1);
  for (auto& chunk : chunker.chunks) {
    pool.AddWork([meta, &g, &deduper_per_thread, &indexed_trs,
                  &chunk](int thread_idx) {
      chunk.chunk_data = thread_idx;
      for (uint32_t from_idx = chunk.start; from_idx < chunk.stop; ++from_idx) {
        const GNode& from_node = g.nodes.at(from_idx);
        // For each outgoing edge of this node.
        for (uint32_t off = 0; off < from_node.num_forward_edges; ++off) {
          GEdge& e = g.edges.at(from_node.edges_start_pos + off);
          // Compute the turn costs for the target node of 'e'.
          TurnCostData tcd = ComputeTurnCostsForEdge(
              g, meta->opt.vt, indexed_trs, {from_idx, off});
          e.turn_cost_idx = deduper_per_thread.at(thread_idx).Add(tcd);
        }
      }
    });
  }

  pool.Start(meta->opt.n_threads);
  pool.WaitAllFinished();

  std::vector<std::vector<uint32_t>> vmappings;
  DeDuperWithIds<TurnCostData>::MergeSort(deduper_per_thread, &(g.turn_costs),
                                          &vmappings);
  uint32_t count = 0;
  for (uint32_t from_idx = 0; from_idx < g.nodes.size(); ++from_idx) {
    const GNode& from_node = g.nodes.at(from_idx);
    const auto& vmap = vmappings.at(chunker.ChunkAt(from_idx).chunk_data);
    for (uint32_t off = 0; off < from_node.num_forward_edges; ++off) {
      GEdge& e = g.edges.at(from_node.edges_start_pos + off);
      e.turn_cost_idx = vmap.at(e.turn_cost_idx);
      count++;
    }
  }

  LOG_S(INFO) << absl::StrFormat(
      "DeDuperWithIds<TurnCostData>: unique:%llu tot:%u", g.turn_costs.size(),
      count);
}

BuildGraphStats CollectThreadStats(
    const std::vector<BuildGraphStats>& thread_stats) {
  BuildGraphStats stats;
  for (const auto& s : thread_stats) {
    stats.AddStats(s);
  }
  return stats;
}

GraphMetaData BuildGraph(const BuildGraphOptions& opt) {
  GraphMetaData meta;
  meta.opt = opt;
  meta.node_table.reset(new DataBlockTable);

  // Reading is fastest with 7 threads on my hardware.
  OsmPbfReader reader(opt.pbf, std::min(16, opt.n_threads));

  // Combine a few setup operations that can be done in parallel.
  {
    ThreadPool pool;
    // Read file structure of pbf file.
    pool.AddWork([&reader](int thread_idx) { reader.ReadFileStructure(); });
    // Read country polygons and initialise tiler.
    pool.AddWork([&meta](int thread_idx) {
      meta.tiler.reset(new TiledCountryLookup(
          meta.opt.admin_filepattern,
          /*tile_size=*/TiledCountryLookup::kDegreeUnits / 5));
    });
    // Read routing config.
    pool.AddWork([&meta](int) {
      meta.per_country_config.reset(new PerCountryConfig);
      meta.per_country_config->ReadConfig(meta.opt.routing_config);
      meta.left_traffic_bits.LoadFromFile(meta.opt.left_traffic_config);
    });
    pool.Start(std::min(meta.opt.n_threads, 3));
    pool.WaitAllFinished();
  }

  LoadNodeCoordsAndAttrributes(opt.vt, &reader, meta.node_table.get(), &meta);
  LoadGWays(&reader, &meta);
  SortGWays(&meta);
  MarkNodesWithAttributesAsNeeded(&meta);

  AllocateGNodes(&meta);
  SetCountryInGNodes(&meta);

  // Now we know exactly which ways we have and which nodes are needed.
  // Create edges.
  ComputeEdgeCounts(&meta);
  AllocateEdgeArrays(&meta);
  PopulateEdgeArrays(&meta);
  for (const GEdge& e : meta.graph.edges) {
    CHECK_S(e.target_idx != INFU32);
  }
  SortAllEdges(&meta);
  MarkUniqueEdges(&meta);

  // ======================================================
  // Not the basic graph with nodes, edges and ways exists.
  // ======================================================

  LoadTurnRestrictionsFromRelations(&reader, &meta, &meta.Stats());
  StoreNodeBarrierData_Obsolete(&meta.graph, &meta.Stats());
  FindLargeComponents(&meta.graph);
  meta.Stats().num_dead_end_nodes = ApplyTarjan(meta.graph);
  LabelAllCarEdges(&meta.graph, Verbosity::Brief);
  // MarkUTurnAllowedEdges_Obsolete(&(meta.graph));

  MarkCrossingNodes(&meta);
  LabelEdgesFromNodeTags(&meta);
  ComputeAllTurnCosts(&meta);

  ClusterGraph(meta.opt, &meta);

  // Add up all the per thread stats.
  meta.global_stats = CollectThreadStats(*meta.thread_stats);

  // Output way tag stats early (it is often a lot of lines).
  if (meta.opt.log_way_tag_stats) {
    PrintWayTagStats(meta.graph, meta.global_stats.way_tag_stats);
  }
  ValidateGraph(meta.graph);
  FillStats(reader, &meta, &meta.global_stats);
  PrintStats(meta, meta.global_stats);
  LogMemoryUsage();

  return meta;
}
}  // namespace build_graph
