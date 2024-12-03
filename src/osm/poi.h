#pragma once

#include <map>
#include <vector>

#include "base/util.h"
#include "graph/graph_def.h"
#include "osm/osm_helpers.h"
#include "osm/read_osm_pbf.h"

namespace pois {

// Note that relations are currently not supported.
struct POI {
  char obj_type = 'x';  // n(ode), w(ay) or r(elation) or x for missing.
  std::int64_t id = -1;
  // Best available name and type of POI.
  std::string name;
  std::string type;

  // lat/lon of the point or the average lat/lon if a way or relation.
  std::uint32_t lat = INFU32;
  std::uint32_t lon = INFU32;

  // Cumulative numbers to compute average lat/lon values above.
  std::uint64_t sum_lat = 0;
  std::uint64_t sum_lon = 0;
  std::uint64_t num_points = 0;

  // Node in the road graph that is closest to the POI by lat/lon.
  std::uint32_t road_graph_node_idx = INFU32;
};

struct CollectedData {
  // The POIs that have been found.
  std::vector<POI> pois;
  // Contains OSM node ids which where referenced by a way or a relation and
  // should be accumulated to a POI.
  std::multimap<int64_t, size_t> refnode_to_poi_idx;
};

namespace {
template <typename T>
bool ParseKeyValues(const OSMTagHelper& tagh, const T& obj, POI* poi) {
  if (obj.keys().size() == 0) return false;
  CHECK_EQ_S(obj.keys().size(), obj.vals().size());
  CHECK_S(poi->type.empty());
  for (int i = 0; i < (int)obj.keys().size(); ++i) {
    std::string_view key = tagh.ToString(obj.keys().at(i));
    std::string_view val = tagh.ToString(obj.vals().at(i));
    if (key == "amenity" && val != "parking_space" && val != "bench" &&
        val != "waste_basket" && val != "clock" && val != "hunting_stand" &&
        val != "grit_bin") {
      poi->name = val;
      poi->type = key;
    }
    if (key == "shop" || key == "tourism" || key == "office") {
      poi->name = val;
      poi->type = key;
    }
    if (key == "railway" && val == "stop") {
      poi->name = "railway-stop";
      poi->type = key;
    }
    if (key == "tourism") {
      poi->name = val;
      poi->type = key;
    }
  }
  return !poi->name.empty();
}

// Add the node data if it was referenced by a way.
void AddReferencedNode(const OsmPbfReader::NodeWithTags& node, std::mutex& mut,
                       CollectedData* data) {
  // No mutex needed to access data->refnode_to_poi_idx.
  auto [iter, end] = data->refnode_to_poi_idx.equal_range(node.id());
  if (iter != end) {
    std::unique_lock<std::mutex> l(mut);
    for (; iter != end; ++iter) {
      POI& poi = data->pois.at(iter->second);
      poi.sum_lat += node.lat_;
      poi.sum_lon += node.lon_;
      poi.num_points++;
    }
  }
}

void ConsumeWayPOI(const OSMTagHelper& tagh, const OSMPBF::Way& osm_way,
                   std::mutex& mut, CollectedData* data) {
  POI poi;
  if (ParseKeyValues(tagh, osm_way, &poi)) {
    poi.id = osm_way.id();
    poi.obj_type = 'w';

    {
      std::unique_lock<std::mutex> l(mut);
      size_t poi_idx = data->pois.size();
      data->pois.push_back(poi);
      // Add the way points to the stuff we need to load.
      std::int64_t running_id = 0;
      for (int ref_idx = 0; ref_idx < osm_way.refs().size(); ++ref_idx) {
        running_id += osm_way.refs(ref_idx);
        data->refnode_to_poi_idx.insert({running_id, poi_idx});
      }
    }
  }
}

void ConsumeNodePOI(const OSMTagHelper& tagh,
                    const OsmPbfReader::NodeWithTags& node, std::mutex& mut,
                    CollectedData* data) {
  POI poi;
  if (ParseKeyValues(tagh, node, &poi)) {
    poi.id = node.id();
    poi.lat = node.lat_;
    poi.lon = node.lon_;
    poi.obj_type = 'n';

    // Not really necessary, but still...
    poi.sum_lat = node.lat_;
    poi.sum_lon = node.lon_;
    poi.num_points = 1;
    {
      std::unique_lock<std::mutex> l(mut);
      data->pois.push_back(poi);
    }
  }
  AddReferencedNode(node, mut, data);
}

void ComputeAverages(CollectedData* data) {
  for (POI& poi : data->pois) {
    if (poi.obj_type == 'w' && poi.num_points > 0) {
      poi.lat = poi.sum_lat / poi.num_points;
      poi.lon = poi.sum_lon / poi.num_points;
    } else {
      CHECK_EQ_S(poi.obj_type, 'n');
      CHECK_EQ_S(poi.lat, poi.sum_lat);
      CHECK_EQ_S(poi.lon, poi.sum_lon);
    }
  }
}
}  // namespace

void ReadPBF(const std::string& filename, int n_threads,
             pois::CollectedData* data) {
  OsmPbfReader reader(filename, n_threads);
  reader.ReadFileStructure();

  reader.ReadWays(
      [data](const OSMTagHelper& tagh, const OSMPBF::Way& way,
             std::mutex& mut) { pois::ConsumeWayPOI(tagh, way, mut, data); });

  reader.ReadNodes(
      [data](const OSMTagHelper& tagh, const OsmPbfReader::NodeWithTags& node,
             std::mutex& mut) { pois::ConsumeNodePOI(tagh, node, mut, data); });
  ComputeAverages(data);
}

}  // namespace pois