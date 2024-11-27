// Extract country polygons to /tmp/admin/ directory:
//   ./extract_admin ../../data/planet-240408.osm.pbf /tmp/admin
// You will find files with "err" in the name if there are errors (and the total
// number of errors is reported in this end). The files are in .csv format.
//
// Afterwards, use class TiledCountryLookup to perform fast country lookups for
// coordinates:
//   TiledCountryLookup tiler("/tmp/admin");
//   uint16_t ncc = tiler.GetCountryNum(lon, lat);  // units of 10^-7 degrees
// CountryNumToString(ncc) returns the two letter country code such as "CH".
#include <osmpbf/osmpbf.h>

#include <filesystem>
#include <map>

#include "absl/strings/str_format.h"
#include "base/argli.h"
#include "base/util.h"
#include "osm/admin_boundary.h"
#include "osm/read_osm_pbf.h"

namespace {
OsmPbfReader::Node FindNode(
    const std::unordered_map<int64_t, OsmPbfReader::Node>& nodes, int64_t id) {
  auto search = nodes.find(id);
  CHECK_S(search != nodes.end());
  return search->second;
}

int64_t WritePolyPoints(const AdminInfo& info, const AdminRelWayPiece& piece,
                        std::ofstream* myfile) {
  int64_t count = 0;
  const auto& nodes = piece.way_data.nodes;
  // Omit the first node if not the first piece.
  for (size_t i = piece.start ? 0 : 1; i < nodes.size(); ++i) {
    count++;
    const OsmPbfReader::Node n = FindNode(info.nodes, nodes.at(i));
    *myfile << absl::StrFormat("%s,%s,%d,%d\n", i > 0 ? "pt" : "poly-start",
                               piece.inner ? "red" : "black", n.lat, n.lon);
  }
  return count;
}

void WritePolygon(const AdminInfo& info,
                  const std::filesystem::path& output_dir,
                  const AdminRelation& rel, const AdminRelPolygon& poly,
                  bool error, int serial) {
  const bool inner = poly.pieces.front().inner;
  char driving_side_letter = 'X';
  if (rel.driving_side == "right") {
    driving_side_letter = 'R';
  } else if (rel.driving_side == "left") {
    driving_side_letter = 'L';
  }

  const std::string filename =
      output_dir / absl::StrFormat("%s_%s_%04d_%c_%c%s.csv", rel.iso_code,
                                   rel.iso_numeric, serial, inner ? 'I' : 'O',
                                   driving_side_letter, error ? "_err" : "");

  LOG_S(INFO) << "Write country borders to " << filename;
  std::ofstream myfile;
  myfile.open(filename, std::ios::trunc | std::ios::binary | std::ios::out);

  int64_t count = 0;
  if (!error) {
    for (const AdminRelWayPiece& piece : poly.pieces) {
      CHECK_EQ_S(piece.inner, inner);
      count += WritePolyPoints(info, piece, &myfile);
    }
  }
  myfile.close();
  LOG_S(INFO) << absl::StrFormat("Written %lld lines to %s", count,
                                 filename.c_str());
}

void ConsumeRelation(const OSMTagHelper& tagh, const OSMPBF::Relation& osm_rel,
                     std::mutex& mut, AdminInfo* admin_info) {
  AdminRelation rel;
  if (ExtractAdminRelation(tagh, osm_rel, &rel)) {
    std::unique_lock<std::mutex> l(mut);
    admin_info->rels.push_back(rel);
  }
}

void ConsumeWay(const OSMTagHelper& tagh, const OSMPBF::Way& osm_way,
                std::mutex& mut, AdminInfo* admin_info) {
  if (admin_info->ref_ways.contains(osm_way.id())) {
    std::unique_lock<std::mutex> l(mut);
    WayData& way_data = admin_info->way_map[osm_way.id()];
    CHECK_S(way_data.nodes.empty());  // Should see each way only once.
    way_data.id = osm_way.id();
    std::uint64_t running_node_id = 0;
    for (int64_t ref : osm_way.refs()) {
      running_node_id += ref;
      admin_info->ref_nodes.insert(running_node_id);
      way_data.nodes.push_back(running_node_id);
    }
  }
}

void ConsumeNode(const OsmPbfReader::NodeWithTags& node, std::mutex& mut,
                 AdminInfo* admin_info) {
  if (admin_info->ref_nodes.contains(node.id())) {
    std::unique_lock<std::mutex> l(mut);
    admin_info->nodes[node.id()] = {
        .id = node.id(), .lat = node.lat_, .lon = node.lon_};
  }
}

void ReadData(const std::string& filename, int n_threads,
              AdminInfo& admin_info) {
  OsmPbfReader reader(filename, n_threads);
  reader.ReadFileStructure();
  reader.ReadRelations([&admin_info](const OSMTagHelper& tagh,
                                     const OSMPBF::Relation& osm_rel,
                                     std::mutex& mut) {
    ConsumeRelation(tagh, osm_rel, mut, &admin_info);
  });
  StoreAdminWayRefs(&admin_info);

  reader.ReadWays([&admin_info](const OSMTagHelper& tagh,
                                const OSMPBF::Way& way, std::mutex& mut) {
    ConsumeWay(tagh, way, mut, &admin_info);
  });

  reader.ReadNodes(
      [&admin_info](const OSMTagHelper& tagh,
                    const OsmPbfReader::NodeWithTags& node,
                    std::mutex& mut) { ConsumeNode(node, mut, &admin_info); });
}

}  // namespace

int main(int argc, char* argv[]) {
  InitLogging(argc, argv);
  FUNC_TIMER();

  Argli argli(argc, argv,
              {
                  {.name = "pbf",
                   .type = "string",
                   .positional = true,
                   .required = true,
                   .desc = "Input OSM pbf file (such as planet file)."},
                  {.name = "output_dir",
                   .type = "string",
                   .positional = true,
                   .dflt = "/tmp/admin/",
                   .desc = "Directory where output admin files are stored. "
                           "Existing files are overwritten."},
              });

  const std::string in_bpf = argli.GetString("pbf");
  const std::filesystem::path output_dir = argli.GetString("output_dir");
  std::filesystem::create_directories(output_dir);

  AdminInfo admin_info;
  ReadData(in_bpf, /*n_threads="*/ 18, admin_info);

  for (AdminRelation& rel : admin_info.rels) {
    StitchCountryBorders(admin_info.way_map, &rel);
  }
  for (AdminRelation& rel : admin_info.rels) {
    LOG_S(INFO) << absl::StrFormat(
        "Eval admin relation %lld type:<%s> name_de:<%s> level:%d iso:<%s><%s> "
        "boundary:<%s> good:%d bad:%d",
        rel.id, rel.type.c_str(), rel.name_de.c_str(), rel.admin_level,
        rel.iso_code.c_str(), rel.iso_numeric.c_str(), rel.boundary.c_str(),
        rel.num_good_poly, rel.num_bad_poly);
  }

  LOG_S(INFO) << absl::StrFormat(
      "Loaded rels:%lld ref-ways:%lld ways:%lld ref-nodes:%lld nodes:%lld",
      admin_info.rels.size(), admin_info.ref_ways.size(),
      admin_info.way_map.size(), admin_info.ref_nodes.size(),
      admin_info.nodes.size());
  CHECK_EQ_S(admin_info.ref_nodes.size(), admin_info.nodes.size())
      << "missing nodes";

  std::map<int64_t, int64_t> stats;
  int count = 0;
  int errors = 0;
  for (const AdminRelation& rel : admin_info.rels) {
    int serial = 0;
    for (const AdminRelPolygon& poly : rel.polygons) {
      const bool error = poly.num_errors > 0 || !poly.closed;
      count += 1;
      errors += (error ? 1 : 0);
      WritePolygon(admin_info, output_dir, rel, poly, error, serial++);
    }
  }

  LOG_S(INFO) << "Written " << count << " polygon files with " << errors
              << " error(s)";
  LOG_S(INFO) << "Finished.";
  return 0;
}
