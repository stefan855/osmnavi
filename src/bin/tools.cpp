#include <osmpbf/osmpbf.h>

#include <filesystem>
#include <map>

#include "absl/strings/str_format.h"
#include "base/argli.h"
#include "base/util.h"
#include "graph/build_graph.h"
#include "osm/poi.h"
#include "osm/read_osm_pbf.h"

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
                  {.name = "command",
                   .type = "string",
                   .positional = true,
                   .required = true,
                   .desc = "Command to execute, possible values are \"pois\""},
                  {.name = "n_threads",
                   .type = "int",
                   .dflt = "6",
                   .desc = "Number of threads to use"},
              });

  const std::string pbf = argli.GetString("pbf");
  const std::string command = argli.GetString("command");
  CHECK_EQ_S(command, "pois");

  // Read POIs.
  pois::CollectedData data;
  pois::ReadPBF(pbf, argli.GetInt("n_threads"), &data);

  // Read Road Network.
  build_graph::BuildGraphOptions opt = {.pbf_filename = pbf};
  build_graph::GraphMetaData meta = build_graph::BuildGraph(opt);

  // Execute random queries and collect traffic.
  // TODO!

  /*
  for (size_t i = 0; i < data.pois.size(); ++i) {
    const pois::POI& p = data.pois.at(i);
    LOG_S(INFO) << absl::StrFormat("%10u. %c %10d lat:%d lon:%d #n:%d %s %s", i,
                                   p.obj_type, p.id, p.lat, p.lon, p.num_points,
                                   p.type, p.name);
  }
  */

  LOG_S(INFO) << "Finished.";
  return 0;
}
