#include <osmpbf/osmpbf.h>

#include "absl/strings/str_format.h"
#include "base/argli.h"
#include "base/util.h"
#include "osm/osm_helpers.h"
#include "osm/read_osm_pbf.h"

int main(int argc, char* argv[]) {
  InitLogging(argc, argv);
  FuncTimer timer("main()");

  Argli argli(
      argc, argv,
      {
          {.name = "pbf",
           .type = "string",
           .positional = true,
           .required = true,
           .desc = "Input OSM pbf file (such as planet file)."},
          {.name = "way_filter",
           .type = "string",
           .desc =
               "Filter ways that should be logged. Syntax is "
               "[-][(key|val|keyval):]<regexp>."
               " \"-\" inverts the result of the match, key|val|keyval selects "
               "the part of the tag to match (default is keyval), and <regexp> "
               "is a regular expression that needs to match a substring. "
               "Multiple such expressions can be or-ed with \"|\""},
          {.name = "relation_filter",
           .type = "string",
           .desc = "Filter relations that should be logged. For syntax see "
                   "--way_filter"},
      });

  const std::string in_bpf = argli.GetString("pbf");
  std::vector<OSMTagHelper::FilterExp> way_filter =
      OSMTagHelper::ParseMatchFilters(argli.GetString("way_filter"));
  std::vector<OSMTagHelper::FilterExp> relation_filter =
      OSMTagHelper::ParseMatchFilters(argli.GetString("relation_filter"));

  OsmPbfReader reader(in_bpf, 16);
  reader.ReadFileStructure();

  uint64_t num_ways = 0;
  uint64_t num_relations = 0;

  if (!way_filter.empty()) {
    reader.ReadWays([&way_filter, &num_ways](const OSMTagHelper& tagh,
                                             const OSMPBF::Way& way,
                                             std::mutex& mut) {
      if (tagh.MatchFilters(way, way_filter)) {
        num_ways++;
        RAW_LOG_F(INFO, "Way:%ld\n%s\n", way.id(),
                  tagh.GetLoggingStr(way).c_str());
      }
    });
  }

  if (!relation_filter.empty()) {
    reader.ReadRelations([&relation_filter, &num_relations](
                             const OSMTagHelper& tagh,
                             const OSMPBF::Relation& rel, std::mutex& mut) {
      if (tagh.MatchFilters(rel, relation_filter)) {
        num_relations++;
        RAW_LOG_F(INFO, "Relation:%ld\n%s\n", rel.id(),
                  tagh.GetLoggingStr(rel).c_str());
      }
    });
  }

  LOG_S(INFO) << absl::StrFormat("Finished (%u ways %u relations)", num_ways,
                                 num_relations);
  return 0;
}
