#include <osmpbf/osmpbf.h>

#include "absl/strings/str_format.h"
#include "base/argli.h"
#include "base/frequency_table.h"
#include "base/util.h"
#include "osm/osm_helpers.h"
#include "osm/read_osm_pbf.h"

/*
 * Use cases for conditions:
 * Way contains a tag highway=path and no tag with surface.
 *    -ways=highway=path||-surface ???
 * Way has tag highway=motorway and another tag lanes=8
 * Way contains a tag with surface. The same tag contains also cycle.
 *    -ways=surfac&&cycle
 */

namespace {
struct FilterExp {
  // When true, then 're' has to match at least once, if false, then it must
  // never match.
  bool negative;
  std::regex re;
};

struct FreqStats {
  FrequencyTable matched_keys;
  FrequencyTable matched_key_values;
};

std::vector<FilterExp> ParseMatchFilters(std::string_view expression) {
  std::vector<FilterExp> res;
  for (std::string_view str :
       absl::StrSplit(expression, "&&", absl::SkipEmpty())) {
    FilterExp exp;
    exp.negative = ConsumePrefixIf("!", &str);
    CHECK_S(!str.empty()) << "Empty regexp not allowed";
    CHECK_S(!(exp.negative && res.empty()))
        << "First expression can't be negated with '!'";
    exp.re = std::regex(std::string(str), std::regex_constants::icase);
    res.push_back(exp);
  }
  return res;
}

// Return true if all filters match tag, else return false.
bool OneTagMatch(const std::vector<FilterExp>& filters,
                 const std::string& tag) {
  for (const FilterExp& f : filters) {
    if (std::regex_search(tag, f.re) == f.negative) {
      return false;
    }
  }
  return true;
}

// Return true if at least one tag of obj (way or relation) matches all filters
// in filters, else return false.
template <typename T>
bool MatchFilters(const OSMTagHelper& tagh, const T& obj,
                  const std::vector<FilterExp>& filters, std::mutex& mut,
                  FreqStats* stats) {
  if (filters.empty() || obj.keys().empty()) {
    return false;
  }

  bool match = false;
  for (int i = 0; i < obj.keys().size(); ++i) {
    if (OneTagMatch(filters, absl::StrCat(tagh.ToString(obj.keys(i)), "=",
                                          tagh.ToString(obj.vals(i))))) {
      match = true;
      std::unique_lock<std::mutex> l(mut);
      stats->matched_keys.Add(tagh.ToString(obj.keys(i)), obj.id());
    }
  }

  if (OneTagMatch(filters, absl::StrCat("id=", obj.id()))) {
    match = true;
    std::unique_lock<std::mutex> l(mut);
    stats->matched_keys.Add("id", obj.id());
  }
  return match;
}

void PrintStats(std::string_view name, const FreqStats& stats) {
  const FrequencyTable& ft = stats.matched_keys;

  LOG_S(INFO) << "======================== Matched Keys in " << name
              << " ======================== ";
  LOG_S(INFO) << absl::StrFormat(
      "unique: %d total: %d (%.3f%%)", ft.TotalUnique(), ft.Total(),
      (100.0 * ft.TotalUnique()) / (std::max(ft.Total(), (uint64_t)1)));
  for (const auto& e : ft.GetSortedElements()) {
    LOG_S(INFO) << absl::StrFormat("%-50s: %12d (id:%d)", e.key, e.ref_count,
                                   e.example_id);
  }
}

}  // namespace

int main(int argc, char* argv[]) {
  InitLogging(argc, argv);
  FUNC_TIMER();
  // FUNC_TIMER2();

  Argli argli(
      argc, argv,
      {
          {.name = "pbf",
           .type = "string",
           .positional = true,
           .required = true,
           .desc = "Input OSM pbf file (such as planet file)."},
          {.name = "ways",
           .type = "string",
           .desc = "Select ways that should be logged. Syntax is "
                   "<regexp>{&&[!]<regexp>}."
                   "<regexp> is a regular expression that needs to match a "
                   "substring in one "
                   "of the tags expressed as \"<key>=<value>\". Additional "
                   "regexps can be and-ed with '&&', they have to match "
                   "the same tag as the first expression. The additional can "
                   "be negated with \"!\"."},
          {.name = "relations",
           .type = "string",
           .desc = "Select relations that should be logged. For syntax see "
                   "--way_filter"},
          {.name = "entries",
           .type = "bool",
           .dflt = "true",
           .desc = "Print each result object that matches"},
          {.name = "stats",
           .type = "bool",
           .dflt = "true",
           .desc = "Collect and print tag stats"},
          {.name = "one_line",
           .type = "bool",
           .desc = "Print each result object way/relation on one line"},
      });

  const std::string in_bpf = argli.GetString("pbf");
  std::vector<FilterExp> way_filter =
      ParseMatchFilters(argli.GetString("ways"));
  std::vector<FilterExp> relation_filter =
      ParseMatchFilters(argli.GetString("relations"));
  bool print_entries = argli.GetBool("entries");
  bool print_stats = argli.GetBool("stats");
  bool print_one_line = argli.GetBool("one_line");

  OsmPbfReader reader(in_bpf, 16);
  reader.ReadFileStructure();

  uint64_t num_ways = 0;
  uint64_t num_relations = 0;

  if (!way_filter.empty()) {
    FreqStats stats;
    reader.ReadWays(
        [&way_filter, &num_ways, print_entries, print_one_line, &stats](
            const OSMTagHelper& tagh, const OSMPBF::Way& way, std::mutex& mut) {
          if (MatchFilters(tagh, way, way_filter, mut, &stats)) {
            num_ways++;
            if (print_entries) {
              RAW_LOG_F(INFO, "Way:\n%s\n",
                        tagh.GetLoggingStr(way, print_one_line).c_str());
            }
          }
        });
    if (print_stats) {
      PrintStats("Ways", stats);
    }
  }

  if (!relation_filter.empty()) {
    FreqStats stats;
    reader.ReadRelations([&relation_filter, &num_relations, print_entries,
                          print_one_line, &stats](const OSMTagHelper& tagh,
                                            const OSMPBF::Relation& rel,
                                            std::mutex& mut) {
      if (MatchFilters(tagh, rel, relation_filter, mut, &stats)) {
        num_relations++;
        RAW_LOG_F(INFO, "Relation:\n%s\n",
                  tagh.GetLoggingStr(rel, print_one_line).c_str());
      }
    });
  }

  LOG_S(INFO) << absl::StrFormat("Finished (%u ways %u relations)", num_ways,
                                 num_relations);
  return 0;
}
