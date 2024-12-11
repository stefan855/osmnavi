#include <osmpbf/osmpbf.h>

#include <map>
#include <string>

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
  // Number of of top values we want to keep for each key. 0 turns value stats
  // off.
  size_t n_values;
  std::map<std::string, FrequencyTable> matched_key_values;
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
  for (int i = 0; i < (int)obj.keys().size(); ++i) {
    if (OneTagMatch(filters, absl::StrCat(tagh.ToString(obj.keys().at(i)), "=",
                                          tagh.ToString(obj.vals().at(i))))) {
      match = true;
      std::unique_lock<std::mutex> l(mut);
      const std::string& key = tagh.ToString(obj.keys().at(i));
      stats->matched_keys.Add(key, obj.id());
      if (stats->n_values > 0) {
        (stats->matched_key_values)[key].Add(tagh.ToString(obj.vals().at(i)),
                                             obj.id());
      }
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
  const FrequencyTable& key_ft = stats.matched_keys;
  const auto keys = key_ft.GetSortedElements();

  RAW_LOG_F(INFO, absl::StrFormat("======================== Matched Keys in %s "
                                  "======================== ",
                                  name)
                      .c_str());
  RAW_LOG_F(INFO, absl::StrFormat("unique: %d total: %d (%.3f%%)",
                                  key_ft.TotalUnique(), key_ft.Total(),
                                  (100.0 * key_ft.TotalUnique()) /
                                      (std::max(key_ft.Total(), (uint64_t)1)))
                      .c_str());
  for (const auto& e : keys) {
    RAW_LOG_F(INFO, absl::StrFormat("=== %s %7d (id:%d)", e.key, e.ref_count,
                                    e.example_id)
                        .c_str());
    if (stats.n_values > 0) {
      auto iter = stats.matched_key_values.find(e.key);
      CHECK_NE_F(iter, stats.matched_key_values.end());
      const auto vals = iter->second.GetSortedElements();
      const size_t max_valkey_size = std::min(
          (size_t)30, FrequencyTable::MaxKeySize(vals, stats.n_values));
      for (size_t i = 0; i < vals.size(); ++i) {
        if (i >= stats.n_values) break;
        const FrequencyTable::Entry& vale = vals.at(i);
        RAW_LOG_F(INFO, absl::StrFormat("    %s %7d (id:%d)",
                                        PadString(vale.key, max_valkey_size),
                                        vale.ref_count, vale.example_id)
                            .c_str());
      }
    }
  }
}

}  // namespace

int main(int argc, char* argv[]) {
  InitLogging(argc, argv);
  FUNC_TIMER();

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
           .desc =
               "Select ways that should be logged. Syntax is "
               "<regexp>{&&[!]<regexp>}."
               "<regexp> is a regular expression that needs to match a "
               "substring in one of the tags expressed as \"<key>=<value>\". "
               "Additional regexps can be and-ed with '&&', they have to match "
               "the same tag as the first expression. The additional regexps "
               "can be individually negated with \"!\"."},
          {.name = "relations",
           .type = "string",
           .desc = "Select relations that should be logged. For syntax see "
                   "--way_filter"},
          {.name = "nodes",
           .type = "string",
           .desc = "Select nodes that should be logged. For syntax see "
                   "--way_filter"},
          {.name = "stats",
           .type = "bool",
           .dflt = "true",
           .desc = "Collect and print tag stats"},
          {.name = "n_values",
           .type = "int",
           .dflt = "10",
           .desc = "Print the top <n> values for each key."},
          {.name = "raw",
           .type = "bool",
           .dflt = "true",
           .desc = "Print each result object that matches"},
          {.name = "one_line",
           .type = "bool",
           .desc = "Print each result object way/relation on one line"},
          {.name = "n_threads",
           .type = "int",
           .dflt = "22",
           .desc = "Number of threads to use for parallel processing"},
      });

  const std::string in_bpf = argli.GetString("pbf");
  std::vector<FilterExp> way_filter =
      ParseMatchFilters(argli.GetString("ways"));
  std::vector<FilterExp> relation_filter =
      ParseMatchFilters(argli.GetString("relations"));
  std::vector<FilterExp> node_filter =
      ParseMatchFilters(argli.GetString("nodes"));
  bool print_raw = argli.GetBool("raw");
  bool print_stats = argli.GetBool("stats");
  bool print_one_line = argli.GetBool("one_line");
  size_t n_values = static_cast<size_t>(argli.GetInt("n_values"));

  OsmPbfReader reader(in_bpf, argli.GetInt("n_threads"));
  reader.ReadFileStructure();

  uint64_t num_ways = 0;
  uint64_t num_relations = 0;
  uint64_t num_nodes = 0;

  if (!way_filter.empty()) {
    FreqStats stats = {.n_values = n_values};
    reader.ReadWays(
        [&way_filter, &num_ways, print_raw, print_one_line, &stats](
            const OSMTagHelper& tagh, const OSMPBF::Way& way, std::mutex& mut) {
          if (MatchFilters(tagh, way, way_filter, mut, &stats)) {
            num_ways++;
            if (print_raw) {
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
    FreqStats stats = {.n_values = n_values};
    reader.ReadRelations([&relation_filter, &num_relations, print_raw,
                          print_one_line, &stats](const OSMTagHelper& tagh,
                                                  const OSMPBF::Relation& rel,
                                                  std::mutex& mut) {
      if (MatchFilters(tagh, rel, relation_filter, mut, &stats)) {
        num_relations++;
        if (print_raw) {
          RAW_LOG_F(INFO, "Relation:\n%s\n",
                    tagh.GetLoggingStr(rel, print_one_line).c_str());
        }
      }
    });
    if (print_stats) {
      PrintStats("Relations", stats);
    }
  }

  if (!node_filter.empty()) {
    FreqStats stats = {.n_values = n_values};
    reader.ReadNodes([&node_filter, &num_nodes, print_raw, print_one_line,
                      &stats](const OSMTagHelper& tagh,
                              const OsmPbfReader::NodeWithTags& node,
                              std::mutex& mut) {
      /*
        LOG_S(INFO) << "Node:" << node.id();
        for (size_t i = 0; i < node.keys().size(); ++i) {
          LOG_S(INFO) << absl::StrFormat(
              "%s=%s", tagh.ToString(node.keys().at(i)),
              tagh.ToString(node.vals().at(i)));
        }
        */
      if (MatchFilters(tagh, node, node_filter, mut, &stats)) {
        num_nodes++;
        if (print_raw) {
          RAW_LOG_F(INFO, "Node:\n%s\n",
                    tagh.GetLoggingStr(node, print_one_line).c_str());
        }
      }
    });
    if (print_stats) {
      PrintStats("Relations", stats);
    }
  }

  LOG_S(INFO) << absl::StrFormat(
      "Finished (matched %u ways, %u relations %u nodes)", num_ways,
      num_relations, num_nodes);
  return 0;
}
