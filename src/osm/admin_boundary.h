#pragma once

// TODO: The algorithm below is much too complicated, so make it simpler. To
// create the polygons for each country border, it is enough to consecutively
// connect ways by start/end-nodes, until the polygons can be  closed, or until
// an error is found. Finding a way that connects to a specific node can simply
// be done by indexing all ways by start- and end-node and doing a lookup into
// this table.

#include <osmpbf/osmpbf.h>
#include <stdio.h>

#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <string_view>
#include <unordered_map>

#include "absl/strings/str_split.h"
#include "base/util.h"
#include "osm/osm_helpers.h"
#include "osm/read_osm_pbf.h"

struct WayData {
  int64_t id;
  std::vector<int64_t> nodes;
};

// An admin relation consists of a list of consecutive way pieces. Each way
// piece is reversed or not, i.e. from end-to-start or from start-to-end.
struct AdminRelWayPiece {
  int64_t way_id;
  WayData way_data;
  bool start = false;
  bool reversed_ = false;
  bool inner = false;
  bool error_no_reverse = false;
  bool error_no_connection = false;
  bool error_way_missing = false;

  void reverse() {
    CHECK_S(!reversed_);
    std::reverse(way_data.nodes.begin(), way_data.nodes.end());
    reversed_ = true;
  }
  int64_t first() const { return way_data.nodes.front(); }
  int64_t last() const { return way_data.nodes.back(); }
  bool error() const {
    return error_no_reverse || error_no_connection || error_way_missing;
  }
  bool may_reverse() const {
    return start || error_no_reverse | error_no_connection;
  }
};

struct AdminRelPolygon {
  std::vector<AdminRelWayPiece> pieces;
  bool closed = false;
  int32_t num_errors = 0;
  int32_t num_missing_ways = 0;
};

struct AdminRelation {
  // Relation as read from OSM. Represents a relation for a specific country,
  // given by iso_code and iso_numeric.
  int64_t id;
  std::string type;
  std::string boundary;  // must be 'administrative' for a valid country border.
  std::string name;
  std::string name_de;
  std::string iso_code;
  std::string iso_numeric;
  int admin_level;
  std::vector<int64_t> memids;
  std::vector<std::string> roles;
  std::vector<OSMPBF::Relation::MemberType> types;

  // Extracted country polygons.
  std::vector<AdminRelPolygon> polygons;
  int32_t num_good_poly = 0;
  int32_t num_bad_poly = 0;
};

struct AdminInfo {
  // Country relations read from osm pbf file.
  std::vector<AdminRelation> rels;
  // Way ids that are referenced in the relations.
  std::unordered_set<int64_t> ref_ways;
  // Referenced ways that were read from osm pbf file.
  std::unordered_map<int64_t, WayData> way_map;
  // Node ids that are referenced in the ways.
  std::unordered_set<int64_t> ref_nodes;
  // Referenced nodes that were read from osm pbf file.
  std::unordered_map<int64_t, OsmPbfReader::Node> nodes;
};

bool ExtractAdminRelation(const OSMTagHelper& tagh,
                          const OSMPBF::Relation& osm_rel, AdminRelation* rel) {
  int admin_level;
  if (!absl::SimpleAtoi(tagh.GetValue(osm_rel, tagh.admin_level_),
                        &admin_level)) {
    return false;
  }
  if (admin_level != 2) {
    return false;  // Only interested in countries so far.
  }

  const std::string_view rel_type = tagh.GetValue(osm_rel, tagh.type_);
  if (rel_type != "boundary" && rel_type != "multipolygon") {
    return false;
  }
  rel->id = osm_rel.id();
  rel->type = std::string(rel_type);
  rel->boundary = tagh.GetValue(osm_rel, tagh.boundary_);
  rel->admin_level = admin_level;
  rel->name = tagh.GetValue(osm_rel, tagh.name_);
  rel->name_de = tagh.GetValueByStr(osm_rel, "name:de");
  if (rel->name_de.empty()) {
    rel->name_de = tagh.GetValueByStr(osm_rel, "name:en");
  }
  rel->iso_code = tagh.GetValueByStr(osm_rel, "ISO3166-1:alpha2");
  if (rel->iso_code.empty()) {
    rel->iso_code = tagh.GetValueByStr(osm_rel, "ISO3166-1");
  }
  if (rel->iso_code.empty()) {
    rel->iso_code = tagh.GetValueByStr(osm_rel, "country_code");
  }
  rel->iso_numeric = tagh.GetValueByStr(osm_rel, "ISO3166-1:numeric");

  if (rel->type != "boundary" || rel->boundary != "administrative" ||
      rel->iso_code.empty()) {
    LOG_S(INFO) << absl::StrFormat(
        "Ignore admin relation %lld type:<%s> name_de:<%s> level:%d iso:<%s> "
        "boundary:<%s>",
        rel->id, rel->type.c_str(), rel->name_de.c_str(), rel->admin_level,
        rel->iso_code.c_str(), rel->boundary.c_str());
    return false;
  }

  rel->memids.reserve(osm_rel.memids().size());
  rel->roles.reserve(osm_rel.memids().size());
  rel->types.reserve(osm_rel.memids().size());
  std::int64_t running_id = 0;
  for (int i = 0; i < osm_rel.memids().size(); ++i) {
    running_id += osm_rel.memids(i);
    rel->memids.push_back(running_id);
    rel->roles.emplace_back(tagh.ToString(osm_rel.roles_sid(i)));
    rel->types.push_back(osm_rel.types(i));
  }
  return true;
}

void StoreAdminWayRefs(AdminInfo* info) {
  for (const AdminRelation& rel : info->rels) {
    if (rel.admin_level == 2) {
      LOG_S(INFO) << absl::StrFormat(
          "Store way refs for rel %lld type:<%s> name_de:<%s> level:%d "
          "iso:<%s> boundary:<%s>",
          rel.id, rel.type.c_str(), rel.name_de.c_str(), rel.admin_level,
          rel.iso_code.c_str(), rel.boundary.c_str());
      for (size_t i = 0; i < rel.memids.size(); ++i) {
        if (rel.types.at(i) == OSMPBF::Relation::RELATION) {
          if (rel.roles.at(i) != "subarea") {
            LOG_S(WARNING) << absl::StrFormat("Subrel %lld with role %s",
                                              rel.memids.at(i),
                                              rel.roles.at(i).c_str());
          }
        } else if (rel.types.at(i) == OSMPBF::Relation::WAY) {
          if (rel.roles.at(i) != "inner" && rel.roles.at(i) != "outer") {
            LOG_S(WARNING) << absl::StrFormat("Subway %lld with role %s",
                                              rel.memids.at(i),
                                              rel.roles.at(i).c_str());
          } else {
            info->ref_ways.insert(rel.memids.at(i));
          }
        } else {
          assert(rel.types.at(i) == OSMPBF::Relation::NODE);
          if (rel.roles.at(i) != "admin_centre" && rel.roles.at(i) != "label") {
            LOG_S(WARNING) << absl::StrFormat("Subnode %lld with role %s",
                                              rel.memids.at(i),
                                              rel.roles.at(i).c_str());
          }
        }
      }
    }
  }
}

namespace {
void GetWayPieces(std::unordered_map<int64_t, WayData> ways_map,
                  const AdminRelation& rel,
                  std::vector<AdminRelWayPiece>* pieces) {
  for (size_t i = 0; i < rel.memids.size(); ++i) {
    if (rel.types.at(i) != OSMPBF::Relation::WAY ||
        (rel.roles.at(i) != "inner" && rel.roles.at(i) != "outer")) {
      continue;
    }
    pieces->emplace_back();
    AdminRelWayPiece& piece = pieces->back();
    piece.way_id = rel.memids.at(i);
    piece.inner = (rel.roles.at(i) == "inner");
    auto search = ways_map.find(piece.way_id);
    piece.error_way_missing = (search == ways_map.end());
    if (!piece.error_way_missing) {
      piece.way_data = search->second;
    }
  }
}

bool CanConnectTo(const AdminRelWayPiece& p1, const AdminRelWayPiece& p2) {
  return !p2.error_way_missing &&
         (p1.first() == p2.first() || p1.first() == p2.last() ||
          p1.last() == p2.first() || p1.last() == p2.last());
}

void FixWayPieceOrdering(const AdminRelation& rel,
                         std::vector<AdminRelWayPiece>* pieces) {
  for (size_t pos = 0; pos < pieces->size() - 1; pos++) {
    const AdminRelWayPiece& p = pieces->at(pos);
    if (p.error_way_missing || p.first() == p.last()) {
      continue;
    }
    // Check if the current piece can be connected with the next piece.
    if (CanConnectTo(p, pieces->at(pos + 1))) {
      continue;
    }
    // Find a piece that connects and swap.
    for (size_t other = pos + 2; other < pieces->size(); other++) {
      if (CanConnectTo(p, pieces->at(other))) {
        auto tmp = pieces->at(pos + 1);
        pieces->at(pos + 1) = pieces->at(other);
        pieces->at(other) = tmp;
        LOG_S(INFO) << absl::StrFormat(
            "  Swap: rel <%s> %lld way %lld with %lld", rel.name_de.c_str(),
            rel.id, pieces->at(pos).way_id, pieces->at(other).way_id);
        break;
      }
    }
  }
}

void UpdateRelationStats(AdminRelation* rel) {
  for (AdminRelPolygon& poly : rel->polygons) {
    for (const AdminRelWayPiece& piece : poly.pieces) {
      poly.num_errors += piece.error() ? 1 : 0;
      poly.num_missing_ways += piece.error_way_missing ? 1 : 0;
    }
    if (poly.num_errors == 0 && poly.closed) {
      rel->num_good_poly++;
    } else {
      rel->num_bad_poly++;
    }
  }
}

}  // namespace

void StitchCountryBorders(const std::unordered_map<int64_t, WayData>& way_map,
                          AdminRelation* rel) {
  if (rel->type != "boundary" || rel->boundary != "administrative" ||
      rel->iso_code.empty() || rel->admin_level != 2) {
    LOG_S(INFO) << absl::StrFormat(
        "Ignore admin relation %lld type:<%s> name_de:<%s> level:%d iso:<%s> "
        "boundary:<%s>",
        rel->id, rel->type.c_str(), rel->name_de.c_str(), rel->admin_level,
        rel->iso_code.c_str(), rel->boundary.c_str());
    return;
  }

  LOG_S(INFO) << absl::StrFormat(
      "Stitch admin relation %lld type:<%s> name_de:<%s> level:%d iso:<%s> "
      "boundary:<%s>",
      rel->id, rel->type.c_str(), rel->name_de.c_str(), rel->admin_level,
      rel->iso_code.c_str(), rel->boundary.c_str());

  std::vector<AdminRelWayPiece> pieces;
  GetWayPieces(way_map, *rel, &pieces);
  FixWayPieceOrdering(*rel, &pieces);

  AdminRelPolygon* poly = nullptr;
  for (const AdminRelWayPiece& tmp : pieces) {
    if (poly == nullptr || poly->closed) {
      // Start a new polygon.
      rel->polygons.emplace_back();
      poly = &rel->polygons.back();
    }
    poly->pieces.push_back(tmp);
    AdminRelWayPiece& piece = poly->pieces.back();
    if (piece.error_way_missing) {
      LOG_S(WARNING) << "  *** Can't find way" << piece.way_id;
      continue;
    }

    // First piece (or outer/inner switch)?
    if (poly->pieces.size() == 1 ||
        piece.inner != poly->pieces.at(poly->pieces.size() - 2).inner) {
      // First piece, just leave it as is.
      // TODO: handle case of a relation containing only one closed way.
      piece.start = true;
      if (piece.first() == piece.last()) {
        poly->closed = true;
      }
      LOG_S(INFO) << absl::StrFormat(
          "  Rel <%s> way %9lld %s start:%lld end:%lld ---- start %s",
          rel->name_de.c_str(), piece.way_id, piece.inner ? "inner" : "outer",
          piece.way_data.nodes.front(), piece.way_data.nodes.back(),
          poly->closed ? "---- closed" : "");
    } else {
      AdminRelWayPiece& prev = poly->pieces.at(poly->pieces.size() - 2);
      if (prev.error_way_missing) {
        piece.error_no_connection = true;
      } else if (piece.first() == prev.last()) {
        ;
      } else if (piece.first() == prev.first()) {
        if (prev.may_reverse()) {
          prev.reverse();
        } else {
          piece.error_no_reverse = true;
        }
      } else if (piece.last() == prev.last()) {
        piece.reverse();
      } else if (piece.last() == prev.first()) {
        if (prev.may_reverse()) {
          prev.reverse();
          piece.reverse();
        } else {
          piece.error_no_reverse = true;
        }
      } else {
        // No connection point found.
        piece.error_no_connection = true;
      }
      if (!poly->pieces.front().error()) {
        if (piece.last() == poly->pieces.front().first()) {
          poly->closed = true;
        } else if (piece.error() &&
                   piece.last() == poly->pieces.front().last()) {
          poly->closed = true;
        }
      }
      LOG_S(INFO) << absl::StrFormat(
          "  Rel <%s> way %9lld %s start:%lld end:%lld %s-%s %s%s",
          rel->name_de.c_str(), piece.way_id, piece.inner ? "inner" : "outer",
          piece.way_data.nodes.front(), piece.way_data.nodes.back(),
          prev.reversed_ ? "rev" : "nor", piece.reversed_ ? "rev" : "nor",
          piece.error_no_connection
              ? "**** no conn"
              : (piece.error_no_reverse ? "**** no reverse" : ""),
          poly->closed ? "---- closed" : "");
    }
  }
  UpdateRelationStats(rel);
}
