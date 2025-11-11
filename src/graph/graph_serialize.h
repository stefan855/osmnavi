#pragma once

#include "base/util.h"
#include "base/varbyte.h"
#include "graph/graph_def.h"
#include "logging/loguru.h"

/*
 * File Layout:
 *
 * Bytes (from 0)                   Content
 * ---------------------------------------------------------------
 * SER_GRAPH_FILE_HEADER_SIZE       Magic, Version, HeaderLength.
 * FileMetaLength                   FileMeta data.
 * remaining bytes                  Data according to FileMeta
 * ---------------------------------------------------------------
 */

// The first value (varint encoded) in every serialized graph file.
#define SER_GRAPH_MAGIC_NUM 224'681'459'823'456'897ull
// The second value (varint encoded) in every serialized graph file.
#define SER_GRAPH_VERSION_NUM 1ull
#define SER_GRAPH_FILE_HEADER_SIZE (3 * max_varint_bytes)

// A mask that has the least significant 'bitnum' bits set to 1 and everything
// else set to 0.
#define BIT_MASK(bitnum) ((1llu << bitnum) - 1)

#define DECODE_UINT(cnt, ptr, dest)      \
  {                                      \
    uint64_t utmp;                       \
    cnt += DecodeUInt(ptr + cnt, &utmp); \
    dest = utmp;                         \
  }

#define DECODE_INT_DIFF(cnt, ptr, prev, dest) \
  {                                           \
    int64_t tmp;                              \
    cnt += DecodeInt(ptr + cnt, &tmp);        \
    dest = prev + tmp;                        \
  }

// Store the last 'num' bits of bitset in 'dest' and remove them from bitset by
// using right-shift.
#define DECODE_BITS(bitset, num, dest)                                \
  {                                                                   \
    dest = static_cast<decltype(dest)>(bitset & ((1llu << num) - 1)); \
    bitset = bitset >> num;                                           \
  }

inline void EncodeGNode(const GNode& base, const GNode& n, WriteBuff* buff) {
  /*
  std::int64_t node_id : GNODE_ID_BITS;
  std::uint32_t cluster_id : NUM_CLUSTER_BITS = INVALID_CLUSTER_ID;
  std::uint64_t edges_start_pos : 36;
  std::uint32_t num_forward_edges : NUM_EDGES_OUT_BITS;
  std::uint16_t ncc : 10 = INVALID_NCC;
  std::int32_t lat = 0;
  std::int32_t lon = 0;
  */
  EncodeInt(n.node_id - base.node_id, buff);
  EncodeUInt(n.cluster_id, buff);
  EncodeInt(n.edges_start_pos - base.edges_start_pos, buff);
  EncodeUInt(n.num_forward_edges, buff);
  EncodeUInt(n.ncc, buff);
  EncodeInt(n.lat, buff);
  EncodeInt(n.lon, buff);
  /*
  ** bit std::uint32_t large_component : 1;
  ** bit std::uint32_t cluster_border_node : 1 = 0;
  ** bit std::uint32_t dead_end : 1;
  ** bit std::uint32_t is_pedestrian_crossing : 1;
  */
  EncodeUInt((n.large_component << 3) + (n.cluster_border_node << 2) +
                 (n.dead_end << 1) + n.is_pedestrian_crossing,
             buff);
}

inline uint32_t DecodeGNode(const GNode& base, const std::uint8_t* ptr,
                            GNode* n) {
  uint32_t cnt = 0;
  int64_t tmp;
  uint64_t utmp;

  cnt += DecodeInt(ptr + cnt, &tmp);
  n->node_id = base.node_id + tmp;

  cnt += DecodeUInt(ptr + cnt, &utmp);
  n->cluster_id = utmp;

  cnt += DecodeInt(ptr + cnt, &tmp);
  n->edges_start_pos = base.edges_start_pos + tmp;

  cnt += DecodeUInt(ptr + cnt, &utmp);
  n->num_forward_edges = utmp;

  cnt += DecodeUInt(ptr + cnt, &utmp);
  n->ncc = utmp;

  cnt += DecodeInt(ptr + cnt, &tmp);
  n->lat = tmp;

  cnt += DecodeInt(ptr + cnt, &tmp);
  n->lon = tmp;

  cnt += DecodeUInt(ptr + cnt, &utmp);
  n->large_component = (utmp & (1 << 3)) > 0;
  n->cluster_border_node = (utmp & (1 << 2)) > 0;
  n->dead_end = (utmp & (1 << 1)) > 0;
  n->is_pedestrian_crossing = (utmp & (1 << 0)) > 0;

  return cnt;
}

inline void EncodeGEdge(const GEdge& e, WriteBuff* buff) {
  /*
  std::uint32_t target_idx;
  std::uint32_t way_idx;
  std::uint32_t distance_cm;
  std::uint32_t turn_cost_idx : MAX_TURN_COST_IDX_BITS;
  */
  EncodeUInt(e.target_idx, buff);
  EncodeUInt(e.way_idx, buff);
  EncodeUInt(e.distance_cm, buff);
  EncodeUInt(e.turn_cost_idx, buff);

  /*
  std::uint32_t unique_target : 1;
  std::uint32_t to_bridge : 1;
  std::uint32_t contra_way : 1;
  std::uint32_t cross_country : 1;
  std::uint32_t inverted : 1;
  std::uint32_t both_directions : 1;
  RESTRICTION car_label : NUM_GEDGE_RESTRICTION_BITS;
  std::uint32_t car_label_strange : 1;
  std::uint32_t complex_turn_restriction_trigger : 1;
  std::uint32_t stop_sign : 1;
  std::uint32_t traffic_signal : 1;
  ROAD_PRIORITY road_priority : NUM_GEDGE_ROAD_PRIORITY_BITS;
  TYPE type : NUM_GEDGE_TYPE_BITS;
  */
  uint64_t bits = 0;
  bits = (bits << 1) + e.unique_target;
  bits = (bits << 1) + e.to_bridge;
  bits = (bits << 1) + e.contra_way;
  bits = (bits << 1) + e.cross_country;
  bits = (bits << 1) + e.inverted;
  bits = (bits << 1) + e.both_directions;
  bits = (bits << NUM_GEDGE_RESTRICTION_BITS) + e.car_label;
  bits = (bits << 1) + e.car_label_strange;
  bits = (bits << 1) + e.complex_turn_restriction_trigger;
  bits = (bits << 1) + e.stop_sign;
  bits = (bits << 1) + e.traffic_signal;
  bits = (bits << NUM_GEDGE_ROAD_PRIORITY_BITS) + e.road_priority;
  bits = (bits << NUM_GEDGE_TYPE_BITS) + e.type;
  EncodeUInt(bits, buff);
}

inline uint32_t DecodeGEdge(const std::uint8_t* ptr, GEdge* e) {
  uint32_t cnt = 0;

  {
    uint64_t utmp;
    cnt += DecodeUInt(ptr + cnt, &utmp);
    e->target_idx = utmp;

    cnt += DecodeUInt(ptr + cnt, &utmp);
    e->way_idx = utmp;

    cnt += DecodeUInt(ptr + cnt, &utmp);
    e->distance_cm = utmp;

    cnt += DecodeUInt(ptr + cnt, &utmp);
    e->turn_cost_idx = utmp;
  }

  {
    uint64_t bits;
    cnt += DecodeUInt(ptr + cnt, &bits);

    e->type = static_cast<GEdge::TYPE>(bits & BIT_MASK(NUM_GEDGE_TYPE_BITS));
    bits = bits >> NUM_GEDGE_TYPE_BITS;

    e->road_priority = static_cast<GEdge::ROAD_PRIORITY>(
        bits & BIT_MASK(NUM_GEDGE_ROAD_PRIORITY_BITS));
    bits = bits >> NUM_GEDGE_ROAD_PRIORITY_BITS;

    e->traffic_signal = bits & 1;
    bits = bits >> 1;

    e->stop_sign = bits & 1;
    bits = bits >> 1;

    e->complex_turn_restriction_trigger = bits & 1;
    bits = bits >> 1;

    e->car_label_strange = bits & 1;
    bits = bits >> 1;

    e->car_label = static_cast<GEdge::RESTRICTION>(
        bits & BIT_MASK(NUM_GEDGE_RESTRICTION_BITS));
    bits = bits >> NUM_GEDGE_RESTRICTION_BITS;

    e->both_directions = bits & 1;
    bits = bits >> 1;

    e->inverted = bits & 1;
    bits = bits >> 1;

    e->cross_country = bits & 1;
    bits = bits >> 1;

    e->contra_way = bits & 1;
    bits = bits >> 1;

    e->to_bridge = bits & 1;
    bits = bits >> 1;

    e->unique_target = bits & 1;
    bits = bits >> 1;
  }

  return cnt;
}

inline void EncodeGWay(const GWay& prev, const GWay& w, WriteBuff* buff) {
  /*
  std::int64_t id : GWAY_ID_BITS;
  HIGHWAY_LABEL highway_label : NUM_HIGHWAY_LABEL_BITS = HW_MAX;
  std::uint8_t uniform_country : 1 = 0;
  std::uint8_t closed_way : 1 = 0;  // First node == Last node.
  std::uint8_t area : 1 = 0;        // Has tag area=yes.
  std::uint8_t roundabout : 1 = 0;  // Has tag junction=roundabout.
  std::uint8_t has_ref : 1 = 0;     // Has tag ref=
  std::uint8_t priority_road_forward : 1 = 0;
  std::uint8_t priority_road_backward : 1 = 0;
  std::uint8_t more_than_two_lanes : 1 = 0;
  std::uint16_t ncc : NUM_CC_BITS = INVALID_NCC;
  std::uint32_t wsa_id = INFU32;
  std::uint32_t streetname_idx;
  */
  EncodeInt(w.id - prev.id, buff);
  uint64_t bitset = 0;
  bitset = (bitset << NUM_HIGHWAY_LABEL_BITS) + w.highway_label;
  bitset = (bitset << 1) + w.uniform_country;
  bitset = (bitset << 1) + w.closed_way;
  bitset = (bitset << 1) + w.area;
  bitset = (bitset << 1) + w.roundabout;
  bitset = (bitset << 1) + w.has_ref;
  bitset = (bitset << 1) + w.priority_road_forward;
  bitset = (bitset << 1) + w.priority_road_backward;
  bitset = (bitset << 1) + w.more_than_two_lanes;
  bitset = (bitset << NUM_CC_BITS) + w.ncc;
  EncodeUInt(bitset, buff);
  EncodeUInt(w.wsa_id, buff);
  EncodeUInt(w.streetname_idx, buff);
}

inline uint32_t DecodeGWay(const GWay& prev, const std::uint8_t* ptr, GWay* w) {
  uint32_t cnt = 0;

  DECODE_INT_DIFF(cnt, ptr, prev.id, w->id);

  uint64_t bitset;
  DECODE_UINT(cnt, ptr, bitset);
  DECODE_BITS(bitset, NUM_CC_BITS, w->ncc);
  DECODE_BITS(bitset, 1, w->more_than_two_lanes);
  DECODE_BITS(bitset, 1, w->priority_road_backward);
  DECODE_BITS(bitset, 1, w->priority_road_forward);
  DECODE_BITS(bitset, 1, w->has_ref);
  DECODE_BITS(bitset, 1, w->roundabout);
  DECODE_BITS(bitset, 1, w->area);
  DECODE_BITS(bitset, 1, w->closed_way);
  DECODE_BITS(bitset, 1, w->uniform_country);
  DECODE_BITS(bitset, NUM_HIGHWAY_LABEL_BITS, w->highway_label);

  DECODE_UINT(cnt, ptr, w->wsa_id);
  DECODE_UINT(cnt, ptr, w->streetname_idx);

  return cnt;
}

void EncodeEdgeDescriptorVector(const std::vector<GCluster::EdgeDescriptor> v,
                                WriteBuff* buff) {
  /*
    struct EdgeDescriptor {
      uint32_t g_from_idx = INFU32;
      uint32_t g_edge_idx = INFU32;
      uint32_t c_from_idx = INFU32;
      uint32_t c_edge_idx = INFU32;
      uint32_t pos = INFU32;
    };
  */
  EncodeUInt(v.size(), buff);
  for (const GCluster::EdgeDescriptor ed : v) {
    EncodeUInt(ed.g_from_idx, buff);
    EncodeUInt(ed.g_edge_idx, buff);
    EncodeUInt(ed.c_from_idx, buff);
    EncodeUInt(ed.c_edge_idx, buff);
    EncodeUInt(ed.pos, buff);
  }
}

uint32_t DecodeEdgeDescriptorVector(const std::uint8_t* ptr,
                                    std::vector<GCluster::EdgeDescriptor>* v) {
  uint32_t cnt = 0;
  uint64_t num;
  cnt += DecodeUInt(ptr + cnt, &num);
  for (size_t i = 0; i < num; ++i) {
    GCluster::EdgeDescriptor ed;
    cnt += DecodeUInt(ptr + cnt, &ed.g_from_idx);
    cnt += DecodeUInt(ptr + cnt, &ed.g_edge_idx);
    cnt += DecodeUInt(ptr + cnt, &ed.c_from_idx);
    cnt += DecodeUInt(ptr + cnt, &ed.c_edge_idx);
    cnt += DecodeUInt(ptr + cnt, &ed.pos);
    v->push_back(ed);
  }
  return cnt;
}

inline void EncodeGCluster(const GCluster& cl, WriteBuff* buff) {
  /*
    std::uint32_t cluster_id = 0;
    std::uint32_t num_nodes = 0;
    std::uint32_t num_border_nodes = 0;
    std::uint32_t num_inner_edges = 0;
    std::uint32_t num_outer_edges = 0;
    std::vector<std::uint32_t> border_nodes;
    std::vector<EdgeDescriptor> border_in_edges;
    std::vector<EdgeDescriptor> border_out_edges;
    std::vector<std::vector<std::uint32_t>> distances;
    std::vector<std::vector<std::uint32_t>> edge_distances;
    std::uint16_t color_no = 0;
  */
  EncodeUInt(cl.cluster_id, buff);
  EncodeUInt(cl.num_nodes, buff);
  EncodeUInt(cl.num_border_nodes, buff);
  EncodeUInt(cl.num_inner_edges, buff);
  EncodeUInt(cl.num_outer_edges, buff);

  EncodeVector(cl.border_nodes, buff);
  EncodeEdgeDescriptorVector(cl.border_in_edges, buff);
  EncodeEdgeDescriptorVector(cl.border_out_edges, buff);
  EncodeVector(cl.distances, buff);
  EncodeVector(cl.edge_distances, buff);
}

inline uint32_t DecodeGCluster(const std::uint8_t* ptr, GCluster* cl) {
  uint32_t cnt = 0;

  cnt += DecodeUInt(ptr + cnt, &cl->cluster_id);
  cnt += DecodeUInt(ptr + cnt, &cl->num_nodes);
  cnt += DecodeUInt(ptr + cnt, &cl->num_border_nodes);
  cnt += DecodeUInt(ptr + cnt, &cl->num_inner_edges);
  cnt += DecodeUInt(ptr + cnt, &cl->num_outer_edges);

  cnt += DecodeVector(ptr + cnt, &cl->border_nodes);
  cnt += DecodeEdgeDescriptorVector(ptr + cnt, &cl->border_in_edges);
  cnt += DecodeEdgeDescriptorVector(ptr + cnt, &cl->border_out_edges);
  cnt += DecodeVector(ptr + cnt, &cl->distances);
  cnt += DecodeVector(ptr + cnt, &cl->edge_distances);

  return cnt;
}

inline void EncodeTurnCostData(const TurnCostData& tcd, WriteBuff* buff) {
  /*
    std::vector<uint8_t> turn_costs;
  */
  EncodeVector(tcd.turn_costs, buff);
}

inline uint32_t DecodeTurnCostData(const std::uint8_t* ptr, TurnCostData* tcd) {
  return DecodeVector(ptr, &tcd->turn_costs);
}

inline void EncodeWaySharedAttrs(const WaySharedAttrs& wsa, WriteBuff* buff) {
  /*
    RoutingAttrs ra[RA_MAX];
  */
  for (const RoutingAttrs& ra : wsa.ra) {
    uint64_t bitset = 0;
    bitset = (bitset << 1) + ra.dir;
    bitset = (bitset << 4) + ra.access;
    bitset = (bitset << 10) + ra.maxspeed;
    bitset = (bitset << 1) + ra.lit;
    bitset = (bitset << 1) + ra.toll;
    bitset = (bitset << 6) + ra.surface;
    bitset = (bitset << 3) + ra.tracktype;
    bitset = (bitset << 4) + ra.smoothness;
    bitset = (bitset << 1) + ra.left_side;
    bitset = (bitset << 1) + ra.right_side;
    bitset = (bitset << 8) + ra.width_dm;
    EncodeUInt(bitset, buff);
  }
}

inline uint32_t DecodeWaySharedAttrs(const std::uint8_t* ptr,
                                     WaySharedAttrs* wsa) {
  uint32_t cnt = 0;
  for (RoutingAttrs& ra : wsa->ra) {
    uint64_t bitset;
    cnt += DecodeUInt(ptr + cnt, &bitset);
    DECODE_BITS(bitset, 8, ra.width_dm);
    DECODE_BITS(bitset, 1, ra.right_side);
    DECODE_BITS(bitset, 1, ra.left_side);
    DECODE_BITS(bitset, 4, ra.smoothness);
    DECODE_BITS(bitset, 3, ra.tracktype);
    DECODE_BITS(bitset, 6, ra.surface);
    DECODE_BITS(bitset, 1, ra.toll);
    DECODE_BITS(bitset, 1, ra.lit);
    DECODE_BITS(bitset, 10, ra.maxspeed);
    DECODE_BITS(bitset, 4, ra.access);
    DECODE_BITS(bitset, 1, ra.dir);
  }
  return cnt;
}

inline void EncodeTurnRestriction(const TurnRestriction& tr, WriteBuff* buff) {
  /*
  std::int64_t relation_id = -1;
  std::int64_t from_way_id = -1;
  std::vector<std::int64_t> via_ids;
  std::int64_t to_way_id = -1;
  bool via_is_node : 1 = 0;  // via is a node (true) or way(s) (false).
  bool forbidden : 1 = 0;
  TurnDirection direction : 3 = TurnDirection::LeftTurn;
  struct TREdge {
    std::uint32_t from_node_idx;
    std::uint32_t way_idx;
    std::uint32_t to_node_idx;
    std::uint32_t edge_idx;
  };
  std::vector<TREdge> path;
  */
  EncodeInt(tr.relation_id, buff);
  EncodeInt(tr.from_way_id, buff);
  EncodeVector(tr.via_ids, buff);
  EncodeInt(tr.to_way_id, buff);
  uint64_t bitset = 0;
  bitset = (bitset << 1) + (tr.via_is_node ? 1 : 0);
  bitset = (bitset << 1) + (tr.forbidden ? 1 : 0);
  bitset = (bitset << 3) + static_cast<uint64_t>(tr.direction);
  EncodeUInt(bitset, buff);
  EncodeUInt(tr.path.size(), buff);
  for (const TurnRestriction::TREdge& e : tr.path) {
    EncodeUInt(e.from_node_idx, buff);
    EncodeUInt(e.way_idx, buff);
    EncodeUInt(e.to_node_idx, buff);
    EncodeUInt(e.edge_idx, buff);
  }
}

inline uint32_t DecodeTurnRestriction(const std::uint8_t* ptr,
                                      TurnRestriction* tr) {
  uint32_t cnt = 0;
  cnt += DecodeInt(ptr + cnt, &tr->relation_id);
  cnt += DecodeInt(ptr + cnt, &tr->from_way_id);
  cnt += DecodeVector(ptr + cnt, &tr->via_ids);
  cnt += DecodeInt(ptr + cnt, &tr->to_way_id);
  uint64_t bitset = 0;
  cnt += DecodeUInt(ptr + cnt, &bitset);
  DECODE_BITS(bitset, 3, tr->direction);
  DECODE_BITS(bitset, 1, tr->forbidden);
  DECODE_BITS(bitset, 1, tr->via_is_node);
  uint64_t v_size = 0;
  cnt += DecodeUInt(ptr + cnt, &v_size);
  tr->path.resize(v_size);
  for (size_t i = 0; i < v_size; ++i) {
    cnt += DecodeUInt(ptr + cnt, &tr->path.at(i).from_node_idx);
    cnt += DecodeUInt(ptr + cnt, &tr->path.at(i).way_idx);
    cnt += DecodeUInt(ptr + cnt, &tr->path.at(i).to_node_idx);
    cnt += DecodeUInt(ptr + cnt, &tr->path.at(i).edge_idx);
  }
  return cnt;
}

inline void EncodeComponent(const Graph::Component& c, WriteBuff* buff) {
  /*
    uint32_t start_node;
    uint32_t size;
  */
  EncodeUInt(c.start_node, buff);
  EncodeUInt(c.size, buff);
}

inline uint32_t DecodeComponent(const std::uint8_t* ptr, Graph::Component* c) {
  uint32_t cnt = 0;
  cnt += DecodeUInt(ptr + cnt, &c->start_node);
  cnt += DecodeUInt(ptr + cnt, &c->size);
  return cnt;
}

constexpr uint64_t NumElementsPerBlock = 500'000;

void FileReadToBuf(const std::string& filename, std::ifstream* file,
                   uint64_t size, std::vector<uint8_t>* buf) {
  buf->resize(size);
  file->read((char*)buf->data(), static_cast<std::streamsize>(size));
  if (static_cast<std::size_t>(file->gcount()) != size) {
    ABORT_S() << "can't read " << size << " bytes from " << filename;
  }
}

// The header section is stored at the beginning of the file.
// When writing, it is written last (but at the beginning of the file).
// When reading, this is read first.
struct FileMeta {
  enum TYPE : uint16_t {
    TYPE_NODE = 0,
    TYPE_EDGE,
    TYPE_WAY,
    TYPE_CLUSTER,
    TYPE_TURN_COST_DATA,
    TYPE_WAY_SHARED_ATTRS,
    TYPE_COMPLEX_TURN_RESTRICTION,
    TYPE_COMPONENTS,
    TYPE_STREETNAMES,
    TYPE_MAX
  };

  struct EntityHeader {
    TYPE type;
    uint64_t rec_count;
    uint64_t first_block;
    uint64_t num_blocks;
  };

  struct BlockHeader {
    TYPE type;
    uint64_t v_start_pos;
    uint64_t v_stop_pos;
    uint64_t file_start_pos;
    uint64_t num_bytes;
  };

  std::vector<EntityHeader> entities;
  std::vector<BlockHeader> blocks;

  // Returns an upper bound for the bytes needed to store the FileMeta data.
  // Uses NumElementsPerBlock to compute the number of blocks that need to be
  // stored.
  static uint64_t FileMetaSizeUpperBound(const Graph& g) {
    uint64_t size = 0;

    // EntityHeaders, one per type.
    size += TYPE_MAX * 4 * max_varint_bytes;

    size += (g.nodes.size() / NumElementsPerBlock + 1) * 5 * max_varint_bytes;
    size += (g.edges.size() / NumElementsPerBlock + 1) * 5 * max_varint_bytes;
    size += (g.ways.size() / NumElementsPerBlock + 1) * 5 * max_varint_bytes;
    size +=
        (g.clusters.size() / NumElementsPerBlock + 1) * 5 * max_varint_bytes;
    size +=
        (g.turn_costs.size() / NumElementsPerBlock + 1) * 5 * max_varint_bytes;
    size += (g.way_shared_attrs.size() / NumElementsPerBlock + 1) * 5 *
            max_varint_bytes;
    size += (g.complex_turn_restrictions.size() / NumElementsPerBlock + 1) * 5 *
            max_varint_bytes;
    size += (g.large_components.size() / NumElementsPerBlock + 1) * 5 *
            max_varint_bytes;
    size += (g.streetnames.size() / NumElementsPerBlock + 1) * 5 *
            max_varint_bytes;
    return size;
  }

  // Write the file magic and the file meta in one go.
  void SerializeFileHeader(uint64_t file_meta_size_upper_bound,
                           std::ofstream* file) {
    WriteBuff wb;

    // Bytes [0..SER_GRAPH_FILE_HEADER_SIZE-1] contain these fields.
    EncodeUInt(SER_GRAPH_MAGIC_NUM, &wb);
    EncodeUInt(SER_GRAPH_VERSION_NUM, &wb);
    EncodeUInt(file_meta_size_upper_bound, &wb);
    while (wb.used() < SER_GRAPH_FILE_HEADER_SIZE) {
      uint8_t tmp = 0;
      wb.CopyBytes(&tmp, 1);
    }
    CHECK_EQ_S(wb.used(), SER_GRAPH_FILE_HEADER_SIZE);

    // This starts at position SER_GRAPH_FILE_HEADER_SIZE in the file
    EncodeUInt(entities.size(), &wb);
    for (const EntityHeader& h : entities) {
      EncodeUInt(h.type, &wb);
      EncodeUInt(h.rec_count, &wb);
      EncodeUInt(h.first_block, &wb);
      EncodeUInt(h.num_blocks, &wb);
    }

    EncodeUInt(blocks.size(), &wb);
    for (const BlockHeader& b : blocks) {
      EncodeUInt(b.type, &wb);
      EncodeUInt(b.v_start_pos, &wb);
      EncodeUInt(b.v_stop_pos, &wb);
      EncodeUInt(b.file_start_pos, &wb);
      EncodeUInt(b.num_bytes, &wb);
    }

    CHECK_LE_S(wb.used(),
               SER_GRAPH_FILE_HEADER_SIZE + file_meta_size_upper_bound);
    file->seekp(0);
    file->write((const char*)wb.base_ptr(), wb.used());
  }

  void ReadFileHeader(const std::string& filename, std::ifstream* file) {
    file->seekg(0);
    uint64_t pos = 0;

    LOG_S(INFO) << "Magic header size:" << SER_GRAPH_FILE_HEADER_SIZE;
    std::vector<uint8_t> buf(SER_GRAPH_FILE_HEADER_SIZE);
    FileReadToBuf(filename, file, SER_GRAPH_FILE_HEADER_SIZE, &buf);
    {
      uint64_t tmp;
      pos += DecodeUInt(buf.data() + pos, &tmp);
      if (tmp != SER_GRAPH_MAGIC_NUM) {
        ABORT_S() << "Wrong magic number in file " << filename;
      }
    }
    {
      uint64_t tmp;
      pos += DecodeUInt(buf.data() + pos, &tmp);
      if (tmp != SER_GRAPH_VERSION_NUM) {
        ABORT_S() << absl::StrFormat(
            "Wrong version (%llu instead of %llu) in file %s", filename, tmp,
            SER_GRAPH_VERSION_NUM);
      }
    }

    uint64_t file_meta_size_upper_bound;
    pos += DecodeUInt(buf.data() + pos, &file_meta_size_upper_bound);
    LOG_S(INFO) << "File meta size:" << file_meta_size_upper_bound;

    pos = 0;
    FileReadToBuf(filename, file, file_meta_size_upper_bound, &buf);

    {
      uint64_t headers_size;
      pos += DecodeUInt(buf.data() + pos, &headers_size);
      entities.resize(headers_size);
      for (size_t i = 0; i < headers_size; ++i) {
        FileMeta::EntityHeader& h = entities.at(i);
        uint64_t type;
        pos += DecodeUInt(buf.data() + pos, &type);
        CHECK_LT_S(type, FileMeta::TYPE_MAX);
        h.type = (FileMeta::TYPE)type;
        pos += DecodeUInt(buf.data() + pos, &h.rec_count);
        pos += DecodeUInt(buf.data() + pos, &h.first_block);
        pos += DecodeUInt(buf.data() + pos, &h.num_blocks);
        LOG_S(INFO) << absl::StrFormat(
            "Entity %d: #elements:%llu start-block:%llu num_blocks:%llu",
            h.type, h.rec_count, h.first_block, h.num_blocks);
      }
    }

    {
      uint64_t blocks_size;
      pos += DecodeUInt(buf.data() + pos, &blocks_size);
      blocks.resize(blocks_size);
      for (size_t i = 0; i < blocks_size; ++i) {
        FileMeta::BlockHeader& b = blocks.at(i);
        uint64_t type;
        pos += DecodeUInt(buf.data() + pos, &type);
        CHECK_LT_S(type, FileMeta::TYPE_MAX);
        b.type = (FileMeta::TYPE)type;
        pos += DecodeUInt(buf.data() + pos, &b.v_start_pos);
        pos += DecodeUInt(buf.data() + pos, &b.v_stop_pos);
        pos += DecodeUInt(buf.data() + pos, &b.file_start_pos);
        pos += DecodeUInt(buf.data() + pos, &b.num_bytes);

        LOG_S(INFO) << absl::StrFormat(
            "Block %llu: type:%d vec:[%llu,%llu) fpos:%llu bytes:%llu", i,
            b.type, b.v_start_pos, b.v_stop_pos, b.file_start_pos, b.num_bytes);
      }
    }
  }
};

// Serialize all records of one type to the serialized graph file.
template <typename RecType, FileMeta::TYPE type>
uint64_t WriteBlocks(std::ofstream* file, uint64_t fsize, FileMeta* meta,
                     const std::vector<RecType>& records) {
  FileMeta::EntityHeader ehead = {.type = type,
                                  .rec_count = records.size(),
                                  .first_block = meta->blocks.size(),
                                  .num_blocks = 0};
  WriteBuff wb;
  const RecType diff_base = {};
  for (uint64_t p = 0; p < records.size(); p += NumElementsPerBlock) {
    uint64_t stop = std::min(records.size(), p + NumElementsPerBlock);
    // Write a block containing nodes [p, stop);
    wb.clear();
    const RecType* prev = &diff_base;  // Initialise with 0.
    for (uint64_t np = p; np < stop; ++np) {
      const RecType& rec = records.at(np);
      if constexpr (std::same_as<RecType, GNode>) {
        EncodeGNode(*prev, rec, &wb);
        prev = &rec;
      } else if constexpr (std::same_as<RecType, GEdge>) {
        EncodeGEdge(rec, &wb);
      } else if constexpr (std::same_as<RecType, GWay>) {
        EncodeGWay(*prev, rec, &wb);
        prev = &rec;
      } else if constexpr (std::same_as<RecType, GCluster>) {
        EncodeGCluster(rec, &wb);
      } else if constexpr (std::same_as<RecType, TurnCostData>) {
        EncodeTurnCostData(rec, &wb);
      } else if constexpr (std::same_as<RecType, WaySharedAttrs>) {
        EncodeWaySharedAttrs(rec, &wb);
      } else if constexpr (std::same_as<RecType, TurnRestriction>) {
        EncodeTurnRestriction(rec, &wb);
      } else if constexpr (std::same_as<RecType, Graph::Component>) {
        EncodeComponent(rec, &wb);
      } else if constexpr (std::same_as<RecType, std::string>) {
        EncodeString(rec, &wb);
      } else {
        static_assert(false, "unsupported type");
      }
    }
    meta->blocks.push_back({.type = type,
                            .v_start_pos = p,
                            .v_stop_pos = stop,
                            .file_start_pos = fsize,
                            .num_bytes = wb.used()});
    file->write((const char*)wb.base_ptr(), wb.used());
    fsize += wb.used();
    ehead.num_blocks++;
  }
  CHECK_EQ_S(ehead.type, meta->entities.size());
  meta->entities.push_back(ehead);
  return fsize;
}

void WriteSerializedGraph(const Graph& g, const std::string& filename) {
  FuncTimer timer(absl::StrFormat("Serialize graph to %s", filename.c_str()),
                  __FILE__, __LINE__);

  std::ofstream file;
  file.open(filename, std::ios::trunc | std::ios::binary | std::ios::out);
  if (!file) {
    ABORT_S() << "Cant open " << filename;
  }

  FileMeta meta;
  const uint64_t file_meta_size_upper_bound = meta.FileMetaSizeUpperBound(g);
  uint64_t fpos = 0;
  {
    // Write empty magic + file_meta. The real data is written at the end.
    size_t cnt = SER_GRAPH_FILE_HEADER_SIZE + file_meta_size_upper_bound;
    std::vector<char> buffer(cnt, 0);
    file.seekp(0);
    file.write(buffer.data(), cnt);
    fpos += cnt;
  }

  fpos = WriteBlocks<GNode, FileMeta::TYPE_NODE>(&file, fpos, &meta, g.nodes);
  fpos = WriteBlocks<GEdge, FileMeta::TYPE_EDGE>(&file, fpos, &meta, g.edges);
  fpos = WriteBlocks<GWay, FileMeta::TYPE_WAY>(&file, fpos, &meta, g.ways);
  fpos = WriteBlocks<GCluster, FileMeta::TYPE_CLUSTER>(&file, fpos, &meta,
                                                       g.clusters);
  fpos = WriteBlocks<TurnCostData, FileMeta::TYPE_TURN_COST_DATA>(
      &file, fpos, &meta, g.turn_costs);
  fpos = WriteBlocks<WaySharedAttrs, FileMeta::TYPE_WAY_SHARED_ATTRS>(
      &file, fpos, &meta, g.way_shared_attrs);
  fpos = WriteBlocks<TurnRestriction, FileMeta::TYPE_COMPLEX_TURN_RESTRICTION>(
      &file, fpos, &meta, g.complex_turn_restrictions);
  fpos = WriteBlocks<Graph::Component, FileMeta::TYPE_COMPONENTS>(
      &file, fpos, &meta, g.large_components);
  fpos = WriteBlocks<std::string, FileMeta::TYPE_STREETNAMES>(
      &file, fpos, &meta, g.streetnames);

  meta.SerializeFileHeader(file_meta_size_upper_bound, &file);
  file.close();
  LOG_S(INFO) << absl::StrFormat("Written %llu bytes", fpos);
}

// Read all records of one type.
template <class RecType>
void ReadBlocks(const FileMeta& meta, const FileMeta::EntityHeader& eh,
                const std::string& filename, std::ifstream* file, Graph* g) {
  std::vector<uint8_t> buf;
  const RecType diff_base = {};

  size_t count = 0;
  for (size_t i = eh.first_block; i < eh.first_block + eh.num_blocks; ++i) {
    const FileMeta::BlockHeader& b = meta.blocks.at(i);
    CHECK_EQ_S(eh.type, b.type);
    buf.resize(b.num_bytes);
    file->seekg(b.file_start_pos);
    FileReadToBuf(filename, file, b.num_bytes, &buf);
    uint64_t pos = 0;
    const RecType* prev = &diff_base;
    for (size_t k = b.v_start_pos; k < b.v_stop_pos; ++k) {
      ++count;
      if constexpr (std::same_as<RecType, GNode>) {
        pos += DecodeGNode(*prev, buf.data() + pos, &g->nodes.at(k));
        prev = &g->nodes.at(k);
      } else if constexpr (std::same_as<RecType, GEdge>) {
        pos += DecodeGEdge(buf.data() + pos, &g->edges.at(k));
      } else if constexpr (std::same_as<RecType, GWay>) {
        pos += DecodeGWay(*prev, buf.data() + pos, &g->ways.at(k));
        prev = &g->ways.at(k);
      } else if constexpr (std::same_as<RecType, GCluster>) {
        pos += DecodeGCluster(buf.data() + pos, &g->clusters.at(k));
      } else if constexpr (std::same_as<RecType, TurnCostData>) {
        pos += DecodeTurnCostData(buf.data() + pos, &g->turn_costs.at(k));
      } else if constexpr (std::same_as<RecType, WaySharedAttrs>) {
        pos +=
            DecodeWaySharedAttrs(buf.data() + pos, &g->way_shared_attrs.at(k));
      } else if constexpr (std::same_as<RecType, TurnRestriction>) {
        pos += DecodeTurnRestriction(buf.data() + pos,
                                     &g->complex_turn_restrictions.at(k));
      } else if constexpr (std::same_as<RecType, Graph::Component>) {
        pos += DecodeComponent(buf.data() + pos, &g->large_components.at(k));
      } else if constexpr (std::same_as<RecType, std::string>) {
        std::string_view sv;
        pos += DecodeString(buf.data() + pos, &sv);
        g->streetnames.at(k) = sv;
      } else {
        static_assert(false, "unsupported type");
      }
    }
  }
  LOG_S(INFO) << absl::StrFormat("Read %lld records type:%d from %s",
                                 eh.rec_count, eh.type, filename);
  CHECK_EQ_S(count, eh.rec_count);
}

Graph ReadSerializedGraph(const std::string& filename) {
  Graph g;

  FuncTimer timer(
      absl::StrFormat("Read serialized graph from %s", filename.c_str()),
      __FILE__, __LINE__);

  std::ifstream file;
  file.open(filename, std::ios::binary | std::ios::in);
  if (!file) {
    ABORT_S() << "Cant open " << filename;
  }

  FileMeta meta;
  meta.ReadFileHeader(filename, &file);

  g.nodes.resize(meta.entities.at(FileMeta::TYPE_NODE).rec_count);
  ReadBlocks<GNode>(meta, meta.entities.at(FileMeta::TYPE_NODE), filename,
                    &file, &g);

  g.edges.resize(meta.entities.at(FileMeta::TYPE_EDGE).rec_count);
  ReadBlocks<GEdge>(meta, meta.entities.at(FileMeta::TYPE_EDGE), filename,
                    &file, &g);

  g.ways.resize(meta.entities.at(FileMeta::TYPE_WAY).rec_count);
  ReadBlocks<GWay>(meta, meta.entities.at(FileMeta::TYPE_WAY), filename, &file,
                   &g);

  g.clusters.resize(meta.entities.at(FileMeta::TYPE_CLUSTER).rec_count);
  ReadBlocks<GCluster>(meta, meta.entities.at(FileMeta::TYPE_CLUSTER), filename,
                       &file, &g);

  g.turn_costs.resize(
      meta.entities.at(FileMeta::TYPE_TURN_COST_DATA).rec_count);
  ReadBlocks<TurnCostData>(meta,
                           meta.entities.at(FileMeta::TYPE_TURN_COST_DATA),
                           filename, &file, &g);

  g.way_shared_attrs.resize(
      meta.entities.at(FileMeta::TYPE_WAY_SHARED_ATTRS).rec_count);
  ReadBlocks<WaySharedAttrs>(meta,
                             meta.entities.at(FileMeta::TYPE_WAY_SHARED_ATTRS),
                             filename, &file, &g);

  g.complex_turn_restrictions.resize(
      meta.entities.at(FileMeta::TYPE_COMPLEX_TURN_RESTRICTION).rec_count);
  ReadBlocks<TurnRestriction>(
      meta, meta.entities.at(FileMeta::TYPE_COMPLEX_TURN_RESTRICTION), filename,
      &file, &g);

  g.large_components.resize(
      meta.entities.at(FileMeta::TYPE_COMPONENTS).rec_count);
  ReadBlocks<Graph::Component>(
      meta, meta.entities.at(FileMeta::TYPE_COMPONENTS), filename, &file, &g);

  g.streetnames.resize(
      meta.entities.at(FileMeta::TYPE_STREETNAMES).rec_count);
  ReadBlocks<std::string>(
      meta, meta.entities.at(FileMeta::TYPE_STREETNAMES), filename, &file, &g);

  // Post processing.
  g.complex_turn_restriction_map =
      ComputeTurnRestrictionMapToFirst(g.complex_turn_restrictions);

  file.close();
  return g;
}
