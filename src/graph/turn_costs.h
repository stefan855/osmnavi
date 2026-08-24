#pragma once

#include <vector>

#include "base/duration_type.h"
#include "geometry/distance.h"
#include "graph/crossing.h"
#include "graph/graph_def.h"
#include "graph/graph_def_utils.h"

#if 0
// For an edge in the graph, this data describes the turn costs when arriving
// at the target node of the edge and continuing on an edge starting at the
// target node (i.e. an outgoing edge).
//
// Note that the dimension of the 'turn_costs' vector of edge e must be equal
// the number of outgoing edges at the target node of e.
//
// A special value indicates that a turn is not possible. This means that a turn
// restriction forbids the turn, a barrier disallows passing the target node, a
// u-turn is not allowed or something else.
struct TurnCostData {
  // TODO: Use a more memory-efficient data structure than a vector.
  std::vector<uint8_t> turn_costs;

  bool operator==(const TurnCostData& other) const {
    return turn_costs == other.turn_costs;
  }
};
#endif

constexpr DurationMS TURN_COST_INFINITY(1'000'000'000u);  // ~11.5 days
constexpr DurationMS TURN_COST_ZERO(0u);
constexpr DurationMS TURN_COST_U_TURN(20000u);

namespace {
// turn costs in milliseconds are compressed into 63 buckets, which
// represent the following values.
static constexpr size_t compressed_turn_cost_values_dim = 64;
static constexpr DurationMS
    compressed_turn_cost_values[compressed_turn_cost_values_dim] = {
        DurationMS(0u),        DurationMS(100u),      DurationMS(200u),
        DurationMS(300u),      DurationMS(400u),      DurationMS(500u),
        DurationMS(600u),      DurationMS(700u),      DurationMS(800u),
        DurationMS(900u),      DurationMS(1000u),     DurationMS(1100u),
        DurationMS(1200u),     DurationMS(1300u),     DurationMS(1400u),
        DurationMS(1500u),     DurationMS(1600u),     DurationMS(1900u),
        DurationMS(2200u),     DurationMS(2500u),     DurationMS(2800u),
        DurationMS(3300u),     DurationMS(3800u),     DurationMS(4300u),
        DurationMS(5000u),     DurationMS(5700u),     DurationMS(6600u),
        DurationMS(7600u),     DurationMS(8700u),     DurationMS(10000u),
        DurationMS(11500u),    DurationMS(13200u),    DurationMS(17200u),
        DurationMS(22400u),    DurationMS(29100u),    DurationMS(37800u),
        DurationMS(49200u),    DurationMS(63900u),    DurationMS(83100u),
        DurationMS(108000u),   DurationMS(140400u),   DurationMS(182600u),
        DurationMS(237300u),   DurationMS(308500u),   DurationMS(401100u),
        DurationMS(521400u),   DurationMS(677800u),   DurationMS(881200u),
        DurationMS(1145500u),  DurationMS(1489200u),  DurationMS(1935900u),
        DurationMS(2516700u),  DurationMS(3271700u),  DurationMS(4253200u),
        DurationMS(5529200u),  DurationMS(7188000u),  DurationMS(9344300u),
        DurationMS(12147700u), DurationMS(15791900u), DurationMS(20529500u),
        DurationMS(26688400u), DurationMS(34694900u), DurationMS(45103400u),
        TURN_COST_INFINITY};

}  // namespace

// Compress a turn cost value (milliseconds) to compressed format
// using 6 bits. Note that all values > 45103400 are compressed to INF and are
// treated as non-routable.
inline constexpr uint32_t compress_turn_cost(DurationMS cost) {
  if (cost == 0) return 0;
  // std::lower_bound finds the first element >= cost.
  size_t pos =
      std::lower_bound(
          compressed_turn_cost_values,
          compressed_turn_cost_values + compressed_turn_cost_values_dim, cost) -
      compressed_turn_cost_values;
  if (pos < compressed_turn_cost_values_dim - 1) {
    assert(pos > 0);
    // Note that by construction both values are positive.
    const auto diff = compressed_turn_cost_values[pos] - cost;
    const auto diff_prev = cost - compressed_turn_cost_values[pos - 1];
    if (diff >= diff_prev) {
      return pos - 1;
    } else {
      return pos;
    }
  } else {
    return compressed_turn_cost_values_dim - 1;
  }
}

constexpr uint32_t TURN_COST_ZERO_COMPRESSED =
    compress_turn_cost(TURN_COST_ZERO);
constexpr uint32_t TURN_COST_INFINITY_COMPRESSED =
    compress_turn_cost(TURN_COST_INFINITY);
constexpr uint32_t TURN_COST_U_TURN_COMPRESSED =
    compress_turn_cost(TURN_COST_U_TURN);

static_assert(TURN_COST_ZERO_COMPRESSED == 0);
static_assert(TURN_COST_INFINITY_COMPRESSED ==
              compressed_turn_cost_values_dim - 1);

inline constexpr DurationMS decompress_turn_cost(uint32_t compressed_cost) {
  if (compressed_cost == 0) return DurationMS(0u);
  CHECK_LT_S(compressed_cost, compressed_turn_cost_values_dim);
  return compressed_turn_cost_values[compressed_cost];
}

// Return the maximal velocity (km/h) in a curve for a typical car.
// arc_length_cm: The distance for which we have a turn_angle. Typically this
//                is 1/2 of the length of the incoming edge plus 1/2 of the
//                length of the outgoing edge.
// turn_angle:    angle change in degrees between incoming and outgoing edge.
//                0 degrees indicate a straight continuation, 180 degrees
//                indicate a u-turn.
//
// See https://www.lernhelfer.de/schuelerlexikon/physik/artikel/kurvenfahrten
// Formula for max. v through curve with length s and angle-change a:
//           v-max: 3.6 * sqrt(u * g * s / (a * pi / 180))
//           u:     coefficient of static friction (Haftreibungskoeffizient)
//                  depends on conditions (rain etc.) surface and more.
//                  Range for cars is 0.4..0.8 (on asphalt roads).
//                  We use 0.5, which is on the conservative side.
//           g:     9.81
//           s:     arc length (Bogenlänge)
//           a:     angle change in degrees [-180..179].
// Example for a curve with length 100m and 90 degree change in direction:
//   v-max = 63.615 km/h = 3.6 * math.sqrt(0.5*9.81*100/(90*math.pi/180))
inline double MaxCurveVelocity(DistanceType arc_length, int32_t turn_angle) {
  return std::sqrt((3.6 * 3.6 * 0.5 * 9.81 * arc_length.meters()) /
                   (std::fabs(turn_angle) * std::numbers::pi / 180.0));
}

struct VehicleAccelerations {
  // All values are positive, put the sign as needed!
  double acc_car;
  double dec_car;
  double acc_hgv;
  double dec_hgv;
  double acc_bicycle;
  double dec_bicycle;

  constexpr double get(VEHICLE vt, bool accelerate) const {
    switch (vt) {
      case VH_MOTORCAR:
      case VH_MOTORCYCLE:
        return accelerate ? acc_car : dec_car;
      case VH_BICYCLE:
      case VH_MOPED:
      case VH_HORSE:
        return accelerate ? acc_bicycle : dec_bicycle;
      case VH_PSV:
      case VH_BUS:
      case VH_HGV:
        return accelerate ? acc_hgv : dec_hgv;
      default:
        ABORT_S() << vt;
    }
  }

  constexpr double get_acc(VEHICLE vt) const { return get(vt, true); }
  constexpr double get_dec(VEHICLE vt) const { return get(vt, false); }
};

static constexpr VehicleAccelerations g_vehicle_acceleration = {
    // Rough assumptions about vehicle specific acceleration/deceleration
    // constants.
    .acc_car = 2.0,  .dec_car = -3.0,    .acc_hgv = 1.0,
    .dec_hgv = -1.5, .acc_bicycle = 1.0, .dec_bicycle = -1.5};

constexpr double GetVHAccelOrDecel(VEHICLE vt, bool accelerate) {
  return g_vehicle_acceleration.get(vt, accelerate);
}

constexpr double GetVHAccel(VEHICLE vt) {
  return g_vehicle_acceleration.get(vt, /*accelerate=*/true);
}

constexpr double GetVHDecel(VEHICLE vt) {
  return g_vehicle_acceleration.get(vt, /*accelerate=*/false);
}

namespace {

// How much time is needed for 'distance' at static 'speed'?
inline DurationMS TimeForDistance(DistanceType distance, double speed) {
  // Use use t = s / v (from s = v * t).
  double tsec = distance.meters() / (speed / 3.6);
  int64_t ms = std::lround(tsec * 1000.0);
  CHECK_GE_S(ms, 0) << tsec;
  return DurationMS(static_cast<uint64_t>(ms));
}

// Compute average speed for full distance, given two partial distances with
// individual speeds.
inline double ComputeAverageSpeed(DistanceType d0, double speed0,
                                  DistanceType d1, double speed1) {
  DurationMS t0 = TimeForDistance(d0, speed0);
  DurationMS t1 = TimeForDistance(d1, speed1);
  // s=v*t ==> v=s/t.
  CHECK_GT_S(t0.seconds() + t1.seconds(), 0.0);
  return 3.6 * (d0.meters() + d1.meters()) / (t0.seconds() + t1.seconds());
}

// Compute the distance for accelerating or decelerating a vehicle from
// speed0_kmh to speed1_kmh.
//
// Uses the following formula for speed changes:
//     dist = abs(v1^2 - v0^2) / (2 * abs(a)).
//
// How to find this formula:
//     1) v1 = v0 + a * t
//     2) t = (v1 - v0) / a
//     3) Replace t with (2) in s = v0 * t + 1/2 * a * t^2
//     4) Simplify until you get s = (v1^2 - v0^2) / (2 * a)
//
// This works for both accelerating and decelerating, but not for negative
// speeds.
inline DistanceType DistanceForSpeedChange(double speed0_kmh, double speed1_kmh,
                                           double a) {
  CHECK_GE_S(speed0_kmh, 0.0);
  CHECK_GE_S(speed1_kmh, 0.0);
  CHECK_EQ_S(speed1_kmh > speed0_kmh, a > 0.0)
      << "speed1_kmh:" << speed1_kmh << " speed0_kmh:" << speed0_kmh
      << " a:" << a;
  const double v0 = speed0_kmh / 3.6;
  const double v1 = speed1_kmh / 3.6;

  return DistanceType(static_cast<uint64_t>(
      std::lround(std::abs(100.0 * (v1 * v1 - v0 * v0) / (2.0 * a)))));
}

// Starting with speed0 and acceleration acc over distance d, compute the
// resulting speed.
//
// Check-fails if there is no solution (when 'acc' is too negative).
//
// Formula:
//     1) Solve the quadratic equation 0 = v0 * t + 1/2 * a * t^2 - dist
//     2) t = -v0 +- sqrt(v0^2 + 2 * a * s) / a
//     3) Put the positive t into v1 = v0 + a * t
//     4) Simplify: v1 = sqrt(v0^2 + 2 * a * s)
inline double SpeedAfterDistance(double speed0_kmh, DistanceType distance,
                                 double a) {
  const double v0 = speed0_kmh / 3.6;
  const double inner = v0 * v0 + 2.0 * a * distance.meters();
  CHECK_GE_S(inner, 0.0) << speed0_kmh << " " << distance << " " << a;
  return 3.6 * std::sqrt(inner);
}

#if 0
// When accelerating over distance 'full_dist' from speed0 to speed1, what is
// the speed after 'partial_dist'?
inline double SpeedAfterPartialDistance(DistanceType full_dist, double speed0,
                                        double speed1,
                                        DistanceType partial_dist) {
  CHECK_LE_S(partial_dist, full_dist);

  // sqrt(v0^2 + n/full (v1^2 - v0^2)).
  double v_square =
      (speed0 * speed0) + (partial_dist.meters() / full_dist.meters()) *
                              (speed1 * speed1 - speed0 * speed0);
  CHECK_GE_S(v_square, 0.0);
  return std::sqrt(v_square);
}
#endif

// Compute the average speed when decelerating (constant 'a_decel') before a
// curve that allows 'max_speed_curve' at point b. The average speed is
// computed for 'distance' leading to the curve. Speeds are in km/h.
//
// <-    distance     ->
// ====================+
// a                  b *
// mr                 mc *
//                        *
//                         *
//                          *
// a = start point on road, allowing max speed mr.
// b = point of curve, allowing max speed mc
// mr = max_speed_road
// mc = max_speed_curve
//
// There are 3 cases:
// 1) mr <= mc: return mr
// 2) mr > mc and 'distance' is long enough to actually decelerate
// 3) mr > mc and 'distance' is to short to decelerate, mr needs to be smaller.
inline double avg_speed_before_curve(DistanceType distance,
                                     double max_speed_road,
                                     double max_speed_curve, double a_decel) {
  if (max_speed_road <= max_speed_curve || distance == DistanceType(0u)) {
    // Case 1)
    return max_speed_road;
  }
  // We know: max_speed_road > max_speed_curve.
  DistanceType dist_needed =
      DistanceForSpeedChange(max_speed_road, max_speed_curve, a_decel);
  if (dist_needed <= distance) {
    // Case 2)
    const double avg_speed =
        ComputeAverageSpeed(distance - dist_needed, max_speed_road, dist_needed,
                            (max_speed_road + max_speed_curve) / 2.0);
    return avg_speed;
  }

  // Case 3)
  // We know: max_speed_road > max_speed_curve and
  //          dist_needed    > distance
  //
  // Compute the maximal speed at point 'a' that allows to decelerate to
  // 'curve_speed' over 'distance'.
  //
  // We actually solve the inverse problem, having same result:
  //     Start with curve speed and acceleration -a_decel (which is > 0) over
  //     distance d, what is the resulting speed?
  const double speed_at_a =
      SpeedAfterDistance(max_speed_curve, distance, -a_decel);
  const double avg_speed = (speed_at_a + max_speed_curve) / 2;
  return avg_speed;
}

// Compute the average speed when accelerating to 'max_speed_road' after a curve
// that allows 'max_speed_curve'.
// Speeds are in km/h.
//
// mc = max_speed_curve
// mr = max_speed_road
//
// There are 3 cases:
// 1) mc >= mr: return mr.
// 2) mc < mr and 'distance' is long enough to actually accelerate to mr
// 3) mc < mr and 'distance' is to short to accelerate to mr.
inline double avg_speed_after_curve(DistanceType distance,
                                    double max_speed_road,
                                    double max_speed_curve, double a_accel) {
  if (max_speed_road <= max_speed_curve || distance == DistanceType(0u)) {
    // Case 1)
    return max_speed_road;
  }

  DistanceType dist_needed =
      DistanceForSpeedChange(max_speed_curve, max_speed_road, a_accel);
  if (dist_needed <= distance) {
    // Case 2)
    // We know: max_speed_road > max_speed_curve.
    const double avg_speed =
        ComputeAverageSpeed(distance - dist_needed, max_speed_road, dist_needed,
                            (max_speed_road + max_speed_curve) / 2.0);
    return avg_speed;
  }

  // Case 3)
  // We know: max_speed_road > max_speed_curve and
  //          dist_needed    > distance
  // Compute the final speed after accelerating, after travelling 'distance'.
  // Acceleration is positive.
  const double final_speed =
      SpeedAfterDistance(max_speed_curve, distance, a_accel);
  const double avg_speed = (max_speed_curve + final_speed) / 2;
  return avg_speed;
}

// The time loss in a curve is computed from the maximum curve speed and the
// distance (and time) that is needed to decelerate/accelerate to/from the curve
// speed.
struct CurveStats {
  double avg_speed_in;
  double avg_speed_out;
  DurationMS time_loss_in;
  DurationMS time_loss_out;
  DurationMS time_loss_total;
  double curve_speed;
};

// Given an incoming and outgoing edge, compute the time loss an both
// legs given the slowdown necessary because of the curve.
// maxspeed[01]:  Maximum allowed speed on incoming and outgoing leg.
// length[01]:    Length of incoming and outgoing leg.
// turn_angle:    Angle between the legs, as computed by GEdge::GetTurnAngle().
inline CurveStats ComputeCurveLoss(double maxspeed0, double maxspeed1,
                                   DistanceType length0, DistanceType length1,
                                   int16_t turn_angle, double a_accel,
                                   double a_decel) {
  // We don't need the sign for now.
  turn_angle = std::labs(turn_angle);

  // Assume that normal drivers achieve 50% of the max possible velocity.
  constexpr double AvgDriverFactor = 0.5;
  // Assume that the angle has to be driven in 10m, i.e. 5m before and 5m
  // after the curve point.
  const double curve_speed =
      AvgDriverFactor * MaxCurveVelocity(DistanceType(10u * 100u), turn_angle);

  // LOG_S(INFO) << "CC1: length0:" << length0 << " length1:" << length1;
  CurveStats res = {.avg_speed_in = avg_speed_before_curve(
                        length0, maxspeed0, curve_speed, a_decel),
                    .avg_speed_out = avg_speed_after_curve(
                        length1, maxspeed1, curve_speed, a_accel),
                    .curve_speed = curve_speed};
  {
    DurationMS t_normal = TimeForDistance(length0, maxspeed0);
    DurationMS t_slow = TimeForDistance(length0, res.avg_speed_in);
    if (t_slow > t_normal) {
      res.time_loss_in = t_slow - t_normal;
    }
  }
  {
    DurationMS t_normal = TimeForDistance(length1, maxspeed1);
    DurationMS t_slow = TimeForDistance(length1, res.avg_speed_out);
    if (t_slow > t_normal) {
      res.time_loss_out = t_slow - t_normal;
    }
  }
  res.time_loss_total = res.time_loss_in + res.time_loss_out;
#if 0
  LOG_S(INFO) << absl::StrFormat(
      "CurveLoss(): ms0:%.1f ms1:%.1f l0:%.2fm l1:%.2fm angle:%d "
      "cusp:%.1f loss0:%ums loss1:%ums avgsp0:%.1f avgsp1:%.1f",
      maxspeed0, maxspeed1, length0.meters(), length1.meters(), turn_angle,
      curve_speed, res.time_loss_in.ms(), res.time_loss_out.ms(),
      res.avg_speed_in, res.avg_speed_out);
#endif

  return res;
}

// Call ComputeCurveLoss() above with graph data.
inline CurveStats ComputeCurveLoss(const Graph& g, VEHICLE vt,
                                   const N3Path& n3p, double a_accel,
                                   double a_decel, bool debug) {
  const GEdge& e0 = n3p.edge0(g);
  const GEdge& e1 = n3p.edge1(g);
  const uint32_t maxspeed0 =
      GetRAFromWSA(GetWSA(g, e0.way_idx), vt, EDGE_DIR(e0)).maxspeed;
  const uint32_t maxspeed1 =
      GetRAFromWSA(GetWSA(g, e1.way_idx), vt, EDGE_DIR(e1)).maxspeed;
  CurveStats res =
      ComputeCurveLoss(maxspeed0, maxspeed1, e0.distance / 2, e1.distance / 2,
                       e0.GetTurnAngle(e1), a_accel, a_decel);
  if (debug) {
    LOG_S(INFO) << absl::StrFormat(
        "CurveLoss(): ms0:%.1f ms1:%.1f l0:%.2fm l1:%.2fm angle:%d "
        "cusp:%.1f loss0:%ums loss1:%ums avgsp0:%.1f avgsp1:%.1f n3p:%s",
        maxspeed0, maxspeed1, (e1.distance / 2).meters(),
        (e1.distance / 2).meters(), e0.GetTurnAngle(e1), res.curve_speed,
        res.time_loss_in.ms(), res.time_loss_out.ms(), res.avg_speed_in,
        res.avg_speed_out, n3p.DebugStr(g));
  }

  return res;
}

// Check if access through a node is blocked.
// Special case:
//   If the access at the node is restricted (for instance "destination"), then
//   the incoming and outgoing ways have to be investigated. At least one of
//   them should have the same access, otherwise the node can't be traversed.
bool VehicleBlockedAtNode(const Graph& g, VEHICLE vt, const NodeTags* node_tags,
                          const N3Path& n3p) {
  if (node_tags == nullptr || (RoutableFullAccess(node_tags->acc_forw) &&
                               RoutableFullAccess(node_tags->acc_backw))) {
    return false;  // not blocked.
  }

  // TODO: handle direction? It is not clear how a direction on a bollard makes
  // any sense.
  if (!RoutableAccess(node_tags->acc_forw) ||
      !RoutableAccess(node_tags->acc_backw)) {
    return true;  // blocked.
  }

  // We know that at least one of the accesses is not "full" (and both are not
  // ACC_NO) from above, i.e. it is restricted. Find the lowest value of
  // restriction.
  ACCESS min_acc = std::min(node_tags->acc_forw, node_tags->acc_backw);
  // Check that either the incoming or outgoing way has the same access,
  // otherwise the vehicle is blocked.

  if (GetRAFromEdge(g, n3p.edge0(g), vt).access == min_acc ||
      GetRAFromEdge(g, n3p.edge1(g), vt).access == min_acc) {
    LOG_S(INFO) << "VehicleBlockedAtNode(): allow access acc:"
                << AccessToStringSafe(min_acc) << " " << n3p.DebugStr(g);
    return false;  // not blocked.
  } else {
    LOG_S(INFO) << "VehicleBlockedAtNode(): forbid access acc:"
                << AccessToStringSafe(min_acc) << " " << n3p.DebugStr(g);
    return true;  // blocked.
  }
}

// Is the U-Turn represented by 'n3p' allowed?
//
// The code checks for various situations, including when a vehicle would be
// trapped at the target node with no way to continue the travel, which is true
// at the end of a street or when facing a restricted access area.
//
// TODO: handle vehicle types properly.
inline bool IsUTurnAllowed(const Graph& g, VEHICLE vt,
                           const NodeTags* node_tags, const N3Path& n3p) {
  CHECK_EQ_S(n3p.node0_idx, n3p.node2_idx);

  if (node_tags != nullptr && node_tags->bit_turning_circle) {
    // LOG_S(INFO) << "TT1 Allowed UTurn " << n3p.DebugStr(g);
    return true;
  }

  const GEdge& edge0 = n3p.edge0(g);
  const GWay& way0 = g.ways.at(edge0.way_idx);

  // TODO: Is it clear that TRUNK and higher should have no automatic u-turns?
  if (way0.highway_label <= HW_TRUNK_LINK) {
    // LOG_S(INFO) << "TT1b Not allowed UTurn " << n3p.DebugStr(g);
    return false;
  }

  // Special case: Way is an area and both edges are on this way.
  if (way0.area && edge0.way_idx == n3p.edge1(g).way_idx) {
    // LOG_S(INFO) << "TT2 Allowed UTurn " << n3p.DebugStr(g);
    return true;
  }

  // Special case, vehicle is blocked at node and returns on the same way.
  if (VehicleBlockedAtNode(g, vt, node_tags, n3p) &&
      edge0.way_idx == n3p.edge1(g).way_idx) {
    // LOG_S(INFO) << "TT3 Allowed UTurn " << n3p.DebugStr(g);
    return true;
  }

  // Now check which kind of continuation edges there are.
  bool found_continuation = false;
  bool found_free_continuation = false;
  for (const GEdge& out : gnode_forward_edges(g, edge0.target_idx)) {
    if (out.target_idx != n3p.node0_idx && out.target_idx != edge0.target_idx) {
      found_continuation = true;
      found_free_continuation |= (out.car_label == GEdge::LABEL_FREE);
    }
  }

  // Vehicle can't continue except for returning.
  if (!found_continuation) {
    // LOG_S(INFO) << "TT4 Allowed UTurn " << n3p.DebugStr(g);
    return true;
  }
  // when arriving through a free edge, it is allowed to do a u-turn if there
  // are only restricted edges to continue on.
  if (edge0.car_label == GEdge::LABEL_FREE && !found_free_continuation) {
    // LOG_S(INFO) << "TT5 Allowed UTurn " << n3p.DebugStr(g);
    return true;
  }
  // LOG_S(INFO) << "TT5b not allowed UTurn " << n3p.DebugStr(g);
  return false;
}

// Check if there are turn restrictions matching the first leg in n3p. Use this
// to determine if the second leg in n3p is allowed or not.
//
// Return TRStatus::EMPTY if no turn restriction was found, or the TRStatus of
// the second edge in n3p.
inline TRStatus CheckSimpleTurnRestriction(
    const Graph& g, const IndexedTurnRestrictions& indexed_trs,
    const N3Path& n3p) {
  const GEdge& e0 = n3p.edge0(g);
  const std::span<const TurnRestriction> trs =
      indexed_trs.FindTurnRestrictions({.from_node_idx = n3p.node0_idx,
                                        .way_idx = e0.way_idx,
                                        .to_node_idx = e0.target_idx});
  if (trs.empty()) {
    return TRStatus::EMPTY;
  }

  const GEdge& e1 = n3p.edge1(g);
  if (n3p.node0_idx == n3p.node2_idx) {  // u-turn
    // Handle u-turns separately, because they are not implicitly
    // allowed/forbidden when another direction is forbidden/allowed.
    // Only use turn restrictions if they match the second leg.
    for (const TurnRestriction& tr : trs) {
      if (e1.way_idx == tr.path.back().way_idx &&
          e1.target_idx == tr.path.back().to_node_idx) {
        return tr.forbidden ? TRStatus::FORBIDDEN : TRStatus::ALLOWED;
      }
    }
    return TRStatus::EMPTY;
  } else {
    // Use all turn restrictions matching the first leg. If the second leg is
    // different, then the result is inverted. If there is a direct mach of the
    // second leg, then always use this value.
    TRStatus result = TRStatus::EMPTY;
    for (const TurnRestriction& tr : trs) {
      CHECK_EQ_S(tr.path.size(), 2) << tr.relation_id;
      const bool matches_edge = (e1.way_idx == tr.path.back().way_idx &&
                                 e1.target_idx == tr.path.back().to_node_idx);
      if (tr.forbidden == matches_edge) {
        result = TRStatus::FORBIDDEN;
      } else {
        result = TRStatus::ALLOWED;
      }
      if (matches_edge) {
        // We have a direct match of the second leg. Use this value anyways.
        break;
      }
    }
    if (result == TRStatus::EMPTY) {
      LOG_S(INFO) << absl::StrFormat(
          "Warning, no match found for turn restriction %lld",
          trs.front().relation_id);
    }
    return result;
  }
}

// Compute costs for obstacles that are not blocking but "cost" time.
// TODO: Differentiate for different vehicle types?
DurationMS NodeTagsCost(const Graph& g, const N3Path& n3p) {
  DurationMS cost(0u);  // unit is millisecond

  const GEdge& e0 = n3p.edge0(g);
  const GNode& node1 = n3p.node1(g);

  if (e0.stop_sign) {
    cost += 3'000;
  }
  if (e0.traffic_signal) {
    cost += 20'000;
  }
  if (node1.is_pedestrian_crossing) {
    cost += 3'000;
  }

  const NodeTags* attr = g.FindNodeTags(n3p.node1(g).node_id);
  if (attr != nullptr) {
    if (attr->bit_railway_crossing) {
      cost += 500;
      if (attr->bit_railway_crossing_barrier) {
        // Check if the previous node is closer than 50m and already had a
        // railway barrier. If so, then discount the current "barrier", it
        // probably doesn't exist.
        // See https://www.openstreetmap.org/node/103007646
        const NodeTags* attr_prev = g.FindNodeTags(n3p.node0(g).node_id);
        if (attr_prev == nullptr || !attr_prev->bit_railway_crossing ||
            !attr_prev->bit_railway_crossing_barrier ||
            n3p.edge0(g).distance.cm() > 5000) {
          // TODO: Maybe guess how busy the railway is? This is very inexact!
          cost += 40'000;
        }
      }
    }
    if (attr->bit_traffic_calming || attr->barrier_type != BARRIER_MAX) {
      // Assume every type of traffic calming or barrier slows down traffic by
      // one second.
      cost += 1'000;
    }
  }
  return cost;
}

// The cost that occurs when entering a new way, i.e. when turning from way A
// onto way B. Currently this is used only for ways with oneway 'reversible',
// i.e. alternating traffic over a long period.
DurationMS EnterNewWayCost(const Graph& g, VEHICLE vt, const N3Path& n3p) {
  if (vt != VH_FOOT) {
    const uint32_t way_idx0 = n3p.full_edge0().gedge(g).way_idx;
    const uint32_t way_idx1 = n3p.full_edge1().gedge(g).way_idx;
    if (way_idx0 != way_idx1 &&
        g.way_ids_with_oneway_reversible.contains(way_idx1) &&
        !g.way_ids_with_oneway_reversible.contains(way_idx0)) {
      return DurationMS(30u * 60u * 1000u);  // 30 minutes.
    }
  }
  return DurationMS(0u);
}

// Compute the (uncompressed) turn cost for the specific turn 'n3p'.
inline DurationMS ComputeTurnCostForN3Path(
    const Graph& g, VEHICLE vt, const IndexedTurnRestrictions& indexed_trs,
    const N3Path& n3p) {
  const bool debug = n3p.node1(g).node_id == 0;

  if (debug) {
    LOG_S(INFO) << "Compute turn cost for " << n3p.DebugStr(g);
  }

  const TRStatus tr_status = CheckSimpleTurnRestriction(g, indexed_trs, n3p);
  if (tr_status == TRStatus::FORBIDDEN) {
    if (debug) {
      LOG_S(INFO) << "  Cost infinity " << TURN_COST_INFINITY;
    }
    // If the leg is explicitly forbidden (also u-turns), then block it.
    return TURN_COST_INFINITY;
  }

  // Is this a u-turn that is forbidden, given the type of crossing at the
  // middle node? Note that a positive turn restriction from above always
  // allows a u-turn.
  const NodeTags* node_tags = g.FindNodeTags(n3p.node1(g).node_id);
  const bool uturn = (n3p.node0_idx == n3p.node2_idx);
  if (uturn) {
    if (tr_status == TRStatus::ALLOWED) {
      // LOG_S(INFO) << "FF1 Allowed UTurn " << n3p.DebugStr(g);
      // Explicitly allowed by turn restriction.
      if (debug) {
        LOG_S(INFO) << "  Cost u turn " << TURN_COST_U_TURN;
      }
      return TURN_COST_U_TURN;
    }
    if (!IsUTurnAllowed(g, vt, node_tags, n3p)) {
      if (debug) {
        LOG_S(INFO) << "  Cost infinity " << TURN_COST_INFINITY;
      }
      return TURN_COST_INFINITY;
    }
    // LOG_S(INFO) << "FF2 Allowed UTurn " << n3p.DebugStr(g);
    if (debug) {
      LOG_S(INFO) << "  Cost u turn " << TURN_COST_U_TURN;
    }
    return TURN_COST_U_TURN;
  }

  // Check if we're blocked by the middle node. We know this is not a u-turn.
  if (VehicleBlockedAtNode(g, vt, node_tags, n3p)) {
    const GWay& way0 = g.ways.at(n3p.edge0(g).way_idx);
    if (!way0.area || n3p.edge0(g).way_idx != n3p.edge1(g).way_idx) {
      if (debug) {
        LOG_S(INFO) << "  Cost infinity " << TURN_COST_INFINITY;
      }
      return TURN_COST_INFINITY;
    }
  }

  // So far we know we can do the turn and it is not a u-turn.
  //
  // Compute three time losses and use the maximum:
  // 1) Time loss because of node (stop sign, signals, etc.)
  // 2) Time loss because of curve.
  // 3) Real crossing
  // 4) Entering a new way. Currently this has costs of 30m when entering a
  // way with direction 'reversible'.

  const DurationMS cost_node_tags = NodeTagsCost(g, n3p);
  const DurationMS cost_crossing = CrossingCost(g, vt, n3p, debug);
  const DurationMS cost_curve =
      ComputeCurveLoss(g, vt, n3p, GetVHAccel(vt), GetVHDecel(vt), debug)
          .time_loss_total;
  const DurationMS cost_enter_new_way = EnterNewWayCost(g, vt, n3p);
  const DurationMS cost =
      std::max({cost_node_tags, cost_curve, cost_crossing, cost_enter_new_way});

  if (debug) {
    LOG_S(INFO) << "  Cost node tags " << cost_node_tags;
    LOG_S(INFO) << "  Cost crossing " << cost_crossing;
    LOG_S(INFO) << "  Cost curve " << cost_curve;
    LOG_S(INFO) << "  Cost enter new way " << cost_enter_new_way;
    LOG_S(INFO) << "  Cost max " << cost;
  }
  return cost;
}

}  // namespace

// Compute all turn costs for an edge at the target node.
// vt: type of the vehicle.
// indexed_trs: Container containing the simple turn restrictions.
// fe: The edge for which we compute turn costs at the target node.
// Returns the resulting turn costs data for fe.target_node(g).
inline TurnCostData ComputeTurnCostsForEdge(
    const Graph& g, VEHICLE vt, const IndexedTurnRestrictions& indexed_trs,
    const FullEdge fe) {
  // Create a tcd with the needed dimension.
  const GNode& crossing_node = fe.target_node(g);
  TurnCostData tcd(crossing_node.num_forward_edges, TURN_COST_ZERO_COMPRESSED);

  for (uint32_t off = 0; off < crossing_node.num_forward_edges; ++off) {
    tcd.turn_costs.at(off) = compress_turn_cost(ComputeTurnCostForN3Path(
        g, vt, indexed_trs, N3Path::Create(g, fe, {fe.target_idx(g), off})));
  }
  return tcd;
}
