#pragma once

#include <fstream>
#include <string_view>

#include "absl/strings/str_split.h"
#include "base/country_code.h"
#include "base/util.h"
#include "graph/routing_attrs.h"

class PerCountryConfig {
 public:
  PerCountryConfig(bool log_details = false)
      : country_defaults_(MAX_NCC, nullptr), log_details_(log_details) {
    Clear();
  }
  ~PerCountryConfig() { Clear(); }

  void ReadConfig(const std::string& filename) {
    LOG_S(INFO) << "Load routing info config from " << filename;
    Clear();
    std::ifstream file(filename);
    if (!file.is_open()) {
      perror(filename.c_str());
      exit(EXIT_FAILURE);
    }

    for (std::string line; std::getline(file, line);) {
      if (line.empty() || absl::StartsWith(line, "#")) {
        continue;
      }
      ApplyConfigLine(line);
    }
    PostProcess();
    LOG_S(INFO) << "Finished loading config";
  }

  bool ApplyConfigLine(std::string_view line) {
    std::vector<std::string_view> elements =
        absl::StrSplit(line, ' ', absl::SkipEmpty());
    CHECK_EQ_S(elements.size(), 2) << "Expecting 2 pieces in <" << line << ">";
    const Operation op = GetOperation(elements[1], line);

    std::vector<std::string_view> keys = absl::StrSplit(elements[0], ':');
    CHECK_GT_S(keys.size(), 0)
        << "Expecting at least one key in <" << line << ">";
    CountryDefaults* arr = &global_defaults_;
    if (keys[0] != "ALL") {
      uint16_t cc_num = TwoLetterCountryCodeToNum(keys[0]);
      CHECK_S(cc_num > 0 && cc_num < MAX_NCC)
          << "Invalid country code in <" << line << ">";
      if (country_defaults_.at(cc_num) == nullptr) {
        country_defaults_.at(cc_num) =
            (CountryDefaults*)malloc(sizeof(CountryDefaults));
        // Copy initial values from global defaults.
        memcpy(country_defaults_.at(cc_num), &global_defaults_,
               sizeof(CountryDefaults));
      }
      arr = country_defaults_.at(cc_num);
    }
    const Selector sel = ParseKeys(keys, line);
    Apply(sel, op, *arr);
    return true;
  }

  void PostProcess() {
    PostProcessRoutingAttrs(global_defaults_);
    for (auto ptr : country_defaults_) {
      if (ptr != nullptr) {
        PostProcessRoutingAttrs(*ptr);
      }
    }
  }

  RoutingAttrs GetDefault(uint16_t cc_num, HIGHWAY_LABEL hw, VEHICLE vh,
                         ENVIRONMENT_TYPE et, IS_MOTORROAD im) const {
    CHECK_NE_S(hw, HW_MAX);
    CHECK_NE_S(vh, VH_MAX);
    CHECK_NE_S(et, ET_MAX);
    CHECK_NE_S(im, IM_MAX);
    if (country_defaults_.at(cc_num) != nullptr) {
      return GetDefaultsFrom(hw, vh, et, im, *country_defaults_.at(cc_num));
    } else {
      return GetDefaultsFrom(hw, vh, et, im, global_defaults_);
    }
  }

 private:
  typedef RoutingAttrs CountryDefaults[HW_MAX][VH_MAX][ET_MAX][IM_MAX];
  struct Selector {
    std::vector<VEHICLE> vts;
    std::vector<HIGHWAY_LABEL> hws;
    IS_MOTORROAD im = IM_MAX;
    ENVIRONMENT_TYPE et = ET_ANY;
  };
  struct Operation {
    std::string op_access;
    std::string op_maxspeed;
    ACCESS access = ACC_MAX;
    uint16_t maxspeed = 0;
  };

  // Set maxspeed to 0 whenever there is no access.
  void PostProcessRoutingAttrs(CountryDefaults& arr) {
    for (size_t hw = 0; hw < HW_MAX; ++hw) {
      for (size_t vh = 0; vh < VH_MAX; ++vh) {
        for (size_t et = ET_ANY; et < ET_MAX; ++et) {
          for (size_t im = 0; im < IM_MAX; ++im) {
            RoutingAttrs& ri = arr[hw][vh][et][im];
            if (ri.access == ACC_NO) {
              ri.maxspeed = 0;
            }
          }
        }
      }
    }
  }

  static RoutingAttrs GetDefaultsFrom(HIGHWAY_LABEL hw, VEHICLE vh,
                                     ENVIRONMENT_TYPE et, IS_MOTORROAD im,
                                     const CountryDefaults& arr) {
    return arr[hw][vh][et][im];
  }

  void Clear() {
    LOG_S(INFO) << "sizeof(CountryDefaults):" << sizeof(CountryDefaults);
    std::memset(&global_defaults_, 0, sizeof(global_defaults_));
    for (size_t i = 0; i < country_defaults_.size(); ++i) {
      if (country_defaults_.at(i) != nullptr) {
        free(country_defaults_.at(i));
        country_defaults_.at(i) = nullptr;
      }
    }
  }

  Selector ParseKeys(const std::vector<std::string_view>& keys,
                     std::string_view line) {
    Selector sel;
    for (size_t i = 1; i < keys.size(); ++i) {
      if (sel.vts.empty()) {
        VEHICLE vh = PrefixedVehicleToEnum(keys[i]);
        if (vh != VH_MAX) {
          sel.vts.push_back(vh);
          continue;
        }
      }
      if (keys[i] == "vh_motorized" && sel.vts.empty()) {
        for (VEHICLE vh = VH_MOTOR_VEHICLE; vh < VH_MAX;
             vh = (VEHICLE)(vh + 1)) {
          if (VehicleIsMotorized(vh)) {
            sel.vts.push_back(vh);
          }
        }
        continue;
      }
      if (keys[i] == "vh_not_motorized" && sel.vts.empty()) {
        for (VEHICLE vh = VH_MOTOR_VEHICLE; vh < VH_MAX;
             vh = (VEHICLE)(vh + 1)) {
          if (!VehicleIsMotorized(vh)) {
            sel.vts.push_back(vh);
          }
        }
        continue;
      }
      if (sel.hws.empty()) {
        if (absl::EndsWith(keys[i], "*")) {
          std::string_view prefix = keys[i];
          prefix.remove_suffix(1);
          sel.hws = HighwayLabelsByPrefix(prefix);
          continue;
        } else {
          HIGHWAY_LABEL hw = HighwayLabelToEnum(keys[i]);
          if (hw != HW_MAX) {
            sel.hws.push_back(hw);
            continue;
          }
        }
      }
      if (keys[i] == "motorroad" && sel.im == IM_MAX) {
        // Special case, there is not way to set IM_NO specifically.
        sel.im = IM_YES;
        continue;
      }
      if (keys[i] == "rural" && sel.et == ET_ANY) {
        sel.et = ET_RURAL;
        continue;
      }
      if (keys[i] == "urban" && sel.et == ET_ANY) {
        sel.et = ET_URBAN;
        continue;
      }
      ABORT_S() << "Error parsing key <" << keys[i] << "> in line <" << line
                << ">";
    }
    // Fill all vehicle types if they haven't been restricted.
    if (sel.vts.empty()) {
      for (VEHICLE vh = VH_MOTOR_VEHICLE; vh < VH_MAX; vh = (VEHICLE)(vh + 1)) {
        sel.vts.push_back(vh);
      }
    }
    // Fill all highways if they haven't been restricted.
    if (sel.hws.empty()) {
      sel.hws = HighwayLabelsByPrefix("");
    }

    return sel;
  }

  Operation GetOperation(std::string_view value,
                         std::string_view line) const {
    Operation op;

    if (ConsumePrefixIf("access=", &value)) {
      op.op_access = "set";
      op.access = AccessToEnum(value);
      CHECK_NE_S(op.access, ACC_MAX) << "Bad access in <" << line << ">";
    } else if (ConsumePrefixIf("speed_max=", &value)) {
        op.op_maxspeed = "set";
        if (!ParseNumericMaxspeed(value, &op.maxspeed)) {
          ABORT_S() << "Invalid speed value in <" << line << ">";
        }
    } else if (ConsumePrefixIf("speed_limit=", &value)) {
        op.op_maxspeed = "limit";
        if (!ParseNumericMaxspeed(value, &op.maxspeed)) {
          ABORT_S() << "Invalid speed value in <" << line << ">";
        }
    } else {
      ABORT_S() << "Unknown value in <" << line << ">";
    }
    /* TODO
    if (log_details_) {
      LOG_S(INFO) << access_str << ":" << maxspeed_str;
      LOG_S(INFO) << op.op_access << ":" << op.op_maxspeed;
    }
    */
    return op;
  }

  void Apply(const Selector& sel, const Operation& op, CountryDefaults& arr) {
    for (HIGHWAY_LABEL hw : sel.hws) {
      for (VEHICLE vh : sel.vts) {
        for (ENVIRONMENT_TYPE et = ET_ANY; et < ET_MAX;
             et = (ENVIRONMENT_TYPE)(et + 1)) {
          if (sel.et != ET_ANY && sel.et != et) {
            continue;
          }
          for (IS_MOTORROAD im = IM_NO; im < IM_MAX;
               im = (IS_MOTORROAD)(im + 1)) {
            if (sel.im != IM_MAX && sel.im != im) {
              continue;
            }
            // RoutingAttrs defaults_[VH_MAX][HW_MAX][ET_MAX][IM_MAX];
            if (op.op_access == "set") {
              arr[hw][vh][et][im].access = op.access;
            } else {
              CHECK_S(op.op_access.empty()) << op.op_access;
            }

            if (op.op_maxspeed == "set") {
              if (log_details_) {
                LOG_S(INFO) << absl::StrFormat(
                    "Set abs maxspeed %u for hw=%s vh=%s et=%u im=%u",
                    op.maxspeed, VehicleToString(vh), HighwayLabelToString(hw),
                    et, im);
              }
              arr[hw][vh][et][im].maxspeed = op.maxspeed;
            } else if (op.op_maxspeed == "limit") {
              if (log_details_) {
                LOG_S(INFO) << absl::StrFormat(
                    "Set limit maxspeed %u for hw=%s vh=%s et=%u im=%u",
                    op.maxspeed, HighwayLabelToString(hw), VehicleToString(vh),
                    et, im);
              }
              arr[hw][vh][et][im].maxspeed =
                  std::min(op.maxspeed, arr[hw][vh][et][im].maxspeed);
            } else {
              CHECK_S(op.op_maxspeed.empty()) << op.op_maxspeed;
            }
          }
        }
      }
    }
  }

  std::vector<CountryDefaults*> country_defaults_;
  CountryDefaults global_defaults_;
  bool log_details_;
};
