#pragma once

#include <charconv>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

/*
 * Parse commandline arguments.
 * Features:
 *   supports both '-' or '--' as prefix, such as "-name=aha" or '--name=aha'.
 *   supports both "--name=aha and "--name aha"
 *   supports string, int, bool and double types.
 *   required string arguments can't have empty values.
 *   numeric values (int, bool, double) are type-checked when being parsed.
 *   supports --bool_flag without value and --bool_flag=true|false|1|0.
 *   supports positional arguments, such as '<filename> --verbose'
 *   does not support concatenation of arguments as in "ls -la"
 *   does not support lists of values (but it would be simple to add).
 *   needs C++20 or newer.
 *
 * Example:
 *   int main(int argc, char* argv[]) {
 *     Argli argli(argc, argv,
 *         {{.name="user", .type="string", .required=true},
 *          {.name="verbose", .type = "bool", .dflt = "true"},
 *          {.name="filename", .type="string", .positional=true, .required=true}
 *          });
 *     // Error reporting happens before this line is reached.
 *     std::string user = argli.GetString("user");
 *     bool verbose = argli.GetBool("verbose");
 *     std::string filename = argli.GetString("filename");
 *     ...
 *   }
 */
class Argli {
 public:
  struct ArgDef {
    std::string name;
    std::string type;  // "string", "int", "bool" or "double".
    bool positional = false;
    bool required = false;  // true if argument must be specified.
    std::string dflt;       // default value if any. The default of the default
                            // is "", 0, false and 0.0 for the four types.
    std::string desc;       // description of the argument.
  };

  // Parse commandline arguments. Aborts with an error message if arguments are
  // missing or if values can't be decoded.
  Argli(int argc, char* argv[], const std::vector<ArgDef>& defined_args) {
    const std::string errmsg = InternalParse(argc, argv, defined_args);
    if (!errmsg.empty()) {
      Fail(errmsg);
    }
  }

  bool ArgIsSet(std::string_view name) const { return FindArgConst(name).done; }

  const std::string& GetString(std::string_view name) const {
    return FindArgConst(name).value;
  }

  int64_t GetInt(std::string_view name) const {
    return FindTypedArgConst(name, "int").value_int;
  }

  bool GetBool(std::string_view name) const {
    return FindTypedArgConst(name, "bool").value_bool;
  }

  double GetDouble(std::string_view name) const {
    return FindTypedArgConst(name, "double").value_double;
  }

  std::string Usage() const {
    std::ostringstream msg;
    msg << std::endl << "Usage: " << prog_ << " [options...]" << std::endl;
    msg << "Options:" << std::endl;
    for (const ArgValue& arg : args_) {
      if (arg.def.positional && arg.def.required) {
        msg << "  <" << arg.def.name << ">";
      } else if (arg.def.positional && !arg.def.required) {
        msg << "  [<" << arg.def.name << ">]";
      } else if (!arg.def.positional && arg.def.required) {
        msg << "  --" << arg.def.name << "=<value>";
      } else if (!arg.def.positional && !arg.def.required) {
        msg << "  [--" + arg.def.name << "=<value>]";
      }
      msg << " (" << arg.def.type << ", default:\"" << arg.def.dflt << "\")"
          << std::endl;
      for (std::string_view text = arg.def.desc; !text.empty();) {
        msg << "     " << WrapLine(&text, 75) << std::endl;
      }
      // TODO: bool has special syntax
    }
    return msg.str();
  }

 private:
  friend class TestArgli;
  friend void TestArglis();

  struct ArgValue {
    ArgDef def;
    bool done;
    std::string value;
    int64_t value_int = 0;
    bool value_bool = false;
    double value_double = 0.0;
  };
  std::vector<ArgValue> args_;
  std::string prog_;

  Argli() {}

  // Find argument with a given name. Return nullptr if not found.
  ArgValue* FindArg(std::string_view name) {
    for (ArgValue& arg : args_) {
      if (arg.def.name == name) return &arg;
    }
    return nullptr;
  }

  // Find argument with a given name.
  // found.
  const ArgValue& FindArgConst(std::string_view name) const {
    for (const ArgValue& arg : args_) {
      if (arg.def.name == name) return arg;
    }
    Fail(ErrMsg("arg <", name, "> is undefined"));
    exit(1);
  }

  // Find argument with a given name and type.
  const ArgValue& FindTypedArgConst(std::string_view name,
                                    std::string_view type) const {
    const ArgValue& arg = FindArgConst(name);
    if (arg.def.type != type) {
      Fail(ErrMsg("arg <", name, "> is not of type <", type, ">"));
    }
    return arg;
  }

  // Print error information and Usage(), then exit.
  // This uses: https://en.wikipedia.org/wiki/Variadic_template.
  template <typename... Args>
  std::string ErrMsg(Args&&... args) const {
    std::ostringstream msg;
    (msg << std::dec << ... << args);
    return msg.str();
  }

  // Print error information and Usage(), then exit.
  void Fail(const std::string& msg) const {
    std::cerr << Usage() << std::endl;
    std::cerr << "Error: " << msg << std::endl;
    exit(1);
  }

  // Parse commandline arguments.
  // Returns a non-empty error message on error, and empty string on success.
  std::string InternalParse(int argc, char* argv[],
                            const std::vector<ArgDef>& defined_args) {
    if (argc < 1) return ErrMsg("unexpected argc value: ", argc);
    args_.clear();
    for (ArgDef arg : defined_args) {
      if (arg.type == "int") {
        if (arg.dflt.empty()) arg.dflt = "0";
      } else if (arg.type == "bool") {
        if (arg.dflt.empty()) arg.dflt = "false";
      } else if (arg.type == "double") {
        if (arg.dflt.empty()) arg.dflt = "0.0";
      } else if (arg.type != "string") {
        return ErrMsg("arg <", arg.name, "> has invalid type <", arg.type, ">");
      }
      args_.push_back({.def = arg, .done = false, .value = arg.dflt});
    }
    prog_ = argv[0];

    for (int pos = 1; pos < argc; ++pos) {
      std::string_view name = argv[pos];
      ArgValue* found_arg = nullptr;
      bool has_hyphen = name.starts_with('-');

      if (!has_hyphen) {
        // Find next available positional argument.
        for (ArgValue& arg : args_) {
          if (arg.def.positional && !arg.done) {
            found_arg = &arg;
            found_arg->value = name;
            break;
          }
        }
        if (found_arg == nullptr) {
          return ErrMsg("missing positional arg <", name, ">");
        }
      } else {
        name.remove_prefix(1);
        if (name.starts_with('-')) name.remove_prefix(1);
        auto eq_pos = name.find('=');
        std::string_view value;
        if (eq_pos != name.npos) {
          value = name.substr(eq_pos + 1);
          name = name.substr(0, eq_pos);
        }
        // Now search the matching definition
        found_arg = FindArg(name);
        if (found_arg == nullptr) {
          return ErrMsg("arg <", name, "> is undefined");
        }
        if (found_arg->done) {
          return ErrMsg("redefinition of arg in <", argv[pos], ">");
        }
        if (found_arg->def.positional) {
          return ErrMsg("invalid use of positional arg in <", argv[pos], ">");
        }
        if (eq_pos == name.npos) {
          if (found_arg->def.type == "bool") {
            value = "true";  // --foo is the same as --foo=1|true
          } else {
            // Value is in next commandline arg.
            if (pos + 1 >= argc) {
              return ErrMsg("missing value of arg <", argv[pos], ">");
            }
            value = argv[++pos];
          }
        }
        found_arg->value = value;
      }
      found_arg->done = true;
    }

    for (ArgValue& arg : args_) {
      if (arg.def.required && (!arg.done || arg.value.empty())) {
        return ErrMsg("arg <", arg.def.name, "> is required");
      }
      // Convert typed value from string
      std::string_view v = arg.value;
      if (arg.def.type == "bool") {
        arg.value_bool = (v == "1" || v == "true");
        if (!arg.value_bool && v != "0" && v != "false") {
          return ErrMsg("arg <", arg.def.name, ">: can't convert <", v,
                        "> to bool");
        }
      } else if (arg.def.type == "int") {
        if (std::from_chars(v.data(), v.data() + v.size(), arg.value_int).ec !=
            std::errc()) {
          return ErrMsg("arg <", arg.def.name, ">: can't convert <", v,
                        "> to integer");
        }
      } else if (arg.def.type == "double") {
        if (std::from_chars(v.data(), v.data() + v.size(), arg.value_double)
                .ec != std::errc()) {
          return ErrMsg("arg <", arg.def.name, ">: can't convert <", v,
                        "> to double");
        }
      }
    }
    return "";
  }

  // Returns a line of at most 'width' characters from text. Wraps at <space>
  // or hard cuts text if no <space> is found.
  static std::string_view WrapLine(std::string_view* text, size_t width) {
    if (text->size() <= width) {
      auto line = *text;
      *text = "";
      return line;
    }
    size_t cut = text->rfind(' ', width);
    auto line = text->substr(0, cut != std::string_view::npos ? cut : width);
    *text = text->substr(cut != std::string_view::npos ? cut + 1 : width);
    return line;
  }
};
