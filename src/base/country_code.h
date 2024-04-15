#pragma once

#include <charconv>
#include <string_view>

#include "absl/strings/str_cat.h"
#include "base/util.h"

// Positive integer value that represents the two letters in 'two_letter_code'
// and can be decoded with CountryNumToTwoLetter().
// 0 represents the invalid country.
inline constexpr uint16_t TwoLetterCountryCodeToNum(
    std::string_view two_letter_code) {
  // CHECK_EQ_S(two_letter_code.size(), 2u);
  if (two_letter_code.size() != 2) return 0;
  int16_t n0 = two_letter_code.at(0) - 'A';
  int16_t n1 = two_letter_code.at(1) - 'A';
  // CHECK_S(n0 < 26) << two_letter_code;
  // CHECK_S(n1 < 26) << two_letter_code;
  if (n0 < 0 || n0 >= 26 || n1 < 0 || n1 >= 26) return 0;
  return ((n0 + 1) << 5) + (n1 + 1);
}

constexpr uint16_t INVALID_NCC = 0;
constexpr uint16_t MAX_NCC = (26 << 5) + 26 + 1;
constexpr uint16_t NCC_CH = TwoLetterCountryCodeToNum("CH");
constexpr uint16_t NCC_DE = TwoLetterCountryCodeToNum("DE");
constexpr uint16_t NCC_AT = TwoLetterCountryCodeToNum("AT");
constexpr uint16_t NCC_FR = TwoLetterCountryCodeToNum("FR");
constexpr uint16_t NCC_LI = TwoLetterCountryCodeToNum("LI");

// Decode the country num to the ISO two-letter code such as "DE".
// 0 is decoded to "--".
inline bool CountryNumToTwoLetter(uint16_t country_num, char* two_letter_code) {
  if (country_num == 0) {
    two_letter_code[0] = '-';
    two_letter_code[1] = '-';
    two_letter_code[2] = 0;
    return false;
  }
  CHECK_LT_S(country_num, 1u << 10) << country_num;
  char n0 = country_num >> 5;
  char n1 = country_num & 31;
  CHECK_S(n0 > 0 && n1 > 0) << country_num;
  two_letter_code[0] = (n0 - 1 + 'A');
  two_letter_code[1] = (n1 - 1 + 'A');
  two_letter_code[2] = 0;
  return true;
}

inline std::string CountryNumToString(uint16_t country_num) {
  char arr[3];
  if (CountryNumToTwoLetter(country_num, arr)) {
    return arr;
  }
  return absl::StrCat("CC(", country_num, ")");
}
