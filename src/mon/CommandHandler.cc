// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat Ltd
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "CommandHandler.h"

#include "common/strtol.h"
#include "include/ceph_assert.h"

#include <ostream>
#include <string>
#include <string_view>
#include <bitset>
#include <climits>

int CommandHandler::parse_bool(std::string_view str, bool* result, std::ostream& ss)
{
  ceph_assert(result != nullptr);

  std::string interr;
  int64_t n = strict_strtoll(str.data(), 10, &interr);

  if (str == "false" || str == "no"
      || (interr.length() == 0 && n == 0)) {
    *result = false;
    return 0;
  } else if (str == "true" || str == "yes"
      || (interr.length() == 0 && n == 1)) {
    *result = true;
    return 0;
  } else {
    ss << "value must be false|no|0 or true|yes|1";
    return -EINVAL;
  }
}

int CommandHandler::parse_hex(std::string hex_string, std::string &bin_string, unsigned int max_bits, std::ostream& ss)
{
  const static unsigned int BITS_PER_QUATET = CHAR_BIT / 2;
  const static unsigned int BITS_PER_ULLONG = sizeof(unsigned long long) * CHAR_BIT ;
  const static unsigned int QUATETS_PER_ULLONG = BITS_PER_ULLONG/BITS_PER_QUATET;
  unsigned int offset = 0;

  std::transform(hex_string.begin(), hex_string.end(), hex_string.begin(), ::tolower);

  if (hex_string.substr(0, 2) == "0x") {
    offset = 2;
  }

  for (unsigned int i = offset; i < hex_string.size(); i += QUATETS_PER_ULLONG) {
    unsigned long long value;
    try {
      value = stoull(hex_string.substr(i, QUATETS_PER_ULLONG), nullptr, 16);
    } catch (std::invalid_argument const& ex) {
      ss << "invalid hex value " << max_bits;
      return -EINVAL;
    }
    std::bitset<BITS_PER_ULLONG> bit_str(value);
    bin_string += bit_str.to_string();
  }

  if (bin_string.length() > max_bits) {
    ss << "a value exceeds max_bits " << max_bits;
    return -EINVAL;
  }

  if (bin_string.find("1") == std::string::npos) {
    ss << "at least one rank must be set";
    return -EINVAL;
  }

  if (bin_string.length() < 256)
    bin_string.insert(0, 256 - bin_string.length(), '0');

  std::reverse(bin_string.begin(), bin_string.end());

  return 0;
}
