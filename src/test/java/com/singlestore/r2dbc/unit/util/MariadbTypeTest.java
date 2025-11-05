// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.unit.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import com.singlestore.r2dbc.util.MariadbType;

public class MariadbTypeTest {

  @Test
  void getName() {
    Assertions.assertEquals("VARCHAR", MariadbType.VARCHAR.getName());
    Assertions.assertEquals("BIGINT", MariadbType.UNSIGNED_BIGINT.getName());
    Assertions.assertEquals("UNSIGNED_BIGINT", MariadbType.UNSIGNED_BIGINT.name());
  }
}
