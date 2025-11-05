// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.unit.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import com.singlestore.r2dbc.util.SingleStoreType;

public class SingleStoreTypeTest {

  @Test
  void getName() {
    Assertions.assertEquals("VARCHAR", SingleStoreType.VARCHAR.getName());
    Assertions.assertEquals("BIGINT", SingleStoreType.UNSIGNED_BIGINT.getName());
    Assertions.assertEquals("UNSIGNED_BIGINT", SingleStoreType.UNSIGNED_BIGINT.name());
  }
}
