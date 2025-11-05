// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.unit.client;

import org.junit.jupiter.api.Test;
import com.singlestore.r2dbc.client.ServerVersion;

public class ServerVersionTest {
  @Test
  void testMinVersion() {
    ServerVersion sv = new ServerVersion("10.2.25-mariadb", true);
    assertEquals(10, sv.getMajorVersion());
    assertEquals(2, sv.getMinorVersion());
    assertEquals(25, sv.getPatchVersion());
    assertTrue(sv.versionGreaterOrEqual(9, 8, 8));
    assertTrue(sv.versionGreaterOrEqual(10, 1, 8));
    assertTrue(sv.versionGreaterOrEqual(10, 2, 8));
    assertTrue(sv.versionGreaterOrEqual(10, 2, 25));
    assertFalse(sv.versionGreaterOrEqual(19, 8, 8));
    assertFalse(sv.versionGreaterOrEqual(10, 3, 8));
    assertFalse(sv.versionGreaterOrEqual(10, 2, 30));

    ServerVersion sv2 = new ServerVersion("10.2.25", true);
    assertEquals(10, sv2.getMajorVersion());
    assertEquals(2, sv2.getMinorVersion());
    assertEquals(25, sv2.getPatchVersion());
  }
}
