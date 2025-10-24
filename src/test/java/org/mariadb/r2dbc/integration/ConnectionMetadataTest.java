// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.ConnectionMetadata;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.api.MariadbConnectionMetadata;

public class ConnectionMetadataTest extends BaseConnectionTest {

  @Test
  void connectionMeta() {
    ConnectionMetadata meta = sharedConn.getMetadata();
    assertEquals(meta.getDatabaseProductName(), isMariaDBServer() ? "MariaDB" : "MySQL");
    assertTrue(
        meta.getDatabaseVersion().contains("7.")
            || meta.getDatabaseVersion().contains("8.")
            || meta.getDatabaseVersion().contains("9."));
  }

  @Test
  void factoryMeta() {
    ConnectionFactoryMetadata meta = factory.getMetadata();
    assertEquals("MariaDB", meta.getName());
  }

  @Test
  void metadataInfo() {
    MariadbConnectionMetadata meta = sharedConn.getMetadata();
    assertTrue(meta.getMajorVersion() >= 5);
    assertTrue(meta.getMinorVersion() > -1);
    assertTrue(meta.getPatchVersion() > -1);
  }
}
