// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.ConnectionMetadata;
import org.junit.jupiter.api.Test;
import com.singlestore.r2dbc.BaseConnectionTest;
import com.singlestore.r2dbc.api.SingleStoreConnectionMetadata;

public class ConnectionMetadataTest extends BaseConnectionTest {

  @Test
  void connectionMeta() {
    ConnectionMetadata meta = sharedConn.getMetadata();
    assertEquals(meta.getDatabaseProductName(), "SingleStore");
    assertTrue(
        meta.getDatabaseVersion().contains("7.")
            || meta.getDatabaseVersion().contains("8.")
            || meta.getDatabaseVersion().contains("9."));
  }

  @Test
  void factoryMeta() {
    ConnectionFactoryMetadata meta = factory.getMetadata();
    assertEquals("SingleStore", meta.getName());
  }

  @Test
  void metadataInfo() {
    SingleStoreConnectionMetadata meta = sharedConn.getMetadata();
    assertTrue(meta.getMajorVersion() >= 5);
    assertTrue(meta.getMinorVersion() > -1);
    assertTrue(meta.getPatchVersion() > -1);
  }
}
