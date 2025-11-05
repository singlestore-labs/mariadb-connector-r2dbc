// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc;

import com.singlestore.r2dbc.client.ServerVersion;

public final class SingleStoreConnectionMetadata
    implements com.singlestore.r2dbc.api.SingleStoreConnectionMetadata {

  private final ServerVersion version;

  SingleStoreConnectionMetadata(ServerVersion version) {
    this.version = version;
  }

  @Override
  public String getDatabaseProductName() {
    return this.version.isMariaDBServer() ? "MariaDB" : "MySQL";
  }

  public boolean isMariaDBServer() {
    return this.version.isMariaDBServer();
  }

  public boolean minVersion(int major, int minor, int patch) {
    return this.version.versionGreaterOrEqual(major, minor, patch);
  }

  public int getMajorVersion() {
    return version.getMajorVersion();
  }

  public int getMinorVersion() {
    return version.getMinorVersion();
  }

  public int getPatchVersion() {
    return version.getPatchVersion();
  }

  @Override
  public String getDatabaseVersion() {
    return this.version.getServerVersion();
  }
}
