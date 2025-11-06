// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.api;

import io.r2dbc.spi.ConnectionMetadata;

public interface SingleStoreConnectionMetadata extends ConnectionMetadata {

  @Override
  String getDatabaseProductName();

  @Override
  String getDatabaseVersion();

  /**
   * Indicate if server does have required version.
   *
   * @param major major version
   * @param minor minor version
   * @param patch patch version
   * @return true is database version is equal or more than indicated version
   */
  boolean minVersion(int major, int minor, int patch);

  /**
   * Indicate server major version. in 10.5.4, return 10
   *
   * @return server major version
   */
  int getMajorVersion();

  /**
   * Indicate server minor version. in 10.5.4, return 5
   *
   * @return server minor version
   */
  int getMinorVersion();

  /**
   * Indicate server patch version. in 10.5.4, return 4
   *
   * @return server patch version
   */
  int getPatchVersion();
}
