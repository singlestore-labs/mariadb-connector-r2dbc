// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package org.mariadb.r2dbc;

import io.r2dbc.spi.Result;

public class MariadbUpdateCount implements Result.UpdateCount {
  public MariadbUpdateCount() {}

  @Override
  public long value() {
    return 0;
  }
}
