// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc;

import io.r2dbc.spi.Result;

public class SingleStoreUpdateCount implements Result.UpdateCount {
  public SingleStoreUpdateCount() {}

  @Override
  public long value() {
    return 0;
  }
}
