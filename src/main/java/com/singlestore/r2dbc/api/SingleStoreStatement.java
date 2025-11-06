// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.api;

import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;

public interface SingleStoreStatement extends Statement {

  @Override
  SingleStoreStatement add();

  @Override
  SingleStoreStatement bind(String identifier, Object value);

  @Override
  SingleStoreStatement bind(int index, Object value);

  @Override
  SingleStoreStatement bindNull(String identifier, Class<?> type);

  @Override
  SingleStoreStatement bindNull(int index, Class<?> type);

  @Override
  Flux<SingleStoreResult> execute();

  @Override
  default SingleStoreStatement fetchSize(int rows) {
    return this;
  }

  @Override
  SingleStoreStatement returnGeneratedValues(String... columns);
}
