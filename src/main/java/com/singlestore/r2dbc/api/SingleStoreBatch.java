// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.api;

import io.r2dbc.spi.Batch;
import reactor.core.publisher.Flux;

public interface SingleStoreBatch extends Batch {

  @Override
  SingleStoreBatch add(String sql);

  @Override
  Flux<SingleStoreResult> execute();
}
