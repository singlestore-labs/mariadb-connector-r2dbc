// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.api;

import io.r2dbc.spi.Batch;
import reactor.core.publisher.Flux;

public interface MariadbBatch extends Batch {

  @Override
  MariadbBatch add(String sql);

  @Override
  Flux<MariadbResult> execute();
}
