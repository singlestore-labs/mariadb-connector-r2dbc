// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.message.server;

import com.singlestore.r2dbc.message.ServerMessage;
import com.singlestore.r2dbc.util.ServerPrepareResult;

public final class CompletePrepareResult implements ServerMessage {

  private final ServerPrepareResult prepare;
  private final boolean continueOnEnd;

  public CompletePrepareResult(final ServerPrepareResult prepare, boolean continueOnEnd) {
    this.prepare = prepare;
    this.continueOnEnd = continueOnEnd;
  }

  @Override
  public boolean ending() {
    return !continueOnEnd;
  }

  public ServerPrepareResult getPrepare() {
    return prepare;
  }
}
