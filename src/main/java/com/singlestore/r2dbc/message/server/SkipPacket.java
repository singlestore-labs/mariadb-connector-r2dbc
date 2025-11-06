// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.message.server;

import com.singlestore.r2dbc.message.ServerMessage;

public class SkipPacket implements ServerMessage {

  private final boolean ending;

  public SkipPacket(boolean ending) {
    this.ending = ending;
  }

  public static SkipPacket decode(boolean ending) {
    return new SkipPacket(ending);
  }

  @Override
  public boolean ending() {
    return this.ending;
  }

  public boolean resultSetEnd() {
    return this.ending;
  }

  public Sequencer getSequencer() {
    return null;
  }
}
