// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.message.server;

import com.singlestore.r2dbc.message.MessageSequence;

public class Sequencer implements MessageSequence {
  private byte sequenceId;

  public Sequencer(byte sequenceId) {
    this.sequenceId = sequenceId;
  }

  public void reset() {
    sequenceId = (byte) 0xff;
  }

  public byte next() {
    return ++sequenceId;
  }
}
