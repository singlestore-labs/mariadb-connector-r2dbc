// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package org.mariadb.r2dbc.message;

import io.netty.buffer.ByteBuf;

public interface AuthMoreData {

  MessageSequence getSequencer();

  ByteBuf getBuf();
}
