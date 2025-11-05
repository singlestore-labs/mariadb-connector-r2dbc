// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import com.singlestore.r2dbc.client.ServerVersion;

public interface Context {

  long getThreadId();

  long getServerCapabilities();

  long getClientCapabilities();

  short getServerStatus();

  void setServerStatus(short serverStatus);

  String getDatabase();

  void setDatabase(String database);

  ServerVersion getVersion();

  ByteBufAllocator getByteBufAllocator();

  default void saveRedo(ClientMessage msg, ByteBuf buf, int initialReaderIndex) {}
}
