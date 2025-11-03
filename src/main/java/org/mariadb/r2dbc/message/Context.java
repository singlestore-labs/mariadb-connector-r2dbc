// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.mariadb.r2dbc.client.ServerVersion;

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
