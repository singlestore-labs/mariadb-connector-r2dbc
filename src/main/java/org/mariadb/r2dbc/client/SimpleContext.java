// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package org.mariadb.r2dbc.client;

import io.netty.buffer.ByteBufAllocator;
import org.mariadb.r2dbc.message.Context;

public class SimpleContext implements Context {

  private final long threadId;
  private final long serverCapabilities;
  private final long clientCapabilities;
  private final ServerVersion version;
  private final ByteBufAllocator byteBufAllocator;
  private short serverStatus;
  private String database;

  public SimpleContext(
      String serverVersion,
      long threadId,
      long capabilities,
      short serverStatus,
      boolean mariaDBServer,
      long clientCapabilities,
      String database,
      ByteBufAllocator byteBufAllocator) {

    this.threadId = threadId;
    this.serverCapabilities = capabilities;
    this.clientCapabilities = clientCapabilities;
    this.serverStatus = serverStatus;
    this.version = new ServerVersion(serverVersion, mariaDBServer);
    this.database = database;
    this.byteBufAllocator = byteBufAllocator;
  }

  public long getThreadId() {
    return threadId;
  }

  public long getServerCapabilities() {
    return serverCapabilities;
  }

  public long getClientCapabilities() {
    return clientCapabilities;
  }

  public short getServerStatus() {
    return serverStatus;
  }

  public void setServerStatus(short serverStatus) {
    this.serverStatus = serverStatus;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public ServerVersion getVersion() {
    return version;
  }

  public ByteBufAllocator getByteBufAllocator() {
    return byteBufAllocator;
  }

  @Override
  public String toString() {
    return "ConnectionContext{" + "threadId=" + threadId + ", version=" + version + '}';
  }
}
