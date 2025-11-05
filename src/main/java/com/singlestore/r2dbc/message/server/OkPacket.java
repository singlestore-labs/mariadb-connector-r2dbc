// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.message.server;

import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.Result;
import com.singlestore.r2dbc.message.Context;
import com.singlestore.r2dbc.message.ServerMessage;
import com.singlestore.r2dbc.util.BufferUtils;
import com.singlestore.r2dbc.util.constants.ServerStatus;
import reactor.util.Logger;
import reactor.util.Loggers;

public class OkPacket implements ServerMessage, Result.UpdateCount {
  public static final byte TYPE = (byte) 0x00;
  private static final Logger logger = Loggers.getLogger(OkPacket.class);
  private final long affectedRows;
  private final long lastInsertId;
  private final short serverStatus;
  private final short warningCount;
  private final boolean ending;

  public OkPacket(
      long affectedRows,
      long lastInsertId,
      short serverStatus,
      short warningCount,
      final boolean ending) {
    this.affectedRows = affectedRows;
    this.lastInsertId = lastInsertId;
    this.serverStatus = serverStatus;
    this.warningCount = warningCount;
    this.ending = ending;
  }

  public static OkPacket decode(ByteBuf buf, Context context) {
    buf.skipBytes(1);
    long affectedRows = BufferUtils.readLengthEncodedInt(buf);
    long lastInsertId = BufferUtils.readLengthEncodedInt(buf);
    short serverStatus = buf.readShortLE();
    short warningCount = buf.readShortLE();
    context.setServerStatus(serverStatus);

    return new OkPacket(
        affectedRows,
        lastInsertId,
        serverStatus,
        warningCount,
        (serverStatus & ServerStatus.MORE_RESULTS_EXISTS) == 0);
  }

  public long getLastInsertId() {
    return lastInsertId;
  }

  public short getServerStatus() {
    return serverStatus;
  }

  public short getWarningCount() {
    return warningCount;
  }

  @Override
  public boolean ending() {
    return this.ending;
  }

  @Override
  public boolean resultSetEnd() {
    return true;
  }

  @Override
  public long value() {
    return affectedRows;
  }

  @Override
  public String toString() {
    return "OkPacket{"
        + "affectedRows="
        + affectedRows
        + ", lastInsertId="
        + lastInsertId
        + ", serverStatus="
        + serverStatus
        + ", warningCount="
        + warningCount
        + ", ending="
        + ending
        + '}';
  }
}
