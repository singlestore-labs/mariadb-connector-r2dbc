// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.message.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.nio.charset.StandardCharsets;
import com.singlestore.r2dbc.message.ClientMessage;
import com.singlestore.r2dbc.message.Context;
import com.singlestore.r2dbc.message.MessageSequence;
import com.singlestore.r2dbc.message.server.Sequencer;
import com.singlestore.r2dbc.util.Assert;
import reactor.core.publisher.Mono;

public final class PreparePacket implements ClientMessage {
  private final String sql;
  private final MessageSequence sequencer = new Sequencer((byte) 0xff);

  public PreparePacket(String sql) {
    this.sql = Assert.requireNonNull(sql, "query must not be null");
  }

  public String getSql() {
    return sql;
  }

  public MessageSequence getSequencer() {
    return sequencer;
  }

  public void resetSequencer() {
    sequencer.reset();
  }

  @Override
  public Mono<ByteBuf> encode(Context context, ByteBufAllocator allocator) {
    ByteBuf buf = allocator.ioBuffer(this.sql.length() + 1);
    buf.writeByte(0x16);
    buf.writeCharSequence(this.sql, StandardCharsets.UTF_8);
    return Mono.just(buf);
  }
}
