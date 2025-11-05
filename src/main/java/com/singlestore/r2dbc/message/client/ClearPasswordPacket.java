// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.message.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.nio.charset.StandardCharsets;
import com.singlestore.r2dbc.message.ClientMessage;
import com.singlestore.r2dbc.message.Context;
import com.singlestore.r2dbc.message.MessageSequence;
import reactor.core.publisher.Mono;

public final class ClearPasswordPacket implements ClientMessage {

  private final CharSequence password;
  private final MessageSequence sequencer;

  public ClearPasswordPacket(MessageSequence sequencer, CharSequence password) {
    this.sequencer = sequencer;
    this.password = password;
  }

  @Override
  public Mono<ByteBuf> encode(Context context, ByteBufAllocator allocator) {
    if (password == null) return Mono.just(allocator.ioBuffer(0));
    ByteBuf buf = allocator.ioBuffer(password.length() * 4);
    buf.writeCharSequence(password, StandardCharsets.UTF_8);
    buf.writeByte(0);
    return Mono.just(buf);
  }

  @Override
  public MessageSequence getSequencer() {
    return sequencer;
  }
}
