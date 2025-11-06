// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.message.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import com.singlestore.r2dbc.message.ClientMessage;
import com.singlestore.r2dbc.message.Context;
import reactor.core.publisher.Mono;

public final class PingPacket implements ClientMessage {

  @Override
  public Mono<ByteBuf> encode(Context context, ByteBufAllocator allocator) {
    ByteBuf buf = allocator.ioBuffer();
    buf.writeByte(0x0e);
    return Mono.just(buf);
  }

  @Override
  public String toString() {
    return "PingPacket{}";
  }
}
