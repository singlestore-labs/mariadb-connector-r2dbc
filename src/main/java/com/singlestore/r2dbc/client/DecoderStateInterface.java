// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.client;

import io.netty.buffer.ByteBuf;
import com.singlestore.r2dbc.message.ServerMessage;
import com.singlestore.r2dbc.message.server.Sequencer;

public interface DecoderStateInterface {

  default DecoderState decoder(short val, int len) {
    return (DecoderState) this;
  }

  default ServerMessage decode(ByteBuf body, Sequencer sequencer, MariadbFrameDecoder decoder) {
    throw new IllegalArgumentException("unexpected state");
  }

  default DecoderState next(MariadbFrameDecoder decoder) {
    return null;
  }
}
