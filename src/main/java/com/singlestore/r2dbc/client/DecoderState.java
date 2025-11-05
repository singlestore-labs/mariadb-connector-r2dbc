// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.client;

import io.netty.buffer.ByteBuf;
import com.singlestore.r2dbc.message.ServerMessage;
import com.singlestore.r2dbc.message.server.*;
import com.singlestore.r2dbc.util.ServerPrepareResult;

public enum DecoderState implements DecoderStateInterface {
  INIT_HANDSHAKE {
    @Override
    public DecoderState decoder(short val, int len) {
      if (val == 255) { // 0xFF
        return ERROR;
      }
      return this;
    }

    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, SingleStoreFrameDecoder decoder) {
      return InitialHandshakePacket.decode(sequencer, body);
    }
  },

  OK_PACKET {
    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, SingleStoreFrameDecoder decoder) {
      return OkPacket.decode(body, decoder.getContext());
    }

    @Override
    public DecoderState next(SingleStoreFrameDecoder decoder) {
      return QUERY_RESPONSE;
    }
  },

  AUTHENTICATION_SWITCH {
    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, SingleStoreFrameDecoder decoder) {
      return AuthSwitchPacket.decode(sequencer, body);
    }

    @Override
    public DecoderState next(SingleStoreFrameDecoder decoder) {
      return AUTHENTICATION_SWITCH_RESPONSE;
    }
  },

  AUTHENTICATION_SWITCH_RESPONSE {
    @Override
    public DecoderState decoder(short val, int len) {
      switch (val) {
        case 0:
          return OK_PACKET;
        case 254: // 0xFE
          return AUTHENTICATION_SWITCH;
        case 255: // 0xFF
          return ERROR;
        default:
          return AUTHENTICATION_MORE_DATA;
      }
    }
  },

  AUTHENTICATION_MORE_DATA {
    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, SingleStoreFrameDecoder decoder) {
      return AuthMoreDataPacket.decode(sequencer, body);
    }
  },

  QUERY_RESPONSE {
    @Override
    public DecoderState decoder(short val, int len) {
      switch (val) {
        case 0:
          return OK_PACKET;
        case 255: // 0xFF
          return ERROR;
        default:
          return COLUMN_COUNT;
      }
    }
  },

  COLUMN_COUNT {
    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, SingleStoreFrameDecoder decoder) {
      ColumnCountPacket columnCountPacket = ColumnCountPacket.decode(body, decoder.getContext());
      decoder.setStateCounter(columnCountPacket.getColumnCount());
      decoder.setMetaFollows(columnCountPacket.isMetaFollows());
      return columnCountPacket;
    }

    @Override
    public DecoderState next(SingleStoreFrameDecoder decoder) {
      if (decoder.isMetaFollows()) {
        return COLUMN_DEFINITION;
      }
      return EOF_INTERMEDIATE_RESPONSE;
    }
  },

  COLUMN_DEFINITION {
    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, SingleStoreFrameDecoder decoder) {
      decoder.decrementStateCounter();
      return ColumnDefinitionPacket.decode(body, false, decoder.getConf());
    }

    @Override
    public DecoderState next(SingleStoreFrameDecoder decoder) {
      if (decoder.getStateCounter() <= 0) {
        return EOF_INTERMEDIATE_RESPONSE;
      }
      return this;
    }
  },

  EOF_INTERMEDIATE_RESPONSE {
    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, SingleStoreFrameDecoder decoder) {
      return EofPacket.decode(body, decoder.getContext(), false);
    }

    @Override
    public DecoderState next(SingleStoreFrameDecoder decoder) {
      return ROW_RESPONSE;
    }
  },

  EOF_END {
    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, SingleStoreFrameDecoder decoder) {
      return EofPacket.decode(body, decoder.getContext(), true);
    }

    @Override
    public DecoderState next(SingleStoreFrameDecoder decoder) {
      return QUERY_RESPONSE;
    }
  },

  EOF_END_OUT_PARAM {
    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, SingleStoreFrameDecoder decoder) {
      // specific for mysql that break protocol, forgetting sometime to set PS_OUT_PARAMETERS and
      // more importantly MORE_RESULTS_EXISTS
      // breaking protocol
      return EofPacket.decodeOutputParam(body, decoder.getContext());
    }

    @Override
    public DecoderState next(SingleStoreFrameDecoder decoder) {
      return QUERY_RESPONSE;
    }
  },

  ROW_RESPONSE {
    @Override
    public DecoderState decoder(short val, int len) {
      switch (val) {
        case 254:
          if (len < 0xffffff) {
            return EOF_END;
          } else {
            // normal ROW
            return ROW;
          }
        case 255: // 0xFF
          return ERROR;
        default:
          return ROW;
      }
    }
  },

  ROW {
    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, SingleStoreFrameDecoder decoder) {
      return new RowPacket(body);
    }

    @Override
    public DecoderState next(SingleStoreFrameDecoder decoder) {
      return ROW_RESPONSE;
    }
  },

  PREPARE_RESPONSE {
    @Override
    public DecoderState decoder(short val, int len) {
      if (val == 255) { // 0xFF
        return ERROR;
      }
      return this;
    }

    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, SingleStoreFrameDecoder decoder) {
      decoder.setPrepare(PrepareResultPacket.decode(sequencer, body, decoder.getContext(), false));
      if (decoder.getPrepare().getNumParams() == 0 && decoder.getPrepare().getNumColumns() == 0) {
        ServerPrepareResult serverPrepareResult = decoder.endPrepare();
        return new CompletePrepareResult(serverPrepareResult, false);
      }
      return new SkipPacket(false);
    }

    @Override
    public DecoderState next(SingleStoreFrameDecoder decoder) {
      if (decoder.getPrepare().getNumParams() == 0) {
        if (decoder.getPrepare().getNumColumns() == 0) {
          decoder.setPrepare(null);
          return QUERY_RESPONSE;
        }
        decoder.setStateCounter(decoder.getPrepare().getNumColumns());
        return PREPARE_COLUMN;
      }
      // skip param and EOF
      decoder.setStateCounter(decoder.getPrepare().getNumParams());
      return PREPARE_PARAMETER;
    }
  },

  PREPARE_AND_EXECUTE_RESPONSE {
    @Override
    public DecoderState decoder(short val, int len) {
      if (val == 255) { // 0xFF
        return ERROR_AND_EXECUTE_RESPONSE;
      }
      return this;
    }

    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, SingleStoreFrameDecoder decoder) {
      decoder.setPrepare(PrepareResultPacket.decode(sequencer, body, decoder.getContext(), true));
      if (decoder.getPrepare().getNumParams() == 0 && decoder.getPrepare().getNumColumns() == 0) {
        ServerPrepareResult serverPrepareResult = decoder.endPrepare();
        return new CompletePrepareResult(serverPrepareResult, true);
      }
      return new SkipPacket(false);
    }

    @Override
    public DecoderState next(SingleStoreFrameDecoder decoder) {
      if (decoder.getPrepare().getNumParams() == 0) {
        if (decoder.getPrepare().getNumColumns() == 0) {
          decoder.setPrepare(null);
          return QUERY_RESPONSE;
        }
        decoder.setStateCounter(decoder.getPrepare().getNumColumns());
        return PREPARE_COLUMN;
      }
      // skip param and EOF
      decoder.setStateCounter(decoder.getPrepare().getNumParams());
      return PREPARE_PARAMETER;
    }
  },

  PREPARE_PARAMETER {
    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, SingleStoreFrameDecoder decoder) {
      decoder.decrementStateCounter();
      return SkipPacket.decode(false);
    }

    @Override
    public DecoderState next(SingleStoreFrameDecoder decoder) {
      if (decoder.getStateCounter() == 0) {
        return PREPARE_PARAMETER_EOF;
      }
      return this;
    }
  },

  PREPARE_PARAMETER_EOF {
    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, SingleStoreFrameDecoder decoder) {
      if (decoder.getPrepare().getNumColumns() == 0) {
        ServerPrepareResult serverPrepareResult = decoder.endPrepare();
        return new CompletePrepareResult(
            serverPrepareResult, decoder.getPrepare().isContinueOnEnd());
      }
      return SkipPacket.decode(false);
    }

    @Override
    public DecoderState next(SingleStoreFrameDecoder decoder) {
      if (decoder.getPrepare().getNumColumns() > 0) {
        decoder.setStateCounter(decoder.getPrepare().getNumColumns());
        return PREPARE_COLUMN;
      }
      decoder.setPrepare(null);
      return QUERY_RESPONSE;
    }
  },

  PREPARE_COLUMN {
    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, SingleStoreFrameDecoder decoder) {
      ColumnDefinitionPacket columnDefinitionPacket =
          ColumnDefinitionPacket.decode(body, false, decoder.getConf());
      decoder
              .getPrepareColumns()[
              decoder.getPrepare().getNumColumns() - decoder.getStateCounter()] =
          columnDefinitionPacket;
      decoder.decrementStateCounter();
      return SkipPacket.decode(false);
    }

    @Override
    public DecoderState next(SingleStoreFrameDecoder decoder) {
      if (decoder.getStateCounter() <= 0) {
        return PREPARE_COLUMN_EOF;
      }
      return this;
    }
  },

  PREPARE_COLUMN_EOF {
    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, SingleStoreFrameDecoder decoder) {
      boolean continueOnEnd = decoder.getPrepare().isContinueOnEnd();
      ServerPrepareResult prepareResult = decoder.endPrepare();
      return new CompletePrepareResult(prepareResult, continueOnEnd);
    }

    @Override
    public DecoderState next(SingleStoreFrameDecoder decoder) {
      return QUERY_RESPONSE;
    }
  },

  ERROR {
    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, SingleStoreFrameDecoder decoder) {
      return ErrorPacket.decode(sequencer, body, true);
    }

    @Override
    public DecoderState next(SingleStoreFrameDecoder decoder) {
      throw new IllegalArgumentException("unexpected state");
    }
  },

  ERROR_AND_EXECUTE_RESPONSE {
    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, SingleStoreFrameDecoder decoder) {
      return ErrorPacket.decode(sequencer, body, false);
    }

    @Override
    public DecoderState next(SingleStoreFrameDecoder decoder) {
      return SKIP_EXECUTE;
    }
  },

  SKIP_EXECUTE {
    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, SingleStoreFrameDecoder decoder) {
      decoder.decrementStateCounter();
      return SkipPacket.decode(true);
    }
  }
}
