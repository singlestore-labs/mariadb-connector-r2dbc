// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.authentication.standard;

import com.singlestore.r2dbc.SingleStoreConnectionConfiguration;
import com.singlestore.r2dbc.authentication.AuthenticationPlugin;
import com.singlestore.r2dbc.message.AuthMoreData;
import com.singlestore.r2dbc.message.ClientMessage;
import com.singlestore.r2dbc.message.client.ClearPasswordPacket;
import com.singlestore.r2dbc.message.server.Sequencer;

public final class PamPluginFlow implements AuthenticationPlugin {

  public static final String TYPE = "dialog";
  private int counter = -1;

  public PamPluginFlow create() {
    return new PamPluginFlow();
  }

  public String type() {
    return TYPE;
  }

  public ClientMessage next(
      SingleStoreConnectionConfiguration configuration,
      byte[] seed,
      Sequencer sequencer,
      AuthMoreData authMoreData) {
    while (true) {
      counter++;
      if (counter == 0) {
        return new ClearPasswordPacket(sequencer, configuration.getPassword());
      } else {
        if (configuration.getPamOtherPwd() == null) {
          throw new IllegalArgumentException(
              "PAM authentication is set with multiple password, but pamOtherPwd option wasn't"
                  + " set");
        }
        if (configuration.getPamOtherPwd().length < counter) {
          throw new IllegalArgumentException(
              String.format(
                  "PAM authentication required at least %s passwords, but pamOtherPwd has only %s",
                  counter, configuration.getPamOtherPwd().length));
        }
        return new ClearPasswordPacket(sequencer, configuration.getPamOtherPwd()[counter - 1]);
      }
    }
  }
}
