// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.authentication.addon;

import com.singlestore.r2dbc.SingleStoreConnectionConfiguration;
import com.singlestore.r2dbc.authentication.AuthenticationPlugin;
import com.singlestore.r2dbc.message.AuthMoreData;
import com.singlestore.r2dbc.message.ClientMessage;
import com.singlestore.r2dbc.message.client.ClearPasswordPacket;
import com.singlestore.r2dbc.message.server.Sequencer;

public final class ClearPasswordPluginFlow implements AuthenticationPlugin {

  public static final String TYPE = "mysql_clear_password";

  public ClearPasswordPluginFlow create() {
    return new ClearPasswordPluginFlow();
  }

  public String type() {
    return TYPE;
  }

  public ClientMessage next(
      SingleStoreConnectionConfiguration configuration,
      byte[] seed,
      Sequencer sequencer,
      AuthMoreData authMoreData) {
    return new ClearPasswordPacket(sequencer, configuration.getPassword());
  }
}
