// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.authentication;

import io.r2dbc.spi.R2dbcException;
import com.singlestore.r2dbc.MariadbConnectionConfiguration;
import com.singlestore.r2dbc.message.AuthMoreData;
import com.singlestore.r2dbc.message.ClientMessage;
import com.singlestore.r2dbc.message.server.Sequencer;

public interface AuthenticationPlugin {

  String type();

  AuthenticationPlugin create();

  ClientMessage next(
      MariadbConnectionConfiguration configuration,
      byte[] seed,
      Sequencer sequencer,
      AuthMoreData authMoreData)
      throws R2dbcException;
}
