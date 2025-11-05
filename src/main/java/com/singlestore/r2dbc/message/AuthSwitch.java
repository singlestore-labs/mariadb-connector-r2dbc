// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.message;

public interface AuthSwitch {

  String getPlugin();

  byte[] getSeed();

  MessageSequence getSequencer();
}
