// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.unit;

import org.junit.jupiter.api.Test;
import com.singlestore.r2dbc.codec.Codecs;
import com.singlestore.r2dbc.util.BufferUtils;
import com.singlestore.r2dbc.util.constants.Capabilities;
import com.singlestore.r2dbc.util.constants.ColumnFlags;
import com.singlestore.r2dbc.util.constants.ServerStatus;

public class InitFinalClass {

  @Test
  public void init() throws Exception {
    Codecs codecs = new Codecs();
    BufferUtils buf = new BufferUtils();
    Capabilities c = new Capabilities();
    ColumnFlags c2 = new ColumnFlags();
    ServerStatus c3 = new ServerStatus();
    System.out.println(codecs.hashCode() + buf.hashCode());
  }
}
