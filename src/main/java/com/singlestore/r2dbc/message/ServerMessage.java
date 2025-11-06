// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.message;

public interface ServerMessage {

  default boolean ending() {
    return false;
  }

  default boolean resultSetEnd() {
    return false;
  }

  default int refCnt() {
    return -1000;
  }

  default boolean release() {
    return true;
  }
}
