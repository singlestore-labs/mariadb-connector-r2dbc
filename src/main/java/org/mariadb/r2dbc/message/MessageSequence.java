// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package org.mariadb.r2dbc.message;

public interface MessageSequence {
  byte next();

  void reset();
}
