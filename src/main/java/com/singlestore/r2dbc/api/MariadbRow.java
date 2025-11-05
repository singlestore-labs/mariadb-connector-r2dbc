// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.api;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import com.singlestore.r2dbc.client.MariadbRowMetadata;

/** A {@link Row} for a MariaDB/MySQL database. */
public interface MariadbRow extends Row {

  /**
   * Returns the {@link RowMetadata} for all columns in this row.
   *
   * @return the {@link RowMetadata} for all columns in this row
   * @since 0.9
   */
  MariadbRowMetadata getMetadata();
}
