// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.codec;

public enum DataType {
  OLDDECIMAL(0),
  TINYINT(1),
  SMALLINT(2),
  INTEGER(3),
  FLOAT(4),
  DOUBLE(5),
  NULL(6),
  TIMESTAMP(7),
  BIGINT(8),
  MEDIUMINT(9),
  DATE(10),
  TIME(11),
  DATETIME(12),
  YEAR(13),
  NEWDATE(14),
  TEXT(15),
  BIT(16),
  JSON(245),
  DECIMAL(246),
  ENUM(247),
  SET(248),
  TINYBLOB(249),
  MEDIUMBLOB(250),
  LONGBLOB(251),
  BLOB(252),
  VARSTRING(253),
  STRING(254),
  GEOMETRY(255),

  // SingleStoreDB extended types
  BSON(1001),
  FLOAT32_VECTOR(2001),
  FLOAT64_VECTOR(2002),
  INT8_VECTOR(2003),
  INT16_VECTOR(2004),
  INT32_VECTOR(2005),
  INT64_VECTOR(2006);

  static final DataType[] basicTypeMap;

  static {
    basicTypeMap = new DataType[256];
    for (DataType v : values()) {
      if (v.singlestoreType < 256) {
        basicTypeMap[v.singlestoreType] = v;
      }
    }
  }

  private final short singlestoreType;

  DataType(int singlestoreType) {
    this.singlestoreType = (short) singlestoreType;
  }

  /**
   * Convert server Type to server type.
   *
   * @param typeValue type value
   * @param charsetNumber charset
   * @return SingleStore type
   */
  public static DataType fromServer(int typeValue, int charsetNumber) {

    DataType dataType = basicTypeMap[typeValue];

    if (charsetNumber != 63 && typeValue >= 249 && typeValue <= 252) {
      // SingleStore Text dataType
      return DataType.TEXT;
    }

    return dataType;
  }

  public short get() {
    return singlestoreType;
  }
}
