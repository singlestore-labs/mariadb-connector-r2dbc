// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.message.server;

import com.singlestore.r2dbc.client.util.ProtocolExtendedTypeCodes;
import com.singlestore.r2dbc.client.util.VectorType;
import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Nullability;
import java.nio.charset.StandardCharsets;
import com.singlestore.r2dbc.SingleStoreConnectionConfiguration;
import com.singlestore.r2dbc.codec.DataType;
import com.singlestore.r2dbc.message.ServerMessage;
import com.singlestore.r2dbc.util.CharsetEncodingLength;
import com.singlestore.r2dbc.util.SingleStoreType;
import com.singlestore.r2dbc.util.constants.ColumnFlags;

public final class ColumnDefinitionPacket
    implements ServerMessage, ColumnMetadata {
  private final String catalog;
  private final String schema;
  private final String table;
  private final String orgTable;
  private final String name;
  private final String orgName;
  private final String extTypeFormat;
  private final int charset;
  private final long length;
  private final DataType dataType;
  private final byte decimals;
  private final int flags;
  private final boolean ending;
  private final SingleStoreConnectionConfiguration conf;

  private ColumnDefinitionPacket(
      String catalog,
      String schema,
      String table,
      String orgTable,
      String name,
      String orgName,
      String extTypeFormat,
      int charset,
      long length,
      DataType dataType,
      byte decimals,
      int flags,
      boolean ending,
      SingleStoreConnectionConfiguration conf) {
    this.catalog = catalog;
    this.schema = schema;
    this.table = table;
    this.orgTable = orgTable;
    this.name = name;
    this.orgName = orgName;
    this.extTypeFormat = extTypeFormat;
    this.charset = charset;
    this.length = length;
    this.dataType = dataType;
    this.decimals = decimals;
    this.flags = flags;
    this.ending = ending;
    this.conf = conf;
  }

  private ColumnDefinitionPacket(String name, SingleStoreConnectionConfiguration conf) {
    this.catalog = "";
    this.schema = "";
    this.table = "";
    this.orgTable = "";
    this.name = name;
    this.orgName = name;
    this.extTypeFormat = null;
    this.charset = 33;
    this.length = 8;
    this.dataType = DataType.BIGINT;
    this.decimals = 0;
    this.flags = ColumnFlags.PRIMARY_KEY;
    this.ending = false;
    this.conf = conf;
  }

  private static String readShortString(ByteBuf buf) {
    int len = buf.readByte() & 0xff;
    return buf.readCharSequence(len, StandardCharsets.UTF_8).toString();
  }

  public static ColumnDefinitionPacket decode(
      ByteBuf buf, boolean ending, SingleStoreConnectionConfiguration conf) {
    String catalog = readShortString(buf);
    String schema = readShortString(buf);
    String table = readShortString(buf);
    String orgTable = readShortString(buf);
    String name = readShortString(buf);
    String orgName = readShortString(buf);

    int fixedLengthFields = buf.readByte();

    int charset = buf.readUnsignedShortLE();
    long length = buf.readUnsignedIntLE();
    DataType dataType = DataType.fromServer(buf.readUnsignedByte(), charset);
    int flags = buf.readUnsignedShortLE();
    byte decimals = buf.readByte();

    String extTypeFormat = null;
    if (fixedLengthFields > 12) {
      buf.skipBytes(2); // unused
      ProtocolExtendedTypeCodes typeCode = ProtocolExtendedTypeCodes.fromCode(buf.readByte());
      if (typeCode == ProtocolExtendedTypeCodes.VECTOR) {
        int dimensionsOfVector = buf.readIntLE();
        VectorType typeOfVectorElements = VectorType.fromCode(buf.readByte());
        dataType = typeOfVectorElements.getType();
        extTypeFormat = dimensionsOfVector + "," + dataType.name();
      } else if (typeCode == ProtocolExtendedTypeCodes.BSON) {
        dataType = DataType.BSON;
      }
    }

    return new ColumnDefinitionPacket(
        catalog, schema, table, orgTable, name, orgName, extTypeFormat,
        charset, length, dataType, decimals, flags, ending, conf);
  }

  public static ColumnDefinitionPacket fromGeneratedId(
      String name, SingleStoreConnectionConfiguration conf) {
    return new ColumnDefinitionPacket(name, conf);
  }

  public String getSchema() {
    return schema;
  }

  public String getTableAlias() {
    return table;
  }

  public String getTable() {
    return orgTable;
  }

  @Override
  public String getName() {
    return name;
  }

  public String getColumn() {
    return orgName;
  }

  public String getExtTypeFormat() {
    return extTypeFormat;
  }

  public int getCharset() {
    return charset;
  }

  public long getLength() {
    return length;
  }

  public DataType getDataType() {
    return dataType;
  }

  public byte getDecimals() {
    return decimals;
  }

  public boolean isSigned() {
    return ((flags & ColumnFlags.UNSIGNED) == 0);
  }

  public int getDisplaySize() {
    if (dataType == DataType.TEXT
        || dataType == DataType.JSON
        || dataType == DataType.ENUM
        || dataType == DataType.SET
        || dataType == DataType.VARSTRING
        || dataType == DataType.STRING) {
      return (int)
          (length
              / (CharsetEncodingLength.maxCharlen.get(charset) == 0
                  ? 1
                  : CharsetEncodingLength.maxCharlen.get(charset)));
    }
    return (int) length;
  }

  public Nullability getNullability() {
    return (flags & ColumnFlags.NOT_NULL) > 0 ? Nullability.NON_NULL : Nullability.NULLABLE;
  }

  public boolean isPrimaryKey() {
    return ((this.flags & ColumnFlags.PRIMARY_KEY) > 0);
  }

  public boolean isUniqueKey() {
    return ((this.flags & ColumnFlags.UNIQUE_KEY) > 0);
  }

  public boolean isMultipleKey() {
    return ((this.flags & ColumnFlags.MULTIPLE_KEY) > 0);
  }

  public boolean isBlob() {
    return ((this.flags & ColumnFlags.BLOB) > 0);
  }

  public boolean isZeroFill() {
    return ((this.flags & ColumnFlags.ZEROFILL) > 0);
  }

  // doesn't use & 128 bit filter, because char binary and varchar binary are not binary (handle
  // like string), but have the binary flag
  public boolean isBinary() {
    return (charset == 63);
  }

  public SingleStoreType getType() {
    switch (dataType) {
      case TINYINT:
        return isSigned() ? SingleStoreType.TINYINT : SingleStoreType.UNSIGNED_TINYINT;
      case YEAR:
        return SingleStoreType.SMALLINT;
      case SMALLINT:
        return isSigned() ? SingleStoreType.SMALLINT : SingleStoreType.UNSIGNED_SMALLINT;
      case INTEGER:
        return isSigned() ? SingleStoreType.INTEGER : SingleStoreType.UNSIGNED_INTEGER;
      case FLOAT:
        return SingleStoreType.FLOAT;
      case DOUBLE:
        return SingleStoreType.DOUBLE;
      case TIMESTAMP:
      case DATETIME:
        return SingleStoreType.TIMESTAMP;
      case BIGINT:
        return isSigned() ? SingleStoreType.BIGINT : SingleStoreType.UNSIGNED_BIGINT;
      case MEDIUMINT:
        return SingleStoreType.INTEGER;
      case DATE:
      case NEWDATE:
        return SingleStoreType.DATE;
      case TIME:
        return SingleStoreType.TIME;
      case JSON:
        return SingleStoreType.VARCHAR;
      case ENUM:
      case SET:
      case STRING:
      case VARSTRING:
      case NULL:
        return isBinary() ? SingleStoreType.BYTES : SingleStoreType.VARCHAR;
      case TEXT:
        return SingleStoreType.CLOB;
      case OLDDECIMAL:
      case DECIMAL:
        return SingleStoreType.DECIMAL;
      case BIT:
        return SingleStoreType.BIT;
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
      case BLOB:
      case GEOMETRY:
      case BSON:
        return SingleStoreType.BLOB;
      case INT8_VECTOR:
      case INT16_VECTOR:
      case INT32_VECTOR:
      case INT64_VECTOR:
      case FLOAT32_VECTOR:
      case FLOAT64_VECTOR:
        return SingleStoreType.VECTOR;
      default:
        return null;
    }
  }

  @Override
  public Integer getPrecision() {
    switch (dataType) {
      case OLDDECIMAL:
      case DECIMAL:
        // DECIMAL and OLDDECIMAL are  "exact" fixed-point number.
        // so :
        // - if can be signed, 1 byte is saved for sign
        // - if decimal > 0, one byte more for dot
        if (isSigned()) {
          return (int) (length - ((getDecimals() > 0) ? 2 : 1));
        } else {
          return (int) (length - ((decimals > 0) ? 1 : 0));
        }
      default:
        return (int) length;
    }
  }

  @Override
  public Integer getScale() {
    switch (dataType) {
      case OLDDECIMAL:
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case FLOAT:
      case DOUBLE:
      case BIGINT:
      case MEDIUMINT:
      case BIT:
      case DECIMAL:
        return (int) decimals;
      default:
        return 0;
    }
  }

  @Override
  public Class<?> getJavaType() {
    return getType().getJavaType();
  }

  public ColumnDefinitionPacket getNativeTypeMetadata() {
    return this;
  }

  @Override
  public boolean ending() {
    return this.ending;
  }
}
