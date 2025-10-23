// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.codec.list;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;

public class ByteCodec implements Codec<Byte> {

  public static final ByteCodec INSTANCE = new ByteCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.TINYINT,
          DataType.SMALLINT,
          DataType.MEDIUMINT,
          DataType.INTEGER,
          DataType.BIGINT,
          DataType.YEAR,
          DataType.BIT,
          DataType.FLOAT,
          DataType.DOUBLE,
          DataType.OLDDECIMAL,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB,
          DataType.DECIMAL,
          DataType.ENUM,
          DataType.VARSTRING,
          DataType.STRING,
          DataType.TEXT);

  public static long parseBit(ByteBuf buf, int length) {
    if (length == 1) {
      return buf.readUnsignedByte();
    }
    long val = 0;
    int idx = 0;
    do {
      val += ((long) buf.readUnsignedByte()) << (8 * length);
      idx++;
    } while (idx < length);
    return val;
  }

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getDataType())
        && ((type.isPrimitive() && type == Byte.TYPE) || type.isAssignableFrom(Byte.class));
  }

  public boolean canEncode(Class<?> value) {
    return Byte.class.isAssignableFrom(value);
  }

  @Override
  public Byte decodeText(
      ByteBuf buf,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends Byte> type,
      ExceptionFactory factory) {

    long result;
    switch (column.getDataType()) {
      case TINYINT:
      case SMALLINT:
      case MEDIUMINT:
      case INTEGER:
      case BIGINT:
      case YEAR:
        result = LongCodec.parse(buf, length);
        break;

      case BIT:
        int index = 0;
        byte val = 0;
        while (index++ < length && val == 0) {
          val = buf.readByte();
        }
        buf.skipBytes(length - index + 1);
        return val;

      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        if (length == 0) return 0;
        byte b = buf.readByte();
        buf.skipBytes(length - 1);
        return b;

      default:
        // FLOAT, DOUBLE, OLDDECIMAL, DECIMAL, ENUM, VARCHAR, VARSTRING, STRING:
        String str = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        try {
          result = new BigDecimal(str).setScale(0, RoundingMode.DOWN).byteValueExact();
        } catch (NumberFormatException | ArithmeticException nfe) {
          throw factory.createParsingException(
              String.format(
                  "value '%s' (%s) cannot be decoded as Byte", str, column.getDataType()));
        }
        break;
    }

    if ((byte) result != result || (result < 0 && !column.isSigned())) {
      throw factory.createParsingException("byte overflow");
    }

    return (byte) result;
  }

  @Override
  public Byte decodeBinary(
      ByteBuf buf,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends Byte> type,
      ExceptionFactory factory) {

    long result;
    switch (column.getDataType()) {
      case TINYINT:
        result = column.isSigned() ? buf.readByte() : buf.readUnsignedByte();
        break;

      case YEAR:
      case SMALLINT:
        result = column.isSigned() ? buf.readShortLE() : buf.readUnsignedShortLE();
        break;

      case MEDIUMINT:
        result = column.isSigned() ? buf.readMediumLE() : buf.readUnsignedMediumLE();
        buf.readByte(); // needed since binary protocol exchange for medium are on 4 bytes
        break;

      case INTEGER:
        result = column.isSigned() ? buf.readIntLE() : buf.readUnsignedIntLE();
        break;

      case BIGINT:
        if (column.isSigned()) {
          result = buf.readLongLE();
        } else {
          // need BIG ENDIAN, so reverse order
          byte[] bb = new byte[8];
          for (int i = 7; i >= 0; i--) {
            bb[i] = buf.readByte();
          }
          BigInteger val = new BigInteger(1, bb);
          result = val.longValue();
        }
        break;

      case BIT:
        int index = 0;
        byte val = 0;
        while (index++ < length && val == 0) {
          val = buf.readByte();
        }
        buf.skipBytes(length - index + 1);
        return val;

      case FLOAT:
        float f = buf.readFloatLE();
        result = (long) f;
        if ((byte) result != result || (result < 0 && !column.isSigned())) {
          throw factory.createParsingException(
              String.format("value '%s' (%s) cannot be decoded as Byte", f, column.getDataType()));
        }
        break;

      case DOUBLE:
        double d = buf.readDoubleLE();
        result = (long) d;
        if ((byte) result != result || (result < 0 && !column.isSigned())) {
          throw factory.createParsingException(
              String.format("value '%s' (%s) cannot be decoded as Byte", d, column.getDataType()));
        }
        break;

      case OLDDECIMAL:
      case DECIMAL:
      case ENUM:
      case TEXT:
      case VARSTRING:
      case STRING:
        String str = buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
        try {
          result = new BigDecimal(str).setScale(0, RoundingMode.DOWN).byteValueExact();
        } catch (NumberFormatException | ArithmeticException nfe) {
          throw factory.createParsingException(
              String.format(
                  "value '%s' (%s) cannot be decoded as Byte", str, column.getDataType()));
        }
        break;

      default:
        // BLOB, TINYBLOB, MEDIUMBLOB, LONGBLOB:
        if (length == 0) return 0;
        byte b = buf.readByte();
        buf.skipBytes(length - 1);
        return b;
    }

    if ((byte) result != result || (result < 0 && !column.isSigned())) {
      throw factory.createParsingException("byte overflow");
    }

    return (byte) result;
  }

  @Override
  public void encodeDirectText(ByteBuf out, Object value, Context context) {
    out.writeCharSequence(Integer.toString((Byte) value), StandardCharsets.US_ASCII);
  }

  @Override
  public void encodeDirectBinary(
      ByteBufAllocator allocator, ByteBuf out, Object value, Context context) {
    out.writeByte((Byte) value);
  }

  public DataType getBinaryEncodeType() {
    return DataType.TINYINT;
  }
}
