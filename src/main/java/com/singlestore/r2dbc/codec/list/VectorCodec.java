// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2021-2024 SingleStore, Inc.

package com.singlestore.r2dbc.codec.list;


import com.singlestore.r2dbc.ExceptionFactory;
import com.singlestore.r2dbc.codec.Codec;
import com.singlestore.r2dbc.codec.DataType;
import com.singlestore.r2dbc.message.Context;
import com.singlestore.r2dbc.message.server.ColumnDefinitionPacket;
import com.singlestore.r2dbc.type.Vector;
import com.singlestore.r2dbc.util.BufferUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.nio.charset.StandardCharsets;
import java.util.EnumSet;

import static com.singlestore.r2dbc.util.BufferUtils.BINARY_PREFIX;
import static com.singlestore.r2dbc.util.BufferUtils.STRING_PREFIX;

public class VectorCodec implements Codec<Vector> {

    public static final VectorCodec INSTANCE = new VectorCodec();

    private static final EnumSet<DataType> COMPATIBLE_TYPES =
        EnumSet.of(
            DataType.FLOAT64_VECTOR,
            DataType.FLOAT32_VECTOR,
            DataType.INT64_VECTOR,
            DataType.INT32_VECTOR,
            DataType.INT16_VECTOR,
            DataType.INT8_VECTOR);

    @Override
    public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
        return COMPATIBLE_TYPES.contains(column.getDataType()) && type.isAssignableFrom(Vector.class);
    }

    @Override
    public boolean canEncode(Class<?> value) {
        return Vector.class.isAssignableFrom(value);
    }

    @Override
    public Vector decodeText(
        ByteBuf buf,
        int length,
        ColumnDefinitionPacket column,
        Class<? extends Vector> type,
        ExceptionFactory factory) {
        if (COMPATIBLE_TYPES.contains(column.getDataType()) && column.getExtTypeFormat() != null) {
            byte[] arr = new byte[length];
            buf.readBytes(arr);
            int dimensions = Integer.parseInt(column.getExtTypeFormat().split(",")[0]);
            return Vector.fromData(arr, dimensions, column.getDataType(), column.isBinary());
        }
        buf.readBytes(length);
        throw factory.createParsingException(String.format("Data type %s cannot be decoded as Vector", column.getDataType()));
    }

    @Override
    public Vector decodeBinary(
        ByteBuf buf,
        int length,
        ColumnDefinitionPacket column,
        Class<? extends Vector> type,
        ExceptionFactory factory) {
        return decodeText(buf, length, column, type, factory);
    }

    @Override
    public void encodeDirectText(ByteBuf out, Object value, Context context) {
        Vector vector = (Vector) value;
        byte[] b;
        if (vector.isBinary()) {
            b = vector.getValues();
            out.writeBytes(BINARY_PREFIX);
        } else {
            b = (vector.stringValue()).getBytes(StandardCharsets.UTF_8);
            out.writeBytes(STRING_PREFIX);
        }
        BufferUtils.escapedBytes(out, b, b.length, context);
        out.writeByte('\'');
    }

    @Override
    public void encodeDirectBinary(ByteBufAllocator allocator, ByteBuf out, Object value, Context context) {
        Vector vector = (Vector) value;
        byte[] b = vector.getValues();
        out.writeBytes(BufferUtils.encodeLength(b.length));
        out.writeBytes(b);
    }

    @Override
    public DataType getBinaryEncodeType() {
        return DataType.BLOB;
    }
}