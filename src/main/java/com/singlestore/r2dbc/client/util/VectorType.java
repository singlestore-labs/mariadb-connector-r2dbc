// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.client.util;

import com.singlestore.r2dbc.codec.DataType;

import java.util.Arrays;

public enum VectorType {
    NONE(0, DataType.NULL),
    F32(1, DataType.FLOAT32_VECTOR),
    F64(2, DataType.FLOAT64_VECTOR),
    I8(3, DataType.INT8_VECTOR),
    I16(4, DataType.INT16_VECTOR),
    I32(5, DataType.INT32_VECTOR),
    I64(6, DataType.INT64_VECTOR);

    private final int code;
    private final DataType type;

    VectorType(int code, DataType type) {
        this.code = code;
        this.type = type;
    }

    public int getCode() {
        return code;
    }

    public DataType getType() {
        return type;
    }

    public static VectorType fromCode(int code) {
        return Arrays.stream(values())
            .filter(v -> v.getCode() == code)
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Wrong extended vector type: " + code));
    }
}