// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.client.util;

import java.util.Arrays;

public enum ProtocolExtendedTypeCodes {
    NONE(0),
    BSON(1),
    VECTOR(2);
    private final int code;

    ProtocolExtendedTypeCodes(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static ProtocolExtendedTypeCodes fromCode(int code) {
        return Arrays.stream(values())
            .filter(v -> v.getCode() == code)
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Wrong extended data type: " + code));
    }
}
