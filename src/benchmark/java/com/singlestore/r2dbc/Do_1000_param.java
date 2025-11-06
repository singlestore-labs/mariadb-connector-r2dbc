// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc;

import com.singlestore.r2dbc.api.SingleStoreStatement;
import org.openjdk.jmh.annotations.Benchmark;

public class Do_1000_param extends Common {
    private static final String sql;

    static {
        StringBuilder sb = new StringBuilder("do ?");
        for (int i = 1; i < 1000; i++) {
            sb.append(",?");
        }
        sql = sb.toString();
    }

    @Benchmark
    public Long testR2dbc(MyState state) throws Throwable {
        return consume(state.r2dbc);
    }

    @Benchmark
    public Long testR2dbcPrepare(MyState state) throws Throwable {
        return consume(state.r2dbcPrepare);
    }

    private Long consume(SingleStoreConnection connection) {
        SingleStoreStatement statement = connection.createStatement(sql);
        for (int i = 0; i < 1000; i++)
            statement.bind(i, i);
        return statement.execute()
                .flatMap(it -> it.getRowsUpdated())
                .blockLast();
    }


}
