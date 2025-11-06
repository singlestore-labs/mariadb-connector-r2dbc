// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc;

import com.singlestore.r2dbc.api.SingleStoreStatement;
import org.openjdk.jmh.annotations.Benchmark;

public class Select_1 extends Common {

    @Benchmark
    public Integer testR2dbc(MyState state) throws Throwable {
        return consume(state.r2dbc);
    }

    @Benchmark
    public Integer testR2dbcPrepare(MyState state) throws Throwable {
        return consume(state.r2dbcPrepare);
    }

    private Integer consume(SingleStoreConnection connection) {
        int rnd = (int) (Math.random() * 1000);
        SingleStoreStatement statement = connection.createStatement("select " + rnd);
        return
                statement.execute()
                        .flatMap(it -> it.map((row, rowMetadata) -> row.get(0, Integer.class)))
                        .blockLast();
    }

}
