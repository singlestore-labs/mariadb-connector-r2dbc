// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc;

import com.singlestore.r2dbc.api.SingleStoreStatement;
import org.openjdk.jmh.annotations.Benchmark;

public class Select_100_cols extends Common {

    @Benchmark
    public int[] testR2dbc(MyState state) throws Throwable {
        return consume(state.r2dbc);
    }

    @Benchmark
    public int[] testR2dbcPrepare(MyState state) throws Throwable {
        return consumePrepare(state.r2dbcPrepare);
    }

    private int[] consume(SingleStoreConnection connection) {

        SingleStoreStatement statement =
                connection.createStatement("select * FROM test100");
        return
                statement.execute()
                        .flatMap(
                                it ->
                                        it.map(
                                                (row, rowMetadata) -> {
                                                    int[] objs = new int[100];
                                                    for (int i = 0; i < 100; i++) {
                                                        objs[i] = row.get(i, Integer.class);
                                                    }
                                                    return objs;
                                                }))
                        .blockLast();
    }

    private int[] consumePrepare(SingleStoreConnection connection) {

        SingleStoreStatement statement =
                connection.createStatement("select * FROM test100 WHERE 1 = ?").bind(0, 1);
        return
                statement.execute()
                        .flatMap(
                                it ->
                                        it.map(
                                                (row, rowMetadata) -> {
                                                    int[] objs = new int[100];
                                                    for (int i = 0; i < 100; i++) {
                                                        objs[i] = row.get(i, Integer.class);
                                                    }
                                                    return objs;
                                                }))
                        .blockLast();
    }
}
