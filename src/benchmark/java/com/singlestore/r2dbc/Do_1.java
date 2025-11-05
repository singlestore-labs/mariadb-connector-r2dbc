// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc;

import org.openjdk.jmh.annotations.Benchmark;

public class Do_1 extends Common {

    @Benchmark
    public Long testR2dbc(MyState state) throws Throwable {
        return consume(state.r2dbc);
    }

    @Benchmark
    public Long testR2dbcPrepare(MyState state) throws Throwable {
        return consume(state.r2dbcPrepare);
    }

    private Long consume(SingleStoreConnection connection) {
        return connection.createStatement("DO 1").execute()
                .flatMap(it -> it.getRowsUpdated())
                .blockLast();
    }


}
