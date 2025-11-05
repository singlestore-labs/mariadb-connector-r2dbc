// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.integration;

import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import com.singlestore.r2dbc.BaseConnectionTest;
import com.singlestore.r2dbc.MariadbConnectionConfiguration;
import com.singlestore.r2dbc.MariadbConnectionFactory;
import com.singlestore.r2dbc.TestConfiguration;
import com.singlestore.r2dbc.api.MariadbConnection;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class NoPipelineTest extends BaseConnectionTest {

  @BeforeAll
  public static void before2() {
    int MAX = 100;
    for (int i = 0; i < MAX; i++) {
      create_seq(sharedConn, String.format("seq_%d_to_%d", (100 * i), (100 * (i + 1) - 1)), (100 * i), (100 * (i + 1) - 1));
    }
  }

  @AfterAll
  public static void after2() {
    int MAX = 100;
    for (int i = 0; i < MAX; i++) {
      sharedConn.createStatement(String.format("DROP TABLE seq_%d_to_%d", (100 * i), (100 * (i + 1) - 1))).execute().blockLast();
    }
  }

  @Test
  void noPipelineConnect() throws Exception {
    MariadbConnectionConfiguration confPipeline =
        TestConfiguration.defaultBuilder.clone().build();
    MariadbConnection connection = new MariadbConnectionFactory(confPipeline).create().block();

    try {
      runWithPipeline(connection);
    } finally {
      connection.close().block();
    }
  }

  private Duration runWithPipeline(MariadbConnection connection) {
    Instant initial = Instant.now();
    int MAX = 100;
    List<Flux<BigInteger>> fluxes = new ArrayList<>();
    for (int i = 0; i < MAX; i++) {
      fluxes.add(
          connection
              .createStatement("SELECT * from seq_" + (100 * i) + "_to_" + (100 * (i + 1) - 1))
              .execute()
              .flatMap(r -> r.map((row, metadata) -> row.get(0, BigInteger.class))));
    }

    for (int i = 0; i < MAX - 1; i++) {
      fluxes.get(i).subscribe();
    }
    Flux.concat(fluxes.get(MAX - 1)).as(StepVerifier::create).expectNextCount(100).verifyComplete();
    return Duration.between(initial, Instant.now());
  }
}
