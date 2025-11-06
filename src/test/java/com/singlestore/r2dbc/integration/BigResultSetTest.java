// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.integration;

import java.math.BigInteger;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;
import com.singlestore.r2dbc.BaseConnectionTest;
import com.singlestore.r2dbc.api.SingleStoreConnection;
import com.singlestore.r2dbc.api.SingleStoreConnectionMetadata;
import com.singlestore.r2dbc.api.SingleStoreResult;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class BigResultSetTest extends BaseConnectionTest {
  @BeforeAll
  public static void before2() {
    create_seq(sharedConn, "seq_1_to_10000", 1, 10000);
    create_seq(sharedConn, "seq_1_to_50000", 1, 50000);
    sharedConn
        .createStatement("CREATE TABLE multiPacketRow(val LONGTEXT, id int)")
        .execute()
        .blockLast();
  }

  @AfterAll
  public static void after2() {
    sharedConn.createStatement("DROP TABLE multiPacketRow").execute().blockLast();
    sharedConn.createStatement("DROP TABLE seq_1_to_10000").execute().blockLast();
    sharedConn.createStatement("DROP TABLE seq_1_to_50000").execute().blockLast();
  }

  @BeforeEach
  public void beforeEach() {
    sharedConn.createStatement("TRUNCATE TABLE multiPacketRow").execute().blockLast();
  }

  @Test
  void BigResultSet() {
    sharedConn
        .createStatement("SELECT * FROM seq_1_to_10000")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0)))
        .as(StepVerifier::create)
        .expectNextCount(10000)
        .verifyComplete();
  }

  @Test
  void multipleFluxSubscription() {
    SingleStoreConnectionMetadata meta = sharedConn.getMetadata();
    Flux<SingleStoreResult> res = sharedConn.createStatement("SELECT * FROM seq_1_to_50000").execute();

    Flux<String> flux1 =
        res.flatMap(r -> r.map((row, metadata) -> row.get(0, String.class))).share();

    AtomicInteger total = new AtomicInteger();

    for (int i = 0; i < 10; i++) {
      flux1.subscribe(s -> total.incrementAndGet());
    }

    flux1.blockLast();
    Assertions.assertTrue(total.get() >= 50000);
  }

  @Test
  void multiPacketRow() {
    Assumptions.assumeTrue(checkMaxAllowedPacketMore20m(sharedConn));
    multiPacketRow(sharedConn);
  }

  @Test
  void multiPacketRowPrepare() {
    Assumptions.assumeTrue(checkMaxAllowedPacketMore20m(sharedConn));
    multiPacketRow(sharedConnPrepare);
  }

  void multiPacketRow(SingleStoreConnection connection) {
    final char[] array19m = new char[19000000];
    for (int i = 0; i < array19m.length; i++) {
      array19m[i] = (char) (0x30 + (i % 10));
    }
    connection
        .createStatement("INSERT INTO multiPacketRow VALUES (?, ?)")
        .bind(0, new String(array19m))
        .bind(1, 2025)
        .execute()
        .blockLast();
    Assertions.assertArrayEquals(
        array19m,
        connection
            .createStatement("SELECT * FROM multiPacketRow")
            .execute()
            .flatMap(
                r ->
                    r.map(
                        (row, metadata) -> {
                          Assertions.assertEquals(2025, row.get(1));
                          String s = row.get(0, String.class);
                          Assertions.assertEquals(2025, row.get(1));
                          return s;
                        }))
            .blockLast()
            .toCharArray());
  }

  public boolean checkMaxAllowedPacketMore20m(SingleStoreConnection connection) {
    BigInteger maxAllowedPacket =
        connection
            .createStatement("select @@max_allowed_packet")
            .execute()
            .flatMap(r -> r.map((row, metadata) -> row.get(0, BigInteger.class)))
            .blockLast();
    return maxAllowedPacket.compareTo(BigInteger.valueOf(20 * 1024 * 1024L)) > 0;
  }
}
