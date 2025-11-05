// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.integration;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import com.singlestore.r2dbc.BaseConnectionTest;
import com.singlestore.r2dbc.SingleStoreConnectionConfiguration;
import com.singlestore.r2dbc.SingleStoreConnectionFactory;
import com.singlestore.r2dbc.TestConfiguration;
import com.singlestore.r2dbc.api.SingleStoreBatch;
import com.singlestore.r2dbc.api.SingleStoreConnection;
import com.singlestore.r2dbc.api.SingleStoreResult;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class BatchTest extends BaseConnectionTest {

  @Test
  void basicBatch() {
    sharedConn
        .createStatement("CREATE TEMPORARY TABLE basicBatch (id int, test varchar(10))")
        .execute()
        .blockLast();
    sharedConn.beginTransaction().block(); // if MAXSCALE ensure using WRITER
    SingleStoreBatch batch = sharedConn.createBatch();
    int[] res = new int[20];
    for (int i = 0; i < 20; i++) {
      batch.add("INSERT INTO basicBatch VALUES (" + i + ", 'test" + i + "')");
      res[i] = i;
    }

    batch
        .execute()
        .flatMap(it -> it.getRowsUpdated())
        .as(StepVerifier::create)
        .expectNext(1L, 1L, 1L, 1L, 1L)
        .expectNextCount(15)
        .verifyComplete();
    sharedConn
        .createStatement("SELECT id FROM basicBatch ORDER BY id")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0)))
        .as(StepVerifier::create)
        .expectNext(0, 1, 2, 3, 4)
        .expectNextCount(15)
        .then(() -> sharedConn.commitTransaction())
        .verifyComplete();
  }

  @Test
  void multiQueriesBatch() throws Exception {
    // error crashing maxscale 6.1.x
    Assumptions.assumeTrue(
        !sharedConn.getMetadata().getDatabaseVersion().contains("maxScale-6.1.")
            && !isMaxscale()
            && !"skysql-ha".equals(System.getenv("srv")));
    SingleStoreConnectionConfiguration confMulti =
        TestConfiguration.defaultBuilder.clone().allowMultiQueries(true).build();
    batchTest(confMulti);
    SingleStoreConnectionConfiguration confNoMulti =
        TestConfiguration.defaultBuilder.clone().allowMultiQueries(false).build();
    batchTest(confNoMulti);
  }

  private void batchTest(SingleStoreConnectionConfiguration conf) throws Exception {
    sharedConn.createStatement("DROP TABLE IF EXISTS multiBatch").execute().blockLast();
    sharedConn
        .createStatement("CREATE TABLE multiBatch (id int, test varchar(10))")
        .execute()
        .blockLast();
    SingleStoreConnection multiConn = new SingleStoreConnectionFactory(conf).create().block();
    try {
      multiConn.beginTransaction().block(); // if MAXSCALE ensure using WRITER
      SingleStoreBatch batch = multiConn.createBatch();
      int[] res = new int[20];
      for (int i = 0; i < 20; i++) {
        batch.add("INSERT INTO multiBatch VALUES (" + i + ", 'test" + i + "')");
        res[i] = i;
      }
      batch
          .execute()
          .flatMap(it -> it.getRowsUpdated())
          .as(StepVerifier::create)
          .expectNext(1L, 1L, 1L, 1L, 1L)
          .expectNextCount(15)
          .verifyComplete();
      multiConn
          .createStatement("SELECT id FROM multiBatch ORDER BY id")
          .execute()
          .flatMap(r -> r.map((row, metadata) -> row.get(0)))
          .as(StepVerifier::create)
          .expectNext(0, 1, 2, 3, 4)
          .expectNextCount(15)
          .verifyComplete();
      multiConn.commitTransaction().block();
    } finally {
      multiConn.close().block();
    }
  }

  @Test
  void cancelBatch() throws Exception {
    // error crashing maxscale 6.1.x
    Assumptions.assumeTrue(
        !sharedConn.getMetadata().getDatabaseVersion().contains("maxScale-6.1.")
            && !"skysql-ha".equals(System.getenv("srv")));
    SingleStoreConnectionConfiguration confNoMulti =
        TestConfiguration.defaultBuilder.clone().allowMultiQueries(false).build();
    SingleStoreConnection multiConn = new SingleStoreConnectionFactory(confNoMulti).create().block();
    try {
      multiConn
          .createStatement("CREATE TEMPORARY TABLE multiBatch (id int, test varchar(10))")
          .execute()
          .blockLast();
      SingleStoreBatch batch = multiConn.createBatch();

      int[] res = new int[10_000];
      for (int i = 0; i < res.length; i++) {
        batch.add("INSERT INTO multiBatch VALUES (" + i + ", 'test" + i + "')");
        res[i] = i;
      }
      AtomicInteger resultNb = new AtomicInteger(0);
      Flux<SingleStoreResult> f = batch.execute();
      Disposable disp =
          f.flatMap(it -> it.getRowsUpdated()).subscribe(i -> resultNb.incrementAndGet());
      for (int i = 0; i < 100; i++) {
        Thread.sleep(50);
        if (resultNb.get() > 0) break;
      }
      disp.dispose();
      Thread.sleep(1000);
      Assertions.assertTrue(
          resultNb.get() > 0 && resultNb.get() < 10_000,
          String.format("expected %s to be 0 < x < 10000", resultNb.get()));
    } finally {
      multiConn.close().block();
    }
  }

  @Test
  void batchWithParameter() {
    SingleStoreBatch batch = sharedConn.createBatch();
    batch.add("INSERT INTO JJ VALUES ('g?')");
    batch.add("INSERT INTO JJ VALUES ('g') /* ?*/");
    batch.add("INSERT INTO JJ VALUES ('g') /* :named_param*/");
    try {
      batch.add("INSERT INTO JJ VALUES (?)");
      Assertions.fail("must have thrown exception");
    } catch (IllegalArgumentException e) {
      Assertions.assertTrue(
          e.getMessage().contains("Statement with parameters cannot be batched (sql:'"));
    }
    try {
      batch.add("INSERT INTO JJ VALUES (:named_param)");
      Assertions.fail("must have thrown exception");
    } catch (IllegalArgumentException e) {
      Assertions.assertTrue(
          e.getMessage().contains("Statement with parameters cannot be batched (sql:'"));
    }
  }

  @Test
  void batchError() {
    batchError(sharedConn);
    batchError(sharedConnPrepare);
  }

  void batchError(SingleStoreConnection conn) {
    conn.createStatement("CREATE TEMPORARY TABLE basicBatch2 (id int, test varchar(10))")
        .execute()
        .blockLast();
    conn.createStatement("INSERT INTO basicBatch2 VALUES (?, ?)")
        .bind(0, 1)
        .bind(1, "dd")
        .execute()
        .blockLast();
    assertThrows(
        IllegalStateException.class,
        () ->
            conn.createStatement("INSERT INTO basicBatch2 VALUES (?, ?)")
                .bind(0, 1)
                .bind(1, "dd")
                .add()
                .bind(1, "dd")
                .add()
                .execute(),
        "Parameter at position 0 is not set");
  }
}
