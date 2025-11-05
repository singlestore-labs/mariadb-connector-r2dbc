// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.integration.codec;

import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import com.singlestore.r2dbc.BaseConnectionTest;
import com.singlestore.r2dbc.api.MariadbConnection;
import reactor.test.StepVerifier;

public class JsonParseTest extends BaseConnectionTest {
  @BeforeAll
  public static void before2() {
    afterAll2();
    sharedConn.beginTransaction().block();
    sharedConn.createStatement("DROP TABLE IF EXISTS JsonTable").execute().blockLast();
    sharedConn.createStatement("CREATE TABLE JsonTable (t1 JSON, t2 INT)").execute().blockLast();
    sharedConn
        .createStatement(
            "INSERT INTO JsonTable VALUES" + " ('{}', 1),('{\"val\": \"val1\"}', 2)," + " (null, 3)")
        .execute()
        .blockLast();
    sharedConn.createStatement("FLUSH TABLES").execute().blockLast();
    sharedConn.commitTransaction().block();
  }

  @AfterAll
  public static void afterAll2() {
    // sharedConn.createStatement("DROP TABLE IF EXISTS JsonTable").execute().blockLast();
  }

  @Test
  void defaultValue() {
    defaultValue(sharedConn);
  }

  @Test
  void defaultValuePrepare() {
    defaultValue(sharedConnPrepare);
  }

  private void defaultValue(MariadbConnection connection) {
    connection
        .createStatement("SELECT t1 FROM JsonTable WHERE 1 = ? ORDER BY t2")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(Optional.of("{}"), Optional.of("{\"val\":\"val1\"}"), Optional.empty())
        .verifyComplete();

    connection
        .createStatement("SELECT t1 FROM JsonTable WHERE 1 = ? ORDER BY t2")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Object.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of("{}"), Optional.of("{\"val\":\"val1\"}"), Optional.empty())
        .verifyComplete();
  }

  @Test
  void stringValue() {
    stringValue(sharedConn);
  }

  @Test
  void stringValuePrepare() {
    stringValue(sharedConnPrepare);
  }

  private void stringValue(MariadbConnection connection) {
    connection
        .createStatement("SELECT t1 FROM JsonTable WHERE 1 = ? ORDER BY t2")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, String.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of("{}"), Optional.of("{\"val\":\"val1\"}"), Optional.empty())
        .verifyComplete();
  }
}
