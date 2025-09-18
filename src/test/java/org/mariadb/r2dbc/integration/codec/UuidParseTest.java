// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration.codec;

import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.*;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.api.MariadbConnection;
import reactor.test.StepVerifier;

public class UuidParseTest extends BaseConnectionTest {
  @BeforeAll
  public static void before2() {
    afterAll2();
    sharedConn.beginTransaction().block();
    sharedConn.createStatement("DROP TABLE IF EXISTS UuidTable").execute().blockLast();
    sharedConn.createStatement("CREATE TABLE UuidTable (t1 TEXT, t2 INT)").execute().blockLast();
    sharedConn
          .createStatement(
              "INSERT INTO UuidTable VALUES"
                  + " ('123e4567-e89b-12d3-a456-426655440000', 1),('ffffffff-ffff-ffff-ffff-fffffffffffe', 2),"
                  + " (null, 3)")
          .execute()
          .blockLast();
    sharedConn.createStatement("FLUSH TABLES").execute().blockLast();
    sharedConn.commitTransaction().block();
  }

  @AfterAll
  public static void afterAll2() {
    sharedConn.createStatement("DROP TABLE IF EXISTS UuidTable").execute().blockLast();
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
        .createStatement("SELECT t1 FROM UuidTable WHERE 1 = ? ORDER BY t2")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of("123e4567-e89b-12d3-a456-426655440000"),
            Optional.of("ffffffff-ffff-ffff-ffff-fffffffffffe"),
            Optional.empty())
        .verifyComplete();

    connection
        .createStatement("SELECT t1 FROM UuidTable WHERE 1 = ? ORDER BY t2")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Object.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of("123e4567-e89b-12d3-a456-426655440000"),
            Optional.of("ffffffff-ffff-ffff-ffff-fffffffffffe"),
            Optional.empty())
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
        .createStatement("SELECT t1 FROM UuidTable WHERE 1 = ? ORDER BY t2")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, String.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of("123e4567-e89b-12d3-a456-426655440000"),
            Optional.of("ffffffff-ffff-ffff-ffff-fffffffffffe"),
            Optional.empty())
        .verifyComplete();
  }

  @Test
  void uuidValue() {
    uuidValue(sharedConn);
  }

  @Test
  void uuidValuePrepare() {
    uuidValue(sharedConnPrepare);
  }

  private void uuidValue(MariadbConnection connection) {
    connection
        .createStatement("SELECT t1 FROM UuidTable WHERE 1 = ? ORDER BY t2")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, UUID.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(UUID.fromString("123e4567-e89b-12d3-a456-426655440000")),
            Optional.of(UUID.fromString("ffffffff-ffff-ffff-ffff-fffffffffffe")),
            Optional.empty())
        .verifyComplete();
  }
}
