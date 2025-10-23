// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration;

import io.r2dbc.spi.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.MariadbConnectionFactory;
import org.mariadb.r2dbc.TestConfiguration;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.api.MariadbConnectionMetadata;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class ErrorTest extends BaseConnectionTest {

  @BeforeAll
  public static void before2() {
    create_seq(sharedConn, "seq_1_to_100000", 1, 100000);
    sharedConn.createStatement("CREATE RESOURCE POOL temp_pool WITH QUERY_TIMEOUT = 1").execute().blockLast();
  }

  @AfterAll
  public static void after2() {
    sharedConn.createStatement("DROP RESOURCE POOL temp_pool").execute().blockLast();
    sharedConn.createStatement("DROP TABLE IF EXISTS seq_1_to_100000").execute().blockLast();
    sharedConn.createStatement("DROP TABLE IF EXISTS deadlock").execute().blockLast();
  }

  @Test
  void queryTimeout() throws Exception {
    sharedConn.createStatement("SET resource_pool = temp_pool").execute().blockLast();
    try {
      sharedConn
          .createStatement("SELECT SLEEP(10)")
          .execute()
          .flatMap(r -> r.getRowsUpdated())
          .as(StepVerifier::create)
          .expectErrorMatches(
              throwable ->
                  throwable instanceof io.r2dbc.spi.R2dbcTransientResourceException
                      && throwable.getMessage().contains("The query has reached the timeout"))
          .verify();
    } finally {
      sharedConn.createStatement("SET resource_pool = default_pool").execute().blockLast();
    }
  }

  @Test
  void permissionDenied() throws Exception {
    sharedConn.createStatement("CREATE USER IF NOT EXISTS userWithoutRight"+getHostSuffix()).execute().blockLast();
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .allowPublicKeyRetrieval(true)
            .username("userWithoutRight")
            .password("")
            .build();
    new MariadbConnectionFactory(conf)
        .create()
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && (throwable.getMessage().contains("Access denied for user 'userWithoutRight'")
                        || throwable.getMessage().contains("Insufficient user permissions")))
        .verify();

    conf =
        TestConfiguration.defaultBuilder
            .clone()
            .allowPublicKeyRetrieval(true)
            .username("userWithoutRight")
            .password("wrongpassword")
            .build();
    new MariadbConnectionFactory(conf)
        .create()
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && throwable.getMessage().contains("Fail to establish connection to")
                    && (throwable
                            .getCause()
                            .getMessage()
                            .contains("Access denied for user 'userWithoutRight'")
                        || throwable.getMessage().contains("Access denied")))
        .verify();
  }

  @Test
  void dataIntegrity() throws Exception {
    sharedConn
        .createStatement("CREATE TEMPORARY TABLE dataIntegrity(t1 VARCHAR(5))")
        .execute()
        .blockLast();
    sharedConn
        .createStatement("INSERT INTO dataIntegrity VALUE ('DATATOOOBIG')")
        .execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcBadGrammarException
                    && throwable.getMessage().contains("Data too long"))
        .verify();
  }

  @Test
  void closeDuringSelect() {
    MariadbConnection connection2 = factory.create().block();
    connection2
        .createStatement("SELECT * FROM seq_1_to_100000")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0)))
        .as(StepVerifier::create)
        .expectNextCount(100000)
        .verifyComplete();
    connection2.close().block();
  }
}
