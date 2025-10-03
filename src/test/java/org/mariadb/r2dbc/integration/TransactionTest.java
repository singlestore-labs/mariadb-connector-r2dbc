// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration;

import org.junit.jupiter.api.*;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.api.MariadbConnection;
import reactor.test.StepVerifier;

public class TransactionTest extends BaseConnectionTest {
  private static final String insertCmd =
      "INSERT INTO `users` (`first_name`, `last_name`, `email`) VALUES ('MariaDB', 'Row',"
          + " 'mariadb@test.com')";

  @BeforeAll
  public static void before2() {
    drop();
    sharedConn
        .createStatement(
            "CREATE TABLE `users` (\n"
                + " `id` int(11) NOT NULL AUTO_INCREMENT,\n"
                + " `first_name` varchar(255) NOT NULL,\n"
                + " `last_name` varchar(255) NOT NULL,\n"
                + " `email` varchar(255) NOT NULL,\n"
                + " PRIMARY KEY (`id`)\n"
                + ")")
        .execute()
        .blockLast();
  }

  @AfterAll
  public static void drop() {
    sharedConn.createStatement("DROP TABLE IF EXISTS `users`").execute().blockLast();
  }

  @BeforeEach
  public void beforeEach() {
    sharedConn.createStatement("TRUNCATE TABLE `users`").execute().blockLast();
  }

  @Test
  void commit() {
    MariadbConnection conn = factory.create().block();
    try {
      conn.beginTransaction().subscribe();
      conn.createStatement(insertCmd).execute().subscribe();
      conn.commitTransaction().block();
      checkInserted(conn, 1);
    } finally {
      conn.close().block();
    }
  }

  @Test
  void multipleBegin() {
    MariadbConnection conn = factory.create().block();
    try {
      // must issue only one begin command
      conn.beginTransaction().subscribe();
      conn.beginTransaction().subscribe();
      conn.beginTransaction().block();
    } finally {
      conn.close().block();
    }
  }

  @Test
  void commitWithoutTransaction() {
    // must issue no commit command
    MariadbConnection conn = factory.create().block();
    try {
      conn.commitTransaction().subscribe();
      conn.commitTransaction().subscribe();
      conn.commitTransaction().block();
    } finally {
      conn.close().block();
    }
  }

  @Test
  void rollbackWithoutTransaction() {
    // must issue no commit command
    MariadbConnection conn = factory.create().block();
    try {
      conn.rollbackTransaction().subscribe();
      conn.rollbackTransaction().subscribe();
      conn.rollbackTransaction().block();
    } finally {
      conn.close().block();
    }
  }

  @Test
  void rollback() {
    MariadbConnection conn = factory.create().block();
    try {
      conn.beginTransaction().block();
      conn.createStatement(insertCmd).execute().subscribe();
      conn.rollbackTransaction().block();
      checkInserted(conn, 0);
    } finally {
      conn.close().block();
    }
  }

  @Test
  void rollbackPipelining() {
    MariadbConnection conn = factory.create().block();
    try {
      conn.beginTransaction().subscribe();
      conn.createStatement(insertCmd).execute().subscribe();
      conn.rollbackTransaction().subscribe();
      conn.rollbackTransaction().block();
      checkInserted(conn, 0);
    } finally {
      conn.close().block();
    }
  }

  @Test
  void savepoints() {
    MariadbConnection conn = factory.create().block();
    try {
      assertThrowsContains(
        UnsupportedOperationException.class,
        () -> conn.createSavepoint("mySavePoint1").block(),
        "Savepoints are not supported in SingleStore");
      assertThrowsContains(
        UnsupportedOperationException.class,
        () -> conn.rollbackTransactionToSavepoint("mySavePoint1").block(),
        "Savepoints are not supported in SingleStore");
      assertThrowsContains(
        UnsupportedOperationException.class,
        () -> conn.releaseSavepoint("mySavePoint1").block(),
        "Savepoints are not supported in SingleStore");
    } finally {
      conn.close().block();
    }
  }

  private void checkInserted(MariadbConnection conn, int expectedValue) {
    conn.createStatement("SELECT count(*) FROM `users`")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, Integer.class)))
        .as(StepVerifier::create)
        .expectNext(Integer.valueOf(expectedValue))
        .verifyComplete();
  }
}
