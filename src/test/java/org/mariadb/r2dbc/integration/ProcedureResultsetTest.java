// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration;

import org.junit.jupiter.api.*;
import org.mariadb.r2dbc.BaseConnectionTest;
import reactor.test.StepVerifier;

public class ProcedureResultsetTest extends BaseConnectionTest {

  @BeforeAll
  public static void before2() {
    dropAll();
    sharedConn
        .createStatement(
            "CREATE PROCEDURE basic_proc (IN t1 INT, INOUT t2 INT unsigned, OUT t3 INT, IN t4 INT,"
                + " OUT t5 VARCHAR(20) CHARSET utf8mb4, OUT t6 TIMESTAMP, OUT t7 VARCHAR(20)"
                + " CHARSET utf8mb4) BEGIN \n"
                + "SELECT 1;\n"
                + "set t3 = t1 * t4;\n"
                + "set t2 = t2 * t1;\n"
                + "set t5 = 'http://test';\n"
                + "set t6 = TIMESTAMP('2003-12-31 12:00:00');\n"
                + "set t7 = 'test';\n"
                + "END")
        .execute()
        .blockLast();
    sharedConn
        .createStatement(
            "CREATE PROCEDURE no_out_proc (t1 INT, t2 INT) AS DECLARE result INT;"
                + "BEGIN \n"
                + "result = t1 + t2;\n"
                + "END")
        .execute()
        .blockLast();
  }

  @AfterAll
  public static void dropAll() {
    sharedConn.createStatement("DROP PROCEDURE IF EXISTS basic_proc").execute().blockLast();
    sharedConn.createStatement("DROP PROCEDURE IF EXISTS no_out_proc").execute().blockLast();
  }

  @Test
  void inParameter() {
    Assumptions.assumeFalse(isXpand());
    sharedConn
        .createStatement("call no_out_proc(?,?)")
        .bind(0, 2)
        .bind(1, 10)
        .execute()
        .flatMap(it -> it.getRowsUpdated())
        .as(StepVerifier::create)
        .expectNext(0L)
        .verifyComplete();
    sharedConn
        .createStatement("/*text*/ call no_out_proc(?,?)")
        .bind(0, 2)
        .bind(1, 10)
        .execute()
        .flatMap(it -> it.getRowsUpdated())
        .as(StepVerifier::create)
        .expectNext(0L)
        .verifyComplete();
  }
}
