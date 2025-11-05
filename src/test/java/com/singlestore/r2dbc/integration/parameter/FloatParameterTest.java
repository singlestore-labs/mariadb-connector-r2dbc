// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.integration.parameter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;
import org.junit.jupiter.api.*;
import com.singlestore.r2dbc.BaseConnectionTest;
import com.singlestore.r2dbc.api.MariadbConnection;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class FloatParameterTest extends BaseConnectionTest {
  @BeforeAll
  public static void before2() {
    sharedConn
        .createStatement("CREATE TABLE FloatParam (t1 FLOAT, t2 FLOAT, t3 FLOAT)")
        .execute()
        .blockLast();
  }

  @AfterAll
  public static void after2() {
    sharedConn.createStatement("DROP TABLE FloatParam").execute().blockLast();
  }

  @BeforeEach
  public void beforeEach() {
    sharedConn.createStatement("TRUNCATE TABLE FloatParam").execute().blockLast();
  }

  @Test
  void nullValue() {
    nullValue(sharedConn);
  }

  @Test
  void nullValuePrepare() {
    nullValue(sharedConnPrepare);
  }

  private void nullValue(MariadbConnection connection) {
    connection
        .createStatement("INSERT INTO FloatParam VALUES (?,?,?)")
        .bindNull(0, Float.class)
        .bindNull(1, Float.class)
        .bindNull(2, Float.class)
        .execute()
        .blockLast();
    validate(Optional.empty(), 0f, Optional.empty(), 0f, Optional.empty(), 0f);
  }

  @Test
  void bigIntValue() {
    bigIntValue(sharedConn);
  }

  @Test
  void bigIntValuePrepare() {
    bigIntValue(sharedConnPrepare);
  }

  private void bigIntValue(MariadbConnection connection) {
    connection
        .createStatement("INSERT INTO FloatParam VALUES (?,?,?)")
        .bind(0, BigInteger.ONE)
        .bind(1, new BigInteger("9223372036854775807"))
        .bind(2, new BigInteger("-9"))
        .execute()
        .blockLast();
    validate(
        Optional.of(1f),
        0f,
        Optional.of(9223372036854775807f),
        4000000000000f,
        Optional.of(-9f),
        0f);
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
        .createStatement("INSERT INTO FloatParam VALUES (?,?,?)")
        .bind(0, "1")
        .bind(1, "9223372036854775807")
        .bind(2, "-9")
        .execute()
        .blockLast();
    validate(
        Optional.of(1f),
        0f,
        Optional.of(9223372036854775807f),
        4000000000000f,
        Optional.of(-9f),
        0f);
  }

  @Test
  void decimalValue() {
    decimalValue(sharedConn);
  }

  @Test
  void decimalValuePrepare() {
    decimalValue(sharedConnPrepare);
  }

  private void decimalValue(MariadbConnection connection) {
    connection
        .createStatement("INSERT INTO FloatParam VALUES (?,?,?)")
        .bind(0, BigDecimal.ONE)
        .bind(1, new BigDecimal("9223372036854775807"))
        .bind(2, new BigDecimal("-9"))
        .execute()
        .blockLast();

    validate(
        Optional.of(1f),
        0f,
        Optional.of(9223372036854775807f),
        4000000000000f,
        Optional.of(-9f),
        0f);
  }

  @Test
  void intValue() {
    intValue(sharedConn);
  }

  @Test
  void intValuePrepare() {
    intValue(sharedConnPrepare);
  }

  private void intValue(MariadbConnection connection) {
    connection
        .createStatement("INSERT INTO FloatParam VALUES (?,?,?)")
        .bind(0, 1)
        .bind(1, -1)
        .bind(2, 0)
        .execute()
        .blockLast();
    validate(Optional.of(1f), 0f, Optional.of(-1f), 0f, Optional.of(0f), 0f);
  }

  @Test
  void byteValue() {
    byteValue(sharedConn);
  }

  @Test
  void byteValuePrepare() {
    byteValue(sharedConnPrepare);
  }

  private void byteValue(MariadbConnection connection) {
    connection
        .createStatement("INSERT INTO FloatParam VALUES (?,?,?)")
        .bind(0, (byte) 127)
        .bind(1, (byte) -128)
        .bind(2, (byte) 0)
        .execute()
        .blockLast();
    validate(Optional.of(127f), 0f, Optional.of(-128f), 0f, Optional.of(0f), 0f);
  }

  @Test
  void floatValue() {
    floatValue(sharedConn);
  }

  @Test
  void floatValuePrepare() {
    floatValue(sharedConnPrepare);
  }

  private void floatValue(MariadbConnection connection) {
    connection
        .createStatement("INSERT INTO FloatParam VALUES (?,?,?)")
        .bind(0, 127f)
        .bind(1, -128f)
        .bind(2, 0f)
        .execute()
        .blockLast();
    validate(Optional.of(127f), 0f, Optional.of(-128f), 0f, Optional.of(0f), 0f);
  }

  @Test
  void doubleValue() {
    doubleValue(sharedConn);
  }

  @Test
  void doubleValuePrepare() {
    doubleValue(sharedConnPrepare);
  }

  private void doubleValue(MariadbConnection connection) {
    connection
        .createStatement("INSERT INTO FloatParam VALUES (?,?,?)")
        .bind(0, 127d)
        .bind(1, -128d)
        .bind(2, 0d)
        .execute()
        .blockLast();
    validate(Optional.of(127f), 0f, Optional.of(-128f), 0f, Optional.of(0f), 0f);
  }

  @Test
  void shortValue() {
    shortValue(sharedConn);
  }

  @Test
  void shortValuePrepare() {
    shortValue(sharedConnPrepare);
  }

  private void shortValue(MariadbConnection connection) {
    connection
        .createStatement("INSERT INTO FloatParam VALUES (?,?,?)")
        .bind(0, Short.valueOf("1"))
        .bind(1, Short.valueOf("-1"))
        .bind(2, Short.valueOf("0"))
        .execute()
        .blockLast();
    validate(Optional.of(1f), 0f, Optional.of(-1f), 0f, Optional.of(0f), 0f);
  }

  @Test
  void longValue() {
    longValue(sharedConn);
  }

  @Test
  void longValuePrepare() {
    longValue(sharedConnPrepare);
  }

  private void longValue(MariadbConnection connection) {
    connection
        .createStatement("INSERT INTO FloatParam VALUES (?,?,?)")
        .bind(0, Long.valueOf("1"))
        .bind(1, Long.valueOf("-1"))
        .bind(2, Long.valueOf("0"))
        .execute()
        .blockLast();
    validate(Optional.of(1f), 0f, Optional.of(-1f), 0f, Optional.of(0f), 0f);
  }

  private void validate(
      Optional<Float> t1,
      float t1Delta,
      Optional<Float> t2,
      float t2Delta,
      Optional<Float> t3,
      float t3Delta) {
    sharedConn
        .createStatement("SELECT * FROM FloatParam")
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) ->
                        Flux.just(
                            Optional.ofNullable((Float) row.get(0)),
                            Optional.ofNullable((Float) row.get(1)),
                            Optional.ofNullable((Float) row.get(2)))))
        .blockLast()
        .as(StepVerifier::create)
        .expectNextMatches(
            val -> {
              if (!val.isPresent()) {
                return !t1.isPresent();
              }
              Assertions.assertEquals(val.get(), t1.get(), t1Delta);
              return true;
            })
        .expectNextMatches(
            val -> {
              if (!val.isPresent()) {
                return !t2.isPresent();
              }
              Assertions.assertEquals(val.get(), t2.get(), t2Delta);
              return true;
            })
        .expectNextMatches(
            val -> {
              if (!val.isPresent()) {
                return !t3.isPresent();
              }
              Assertions.assertEquals(val.get(), t3.get(), t3Delta);
              return true;
            })
        .verifyComplete();
  }
}
