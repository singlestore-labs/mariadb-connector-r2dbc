// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.integration.parameter;

import io.r2dbc.spi.R2dbcBadGrammarException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Optional;

import org.junit.jupiter.api.*;
import com.singlestore.r2dbc.BaseConnectionTest;
import com.singlestore.r2dbc.api.SingleStoreConnection;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class MediumIntParameterTest extends BaseConnectionTest {
  @BeforeAll
  public static void before2() {
    sharedConn
        .createStatement("CREATE TABLE MediumIntParam (t1 MEDIUMINT, t2 MEDIUMINT, t3 MEDIUMINT)")
        .execute()
        .blockLast();
  }

  @AfterAll
  public static void after2() {
    sharedConn.createStatement("DROP TABLE MediumIntParam").execute().blockLast();
  }

  @BeforeEach
  public void beforeEach() {
    sharedConn.createStatement("TRUNCATE TABLE MediumIntParam").execute().blockLast();
  }

  @Test
  void nullValue() {
    nullValue(sharedConn);
  }

  @Test
  void nullValuePrepare() {
    nullValue(sharedConnPrepare);
  }

  private void nullValue(SingleStoreConnection connection) {
    connection
        .createStatement("INSERT INTO MediumIntParam VALUES (?,?,?)")
        .bindNull(0, BigInteger.class)
        .bindNull(1, BigInteger.class)
        .bindNull(2, BigInteger.class)
        .execute()
        .blockLast();
    validate(Optional.empty(), Optional.empty(), Optional.empty());
  }

  @Test
  void bigIntValue() {
    bigIntValue(sharedConn);
  }

  @Test
  void bigIntValuePrepare() {
    bigIntValue(sharedConnPrepare);
  }

  private void bigIntValue(SingleStoreConnection connection) {
    connection
        .createStatement("INSERT INTO MediumIntParam VALUES (?,?,?)")
        .bind(0, BigInteger.ONE)
        .bind(1, new BigInteger("8388607"))
        .bind(2, new BigInteger("-9"))
        .execute()
        .blockLast();
    validate(Optional.of(1), Optional.of(8388607), Optional.of(-9));
  }

  @Test
  void stringValue() {
    stringValue(sharedConn);
  }

  @Test
  void stringValuePrepare() {
    stringValue(sharedConnPrepare);
  }

  private void stringValue(SingleStoreConnection connection) {
    connection
        .createStatement("INSERT INTO MediumIntParam VALUES (?,?,?)")
        .bind(0, "1")
        .bind(1, "8388607")
        .bind(2, "-9")
        .execute()
        .blockLast();
    validate(Optional.of(1), Optional.of(8388607), Optional.of(-9));
  }

  @Test
  void decimalValue() {
    decimalValue(sharedConn);
  }

  @Test
  void decimalValuePrepare() {
    decimalValue(sharedConnPrepare);
  }

  private void decimalValue(SingleStoreConnection connection) {
    connection
        .createStatement("INSERT INTO MediumIntParam VALUES (?,?,?)")
        .bind(0, BigDecimal.ONE)
        .bind(1, new BigDecimal("8388607"))
        .bind(2, new BigDecimal("-9"))
        .execute()
        .blockLast();
    validate(Optional.of(1), Optional.of(8388607), Optional.of(-9));
  }

  @Test
  void intValue() {
    intValue(sharedConn);
  }

  @Test
  void intValuePrepare() {
    intValue(sharedConnPrepare);
  }

  private void intValue(SingleStoreConnection connection) {
    connection
        .createStatement("INSERT INTO MediumIntParam VALUES (?,?,?)")
        .bind(0, 1)
        .bind(1, 8388607)
        .bind(2, -9)
        .execute()
        .blockLast();
    validate(Optional.of(1), Optional.of(8388607), Optional.of(-9));
  }

  @Test
  void byteValue() {
    byteValue(sharedConn);
  }

  @Test
  void byteValuePrepare() {
    byteValue(sharedConnPrepare);
  }

  private void byteValue(SingleStoreConnection connection) {
    connection
        .createStatement("INSERT INTO MediumIntParam VALUES (?,?,?)")
        .bind(0, (byte) 127)
        .bind(1, (byte) -128)
        .bind(2, (byte) 0)
        .execute()
        .blockLast();
    validate(Optional.of(127), Optional.of(-128), Optional.of(0));
  }

  @Test
  void floatValue() {
    floatValue(sharedConn);
  }

  @Test
  void floatValuePrepare() {
    floatValue(sharedConnPrepare);
  }

  private void floatValue(SingleStoreConnection connection) {
    connection
        .createStatement("INSERT INTO MediumIntParam VALUES (?,?,?)")
        .bind(0, 127f)
        .bind(1, -128f)
        .bind(2, 0f)
        .execute()
        .blockLast();
    validate(Optional.of(127), Optional.of(-128), Optional.of(0));
  }

  @Test
  void doubleValue() {
    doubleValue(sharedConn);
  }

  @Test
  void doubleValuePrepare() {
    doubleValue(sharedConnPrepare);
  }

  private void doubleValue(SingleStoreConnection connection) {
    connection
        .createStatement("INSERT INTO MediumIntParam VALUES (?,?,?)")
        .bind(0, 127d)
        .bind(1, -128d)
        .bind(2, 0d)
        .execute()
        .blockLast();
    validate(Optional.of(127), Optional.of(-128), Optional.of(0));
  }

  @Test
  void shortValue() {
    shortValue(sharedConn);
  }

  @Test
  void shortValuePrepare() {
    shortValue(sharedConnPrepare);
  }

  private void shortValue(SingleStoreConnection connection) {
    connection
        .createStatement("INSERT INTO MediumIntParam VALUES (?,?,?)")
        .bind(0, Short.valueOf("1"))
        .bind(1, Short.valueOf("-1"))
        .bind(2, Short.valueOf("0"))
        .execute()
        .blockLast();
    validate(Optional.of(1), Optional.of(-1), Optional.of(0));
  }

  @Test
  void longValue() {
    longValue(sharedConn);
  }

  @Test
  void longValuePrepare() {
    longValue(sharedConnPrepare);
  }

  private void longValue(SingleStoreConnection connection) {
    connection
        .createStatement("INSERT INTO MediumIntParam VALUES (?,?,?)")
        .bind(0, Long.valueOf("1"))
        .bind(1, Long.valueOf("-1"))
        .bind(2, Long.valueOf("0"))
        .execute()
        .blockLast();
    validate(Optional.of(1), Optional.of(-1), Optional.of(0));
  }

  @Test
  void localDateTimeValue() {
    sharedConn
        .createStatement("INSERT INTO MediumIntParam VALUES (?,?,?)")
        .bind(0, LocalDateTime.now())
        .bind(1, LocalDateTime.now())
        .bind(2, LocalDateTime.now())
        .execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcBadGrammarException
                    && ((R2dbcBadGrammarException) throwable).getSqlState().equals("22003"))
        .verify();
  }

  @Test
  void localDateValue() {
    sharedConn
        .createStatement("INSERT INTO MediumIntParam VALUES (?,?,?)")
        .bind(0, LocalDate.now())
        .bind(1, LocalDate.now())
        .bind(2, LocalDate.now())
        .execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcBadGrammarException
                    && ((R2dbcBadGrammarException) throwable).getSqlState().equals("22003"))
        .verify();
  }

  @Test
  void localTimeValue() {
    sharedConn
        .createStatement("INSERT INTO MediumIntParam VALUES (?,?,?)")
        .bind(0, LocalTime.now())
        .bind(1, LocalTime.now())
        .bind(2, LocalTime.now())
        .execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcBadGrammarException
                    && ((R2dbcBadGrammarException) throwable).getSqlState().equals("22003"))
        .verify();
  }

  private void validate(Optional<Integer> t1, Optional<Integer> t2, Optional<Integer> t3) {
    sharedConn
        .createStatement("SELECT * FROM MediumIntParam")
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) ->
                        Flux.just(
                            Optional.ofNullable((Integer) row.get(0)),
                            Optional.ofNullable(row.get(1)),
                            Optional.ofNullable(row.get(2)))))
        .blockLast()
        .as(StepVerifier::create)
        .expectNext(t1, t2, t3)
        .verifyComplete();
  }
}
