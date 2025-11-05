// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.integration.parameter;

import io.r2dbc.spi.R2dbcTransientResourceException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Optional;

import org.junit.jupiter.api.*;
import com.singlestore.r2dbc.BaseConnectionTest;
import com.singlestore.r2dbc.api.SingleStoreConnection;
import com.singlestore.r2dbc.api.SingleStoreResult;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class TimeStampParameterTest extends BaseConnectionTest {
  @BeforeAll
  public static void before2() {
    sharedConn
        .createStatement(
            "CREATE TABLE TimestampParam (t1 TIMESTAMP(6) NULL, t2 TIMESTAMP(6) NULL, t3"
                + " TIMESTAMP(6) NULL)")
        .execute()
        .blockLast();
  }

  @AfterAll
  public static void after2() {
    sharedConn.createStatement("DROP TABLE TimestampParam").execute().blockLast();
  }

  @BeforeEach
  public void beforeEach() {
    sharedConn.createStatement("TRUNCATE TABLE TimestampParam").execute().blockLast();
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
    Flux<SingleStoreResult> f =
        connection
            .createStatement("INSERT INTO TimestampParam VALUES (?,?,?)")
            .bind(0, BigInteger.ONE)
            .bind(1, new BigInteger("9223372036854775807"))
            .bind(2, new BigInteger("-9"))
            .execute();
    if ((isMariaDBServer() && !minVersion(10, 2, 0))
        || (!isMariaDBServer() && !minVersion(5, 7, 0))) {
      f.blockLast();
    } else {
      f.flatMap(r -> r.getRowsUpdated())
          .as(StepVerifier::create)
          .expectErrorMatches(
              throwable ->
                  throwable instanceof R2dbcTransientResourceException
                      && ((R2dbcTransientResourceException) throwable).getSqlState().equals("HY000"))
          .verify();
    }
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
    Flux<SingleStoreResult> f =
        connection
            .createStatement("INSERT INTO TimestampParam VALUES (?,?,?)")
            .bind(0, "1")
            .bind(1, "9223372036854775807")
            .bind(2, "-9")
            .execute();
    if ((isMariaDBServer() && !minVersion(10, 2, 0))
        || (!isMariaDBServer() && !minVersion(5, 7, 0))) {
      f.blockLast();
    } else {
      f.flatMap(r -> r.getRowsUpdated())
          .as(StepVerifier::create)
          .expectErrorMatches(
              throwable ->
                  throwable instanceof R2dbcTransientResourceException
                      && ((R2dbcTransientResourceException) throwable).getSqlState().equals("HY000"))
          .verify();
    }
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
    Flux<SingleStoreResult> f =
        connection
            .createStatement("INSERT INTO TimestampParam VALUES (?,?,?)")
            .bind(0, BigDecimal.ONE)
            .bind(1, new BigDecimal("9223372036854775807"))
            .bind(2, new BigDecimal("-9"))
            .execute();
    if ((isMariaDBServer() && !minVersion(10, 2, 0))
        || (!isMariaDBServer() && !minVersion(5, 7, 0))) {
      f.blockLast();
    } else {
      f.flatMap(r -> r.getRowsUpdated())
          .as(StepVerifier::create)
          .expectErrorMatches(
              throwable ->
                  throwable instanceof R2dbcTransientResourceException
                      && ((R2dbcTransientResourceException) throwable).getSqlState().equals("HY000"))
          .verify();
    }
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
    Flux<SingleStoreResult> f =
        connection
            .createStatement("INSERT INTO TimestampParam VALUES (?,?,?)")
            .bind(0, 1)
            .bind(1, -1)
            .bind(2, 0)
            .execute();
    if ((isMariaDBServer() && !minVersion(10, 2, 0))
        || (!isMariaDBServer() && !minVersion(5, 7, 0))) {
      f.blockLast();
    } else {
      f.flatMap(r -> r.getRowsUpdated())
          .as(StepVerifier::create)
          .expectErrorMatches(
              throwable ->
                  throwable instanceof R2dbcTransientResourceException
                      && ((R2dbcTransientResourceException) throwable).getSqlState().equals("HY000"))
          .verify();
    }
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
    Flux<SingleStoreResult> f =
        connection
            .createStatement("INSERT INTO TimestampParam VALUES (?,?,?)")
            .bind(0, (byte) 127)
            .bind(1, (byte) 128)
            .bind(2, (byte) 0)
            .execute();
    if ((isMariaDBServer() && !minVersion(10, 2, 0))
        || (!isMariaDBServer() && !minVersion(5, 7, 0))) {
      f.blockLast();
    } else {
      f.flatMap(r -> r.getRowsUpdated())
          .as(StepVerifier::create)
          .expectErrorMatches(
              throwable ->
                  throwable instanceof R2dbcTransientResourceException
                      && ((R2dbcTransientResourceException) throwable).getSqlState().equals("HY000"))
          .verify();
    }
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
    Flux<SingleStoreResult> f =
        connection
            .createStatement("INSERT INTO TimestampParam VALUES (?,?,?)")
            .bind(0, 127f)
            .bind(1, -128f)
            .bind(2, 0f)
            .execute();
    if ((isMariaDBServer() && !minVersion(10, 2, 0))
        || (!isMariaDBServer() && !minVersion(5, 7, 0))) {
      f.blockLast();
    } else {
      f.flatMap(r -> r.getRowsUpdated())
          .as(StepVerifier::create)
          .expectErrorMatches(
              throwable ->
                  throwable instanceof R2dbcTransientResourceException
                      && ((R2dbcTransientResourceException) throwable).getSqlState().equals("HY000"))
          .verify();
    }
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
    Flux<SingleStoreResult> f =
        connection
            .createStatement("INSERT INTO TimestampParam VALUES (?,?,?)")
            .bind(0, 127d)
            .bind(1, -128d)
            .bind(2, 0d)
            .execute();
    if ((isMariaDBServer() && !minVersion(10, 2, 0))
        || (!isMariaDBServer() && !minVersion(5, 7, 0))) {
      f.blockLast();
    } else {
      f.flatMap(r -> r.getRowsUpdated())
          .as(StepVerifier::create)
          .expectErrorMatches(
              throwable ->
                  throwable instanceof R2dbcTransientResourceException
                      && ((R2dbcTransientResourceException) throwable).getSqlState().equals("HY000"))
          .verify();
    }
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
    Flux<SingleStoreResult> f =
        connection
            .createStatement("INSERT INTO TimestampParam VALUES (?,?,?)")
            .bind(0, Short.valueOf("1"))
            .bind(1, Short.valueOf("-1"))
            .bind(2, Short.valueOf("0"))
            .execute();
    if ((isMariaDBServer() && !minVersion(10, 2, 0))
        || (!isMariaDBServer() && !minVersion(5, 7, 0))) {
      f.blockLast();
    } else {
      f.flatMap(r -> r.getRowsUpdated())
          .as(StepVerifier::create)
          .expectErrorMatches(
              throwable ->
                  throwable instanceof R2dbcTransientResourceException
                      && ((R2dbcTransientResourceException) throwable).getSqlState().equals("HY000"))
          .verify();
    }
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
    Flux<SingleStoreResult> f =
        connection
            .createStatement("INSERT INTO TimestampParam VALUES (?,?,?)")
            .bind(0, Long.valueOf("1"))
            .bind(1, Long.valueOf("-1"))
            .bind(2, Long.valueOf("0"))
            .execute();
    if ((isMariaDBServer() && !minVersion(10, 2, 0))
        || (!isMariaDBServer() && !minVersion(5, 7, 0))) {
      f.blockLast();
    } else {
      f.flatMap(r -> r.getRowsUpdated())
          .as(StepVerifier::create)
          .expectErrorMatches(
              throwable ->
                  throwable instanceof R2dbcTransientResourceException
                      && ((R2dbcTransientResourceException) throwable).getSqlState().equals("HY000"))
          .verify();
    }
  }

  @Test
  void localDateTimeValue() {
    localDateTimeValue(sharedConn);
  }

  @Test
  void localDateTimeValuePrepare() {
    localDateTimeValue(sharedConnPrepare);
  }

  private void localDateTimeValue(SingleStoreConnection connection) {
    connection
        .createStatement("INSERT INTO TimestampParam VALUES (?,?,?)")
        .bind(0, LocalDateTime.parse("2010-01-12T05:08:09.0014"))
        .bind(1, LocalDateTime.parse("2018-12-15T05:08:10.123456"))
        .bind(2, LocalDateTime.parse("2025-05-12T05:08:11.123"))
        .execute()
        .blockLast();
    validate(
        Optional.of(LocalDateTime.parse("2010-01-12T05:08:09.001400")),
        Optional.of(LocalDateTime.parse("2018-12-15T05:08:10.123456")),
        Optional.of(LocalDateTime.parse("2025-05-12T05:08:11.123000")));
  }

  @Test
  void localDateValue() {
    localDateValue(sharedConn);
  }

  @Test
  void localDateValuePrepare() {
    localDateValue(sharedConnPrepare);
  }

  private void localDateValue(SingleStoreConnection connection) {
    connection
        .createStatement("INSERT INTO TimestampParam VALUES (?,?,?)")
        .bind(0, LocalDate.parse("2010-01-12"))
        .bind(1, LocalDate.parse("2018-12-15"))
        .bind(2, LocalDate.parse("2025-05-12"))
        .execute()
        .blockLast();
    validate(
        Optional.of(LocalDateTime.parse("2010-01-12T00:00:00.000000")),
        Optional.of(LocalDateTime.parse("2018-12-15T00:00:00.000000")),
        Optional.of(LocalDateTime.parse("2025-05-12T00:00:00.000000")));
  }

  @Test
  void localTimeValue() {
    localTimeValue(sharedConn);
  }

  private void localTimeValue(SingleStoreConnection connection) {
    LocalTime localTime = LocalTime.parse("05:08:10.123456");
    connection
        .createStatement("INSERT INTO TimestampParam VALUES (?,?,?)")
        .bind(0, localTime)
        .bind(1, localTime)
        .bind(2, localTime)
        .execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && ((R2dbcTransientResourceException) throwable).getSqlState().equals("HY000"))
        .verify();
  }

  private void validate(
      Optional<LocalDateTime> t1, Optional<LocalDateTime> t2, Optional<LocalDateTime> t3) {
    sharedConn
        .createStatement("SELECT * FROM TimestampParam")
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) ->
                        Flux.just(
                            Optional.ofNullable((LocalDateTime) row.get(0)),
                            Optional.ofNullable(row.get(1)),
                            Optional.ofNullable(row.get(2)))))
        .blockLast()
        .as(StepVerifier::create)
        .expectNext(t1, t2, t3)
        .verifyComplete();
  }
}
