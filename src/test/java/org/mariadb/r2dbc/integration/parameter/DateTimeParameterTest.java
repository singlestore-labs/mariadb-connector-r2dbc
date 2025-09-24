// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration.parameter;

import io.r2dbc.spi.R2dbcTransientResourceException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Optional;

import org.junit.jupiter.api.*;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.api.MariadbConnectionMetadata;
import org.mariadb.r2dbc.api.MariadbResult;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class DateTimeParameterTest extends BaseConnectionTest {
  @BeforeAll
  public static void before2() {
    sharedConn
        .createStatement(
            "CREATE TABLE DateTimeParam (t1 DATETIME(6), t2 DATETIME(6), t3 DATETIME(6))")
        .execute()
        .blockLast();
  }

  @AfterAll
  public static void after2() {
    sharedConn.createStatement("DROP TABLE DateTimeParam").execute().blockLast();
  }

  @BeforeEach
  public void beforeEach() {
    sharedConn.createStatement("TRUNCATE TABLE DateTimeParam").execute().blockLast();
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
        .createStatement("INSERT INTO DateTimeParam VALUES (?,?,?)")
        .bindNull(0, LocalDateTime.class)
        .bindNull(1, LocalDateTime.class)
        .bindNull(2, LocalDateTime.class)
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

  private void bigIntValue(MariadbConnection connection) {
    Flux<MariadbResult> f =
        connection
            .createStatement("INSERT INTO DateTimeParam VALUES (?,?,?)")
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

  private void stringValue(MariadbConnection connection) {
    Flux<MariadbResult> f =
        connection
            .createStatement("INSERT INTO DateTimeParam VALUES (?,?,?)")
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

  private void decimalValue(MariadbConnection connection) {
    Flux<MariadbResult> f =
        connection
            .createStatement("INSERT INTO DateTimeParam VALUES (?,?,?)")
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

  private void intValue(MariadbConnection connection) {
    Flux<MariadbResult> f =
        connection
            .createStatement("INSERT INTO DateTimeParam VALUES (?,?,?)")
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

  private void byteValue(MariadbConnection connection) {
    Flux<MariadbResult> f =
        connection
            .createStatement("INSERT INTO DateTimeParam VALUES (?,?,?)")
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

  private void floatValue(MariadbConnection connection) {
    Flux<MariadbResult> f =
        connection
            .createStatement("INSERT INTO DateTimeParam VALUES (?,?,?)")
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

  private void doubleValue(MariadbConnection connection) {
    Flux<MariadbResult> f =
        connection
            .createStatement("INSERT INTO DateTimeParam VALUES (?,?,?)")
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

  private void shortValue(MariadbConnection connection) {
    Flux<MariadbResult> f =
        connection
            .createStatement("INSERT INTO DateTimeParam VALUES (?,?,?)")
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

  private void longValue(MariadbConnection connection) {
    Flux<MariadbResult> f =
        connection
            .createStatement("INSERT INTO DateTimeParam VALUES (?,?,?)")
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

  private void localDateTimeValue(MariadbConnection connection) {
    connection
        .createStatement("INSERT INTO DateTimeParam VALUES (?,?,?)")
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

  private void localDateValue(MariadbConnection connection) {
    connection
        .createStatement("INSERT INTO DateTimeParam VALUES (?,?,?)")
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

  @Test
  void localTimeValuePrepare() {
    LocalTime localTime = LocalTime.parse("05:08:10.123456");
    LocalDateTime localDateTime =
        LocalDateTime.now().withHour(5).withMinute(8).withSecond(10).withNano(123456 * 1000);
    MariadbConnectionMetadata meta = sharedConn.getMetadata();
    localDateTime = localDateTime.withYear(0).withDayOfMonth(1).withMonth(1);
    sharedConnPrepare
        .createStatement("INSERT INTO DateTimeParam VALUES (? :> TIME(6),? :> TIME(6),? :> TIME(6))")
        .bind(0, localTime)
        .bind(1, localTime)
        .bind(2, localTime)
        .execute()
        .blockLast();
    validate(Optional.of(localDateTime), Optional.of(localDateTime), Optional.of(localDateTime));
  }

  private void localTimeValue(MariadbConnection connection) {
    LocalTime localTime = LocalTime.parse("05:08:10.123456");
    connection
        .createStatement("INSERT INTO DateTimeParam VALUES (?,?,?)")
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
        .createStatement("SELECT * FROM DateTimeParam")
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
