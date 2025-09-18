// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration.codec;

import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcTransientResourceException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.api.MariadbConnectionMetadata;
import org.mariadb.r2dbc.util.MariadbType;
import reactor.test.StepVerifier;

public class YearParseTest extends BaseConnectionTest {
  private static final MariadbConnectionMetadata meta = sharedConn.getMetadata();

  @BeforeAll
  public static void before2() {
    // xpand doesn't support YEAR 2
    Assumptions.assumeFalse(isXpand());
    afterAll2();
    sharedConn.beginTransaction().block();
    String sqlCreate = "CREATE TABLE YearTable (t1 YEAR(4), t2 YEAR(2), t3 INT)";
    String sqlInsert = "INSERT INTO YearTable VALUES (2060, 60, 1),(2071, 71, 2),(0, 0, 3), (null, null, 4)";
    // mysql doesn't support YEAR(2) anymore
    if (!meta.isMariaDBServer()) {
      sqlCreate = "CREATE TABLE YearTable (t1 YEAR(4), t2 YEAR(4), t3 INT)";
      sqlInsert = "INSERT INTO YearTable VALUES (2060, 2060, 1),(2071, 1971, 2),(2000, 2000, 3), (null, null, 4)";
    }

    sharedConn.createStatement(sqlCreate).execute().blockLast();
    sharedConn.createStatement(sqlInsert).execute().blockLast();
    sharedConn.createStatement("FLUSH TABLES").execute().blockLast();
    sharedConn.commitTransaction().block();
  }

  @AfterAll
  public static void afterAll2() {
    sharedConn.createStatement("DROP TABLE IF EXISTS YearTable").execute().blockLast();
  }

  @Test
  void wrongType() {
    sharedConn
        .createStatement("SELECT t1 FROM YearTable WHERE 1 = ? ORDER BY t3")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, this.getClass()))))
        .as(StepVerifier::create)
        .expectError();
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
        .createStatement("SELECT t1 FROM YearTable WHERE 1 = ? ORDER BY t3")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of((short) 2060),
            Optional.of((short) 2071),
            Optional.of((short) 2000),
            Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t2 FROM YearTable WHERE 1 = ? ORDER BY t3")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of((short) (meta.isMariaDBServer() ? 60 : 2060)),
            Optional.of((short) (meta.isMariaDBServer() ? 71 : 1971)),
            Optional.of((short) 2000),
            Optional.empty())
        .verifyComplete();
  }

  @Test
  void booleanValue() {
    booleanValue(sharedConn);
  }

  @Test
  void booleanValuePrepare() {
    booleanValue(sharedConnPrepare);
  }

  private void booleanValue(MariadbConnection connection) {
    connection
        .createStatement("SELECT t2 FROM YearTable WHERE 1 = ? ORDER BY t3 LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Boolean.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.lang.Boolean and column type YEAR"))
        .verify();
  }

  @Test
  void byteArrayValue() {
    byteArrayValue(sharedConn);
  }

  @Test
  void byteArrayValuePrepare() {
    byteArrayValue(sharedConnPrepare);
  }

  private void byteArrayValue(MariadbConnection connection) {
    connection
        .createStatement("SELECT t1 FROM YearTable WHERE 1 = ? ORDER BY t3 LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, byte[].class)))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type byte[] and column type YEAR"))
        .verify();
  }

  @Test
  void ByteValue() {
    ByteValue(sharedConn);
  }

  @Test
  void ByteValuePrepare() {
    ByteValue(sharedConnPrepare);
  }

  private void ByteValue(MariadbConnection connection) {
    connection
        .createStatement("SELECT t1 FROM YearTable WHERE 1 = ? ORDER BY t3 LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Byte.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && throwable.getMessage().equals("byte overflow"))
        .verify();
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
        .createStatement("SELECT t1 FROM YearTable WHERE 1 = ? ORDER BY t3 LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, byte.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && throwable.getMessage().equals("byte overflow"))
        .verify();
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
        .createStatement("SELECT t1 FROM YearTable WHERE 1 = ? ORDER BY t3")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of((short) 2060),
            Optional.of((short) 2071),
            Optional.of((short) 2000),
            Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t2 FROM YearTable WHERE 1 = ? ORDER BY t3")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of((short) (meta.isMariaDBServer() ? 60 : 2060)),
            Optional.of((short) (meta.isMariaDBServer() ? 71 : 1971)),
            Optional.of((short) (2000)),
            Optional.empty())
        .verifyComplete();
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
        .createStatement("SELECT t1 FROM YearTable WHERE 1 = ? ORDER BY t3")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Integer.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(2060), Optional.of(2071), Optional.of(2000), Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t2 FROM YearTable WHERE 1 = ? ORDER BY t3")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Integer.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(meta.isMariaDBServer() ? 60 : 2060),
            Optional.of(meta.isMariaDBServer() ? 71 : 1971),
            Optional.of(2000),
            Optional.empty())
        .verifyComplete();
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
        .createStatement("SELECT t1 FROM YearTable WHERE 1 = ? ORDER BY t3")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Long.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(2060L), Optional.of(2071L), Optional.of(2000L), Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t2 FROM YearTable WHERE 1 = ? ORDER BY t3")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Long.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(meta.isMariaDBServer() ? 60L : 2060L),
            Optional.of(meta.isMariaDBServer() ? 71L : 1971L),
            Optional.of(2000L),
            Optional.empty())
        .verifyComplete();
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
        .createStatement("SELECT t1 FROM YearTable WHERE 1 = ? ORDER BY t3")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Float.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(2060f), Optional.of(2071f), Optional.of(2000f), Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t2 FROM YearTable WHERE 1 = ? ORDER BY t3")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Float.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(meta.isMariaDBServer() ? 60F : 2060F),
            Optional.of(meta.isMariaDBServer() ? 71F : 1971F),
            Optional.of(2000F),
            Optional.empty())
        .verifyComplete();
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
        .createStatement("SELECT t1 FROM YearTable WHERE 1 = ? ORDER BY t3")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Double.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(2060D), Optional.of(2071D), Optional.of(2000D), Optional.empty())
        .verifyComplete();

    connection
        .createStatement("SELECT t2 FROM YearTable WHERE 1 = ? ORDER BY t3")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Double.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(meta.isMariaDBServer() ? 60D : 2060D),
            Optional.of(meta.isMariaDBServer() ? 71D : 1971D),
            Optional.of(2000D),
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
        .createStatement("SELECT t1 FROM YearTable WHERE 1 = ? ORDER BY t3")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, String.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of("2060"), Optional.of("2071"), Optional.of("2000"), Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t2 FROM YearTable WHERE 1 = ? ORDER BY t3")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, String.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(meta.isMariaDBServer() ? "60" : "2060"),
            Optional.of(meta.isMariaDBServer() ? "71" : "1971"),
            Optional.of(meta.isMariaDBServer() ? "00" : "2000"),
            Optional.empty())
        .verifyComplete();
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
        .createStatement("SELECT t1 FROM YearTable WHERE 1 = ? ORDER BY t3")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, LocalDate.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(LocalDate.parse("2060-01-01")),
            Optional.of(LocalDate.parse("2071-01-01")),
            Optional.of(LocalDate.parse("2000-01-01")),
            Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t2 FROM YearTable WHERE 1 = ? ORDER BY t3")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, LocalDate.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(LocalDate.parse("2060-01-01")),
            Optional.of(LocalDate.parse("1971-01-01")),
            Optional.of(LocalDate.parse(meta.isMariaDBServer() ? "2000-01-01" : "2000-01-01")),
            Optional.empty())
        .verifyComplete();
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
        .createStatement("SELECT t1 FROM YearTable WHERE 1 = ? ORDER BY t3")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, BigDecimal.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(new BigDecimal("2060")),
            Optional.of(new BigDecimal("2071")),
            Optional.of(new BigDecimal("2000")),
            Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t2 FROM YearTable WHERE 1 = ? ORDER BY t3")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, BigDecimal.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(new BigDecimal(meta.isMariaDBServer() ? "60" : "2060")),
            Optional.of(new BigDecimal(meta.isMariaDBServer() ? "71" : "1971")),
            Optional.of(new BigDecimal(meta.isMariaDBServer() ? "00" : "2000")),
            Optional.empty())
        .verifyComplete();
  }

  @Test
  void bigintValue() {
    bigintValue(sharedConn);
  }

  @Test
  void bigintValuePrepare() {
    bigintValue(sharedConnPrepare);
  }

  private void bigintValue(MariadbConnection connection) {
    connection
        .createStatement("SELECT t1 FROM YearTable WHERE 1 = ? ORDER BY t3")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, BigInteger.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(new BigInteger("2060")),
            Optional.of(new BigInteger("2071")),
            Optional.of(new BigInteger("2000")),
            Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t2 FROM YearTable WHERE 1 = ? ORDER BY t3")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, BigInteger.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(new BigInteger(meta.isMariaDBServer() ? "60" : "2060")),
            Optional.of(new BigInteger(meta.isMariaDBServer() ? "71" : "1971")),
            Optional.of(new BigInteger("2000")),
            Optional.empty())
        .verifyComplete();
  }

  @Test
  void meta() {
    meta(sharedConn);
  }

  @Test
  void metaPrepare() {
    meta(sharedConnPrepare);
  }

  private void meta(MariadbConnection connection) {
    connection
        .createStatement("SELECT t1 FROM YearTable WHERE 1 = ? ORDER BY t3 LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> metadata.getColumnMetadata(0).getJavaType()))
        .as(StepVerifier::create)
        .expectNextMatches(c -> c.equals(Short.class))
        .verifyComplete();
    connection
        .createStatement("SELECT t1 FROM YearTable WHERE 1 = ? ORDER BY t3 LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> metadata.getColumnMetadata(0).getType()))
        .as(StepVerifier::create)
        .expectNextMatches(c -> c.equals(MariadbType.SMALLINT))
        .verifyComplete();
  }
}
