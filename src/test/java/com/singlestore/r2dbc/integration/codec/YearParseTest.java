// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.integration.codec;

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
import com.singlestore.r2dbc.BaseConnectionTest;
import com.singlestore.r2dbc.api.SingleStoreConnection;
import com.singlestore.r2dbc.api.SingleStoreConnectionMetadata;
import com.singlestore.r2dbc.util.SingleStoreType;
import reactor.test.StepVerifier;

public class YearParseTest extends BaseConnectionTest {
  private static final SingleStoreConnectionMetadata meta = sharedConn.getMetadata();

  @BeforeAll
  public static void before2() {
    // xpand doesn't support YEAR 2
    Assumptions.assumeFalse(isXpand());
    afterAll2();
    sharedConn.beginTransaction().block();
    String sqlCreate = "CREATE TABLE YearTable (t1 YEAR(4), t2 YEAR(4), t3 INT)";
    String sqlInsert = "INSERT INTO YearTable VALUES (2060, 2060, 1),(2071, 1971, 2),(2000, 2000, 3), (null, null, 4)";

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

  private void defaultValue(SingleStoreConnection connection) {
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
            Optional.of((short) (2060)),
            Optional.of((short) (1971)),
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

  private void booleanValue(SingleStoreConnection connection) {
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

  private void byteArrayValue(SingleStoreConnection connection) {
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

  private void ByteValue(SingleStoreConnection connection) {
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

  private void byteValue(SingleStoreConnection connection) {
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

  private void shortValue(SingleStoreConnection connection) {
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
            Optional.of((short) (2060)),
            Optional.of((short) (1971)),
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

  private void intValue(SingleStoreConnection connection) {
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
            Optional.of(2060),
            Optional.of(1971),
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

  private void longValue(SingleStoreConnection connection) {
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
            Optional.of(2060L),
            Optional.of(1971L),
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

  private void floatValue(SingleStoreConnection connection) {
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
            Optional.of(2060F),
            Optional.of(1971F),
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

  private void doubleValue(SingleStoreConnection connection) {
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
            Optional.of(2060D),
            Optional.of(1971D),
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

  private void stringValue(SingleStoreConnection connection) {
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
            Optional.of("2060"),
            Optional.of("1971"),
            Optional.of("2000"),
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

  private void localDateValue(SingleStoreConnection connection) {
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
            Optional.of(LocalDate.parse("2000-01-01")),
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

  private void decimalValue(SingleStoreConnection connection) {
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
            Optional.of(new BigDecimal("2060")),
            Optional.of(new BigDecimal("1971")),
            Optional.of(new BigDecimal("2000")),
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

  private void bigintValue(SingleStoreConnection connection) {
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
            Optional.of(new BigInteger("2060")),
            Optional.of(new BigInteger("1971")),
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

  private void meta(SingleStoreConnection connection) {
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
        .expectNextMatches(c -> c.equals(SingleStoreType.SMALLINT))
        .verifyComplete();
  }
}
