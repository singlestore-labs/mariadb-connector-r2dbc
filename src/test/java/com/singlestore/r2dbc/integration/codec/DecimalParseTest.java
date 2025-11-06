// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.integration.codec;

import io.r2dbc.spi.Blob;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcTransientResourceException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import com.singlestore.r2dbc.BaseConnectionTest;
import com.singlestore.r2dbc.api.SingleStoreConnection;
import com.singlestore.r2dbc.util.SingleStoreType;
import reactor.test.StepVerifier;

public class DecimalParseTest extends BaseConnectionTest {
  @BeforeAll
  public static void before2() {
    afterAll2();
    sharedConn.beginTransaction().block();
    sharedConn
        .createStatement("CREATE TABLE DecimalTable (t1 DECIMAL(40,20), t2 INT)")
        .execute()
        .blockLast();
    sharedConn
        .createStatement(
            "INSERT INTO DecimalTable VALUES (0.1, 1),(1, 2),(9223372036854775807.9223372036854775807, 3),"
                + " (null, 4), (19223372036854775807.9223372036854775807, 5)")
        .execute()
        .blockLast();
    sharedConn.createStatement("FLUSH TABLES").execute().blockLast();
    sharedConn.commitTransaction().block();
  }

  @AfterAll
  public static void afterAll2() {
    sharedConn.createStatement("DROP TABLE IF EXISTS DecimalTable").execute().blockLast();
  }

  @Test
  void wrongType() {
    sharedConn
        .createStatement("SELECT t1 FROM DecimalTable WHERE 1 = ? ORDER BY t2")
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
        .createStatement("SELECT t1 FROM DecimalTable WHERE 1 = ? ORDER BY t2")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(new BigDecimal("0.10000000000000000000")),
            Optional.of(new BigDecimal("1.00000000000000000000")),
            Optional.of(new BigDecimal("9223372036854775807.92233720368547758070")),
            Optional.empty(),
            Optional.of(new BigDecimal("19223372036854775807.92233720368547758070")))
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
        .createStatement("SELECT t1 FROM DecimalTable WHERE 1 = ? ORDER BY t2")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Boolean.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(false),
            Optional.of(true),
            Optional.of(true),
            Optional.empty(),
            Optional.of(true))
        .verifyComplete();
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
        .createStatement("SELECT t1 FROM DecimalTable WHERE 1 = ? ORDER BY t2 LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, byte[].class)))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type byte[] and column type DECIMAL"))
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
        .createStatement("SELECT t1 FROM DecimalTable WHERE 1 = ? ORDER BY t2 LIMIT 3")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Byte.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of((byte) 0), Optional.of((byte) 1))
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && throwable
                        .getMessage()
                        .equals(
                            "value '9223372036854775807.92233720368547758070' (DECIMAL) cannot be"
                                + " decoded as Byte"))
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
        .createStatement("SELECT t1 FROM DecimalTable WHERE 1 = ? ORDER BY t2 LIMIT 3")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, byte.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of((byte) 0), Optional.of((byte) 1))
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && throwable
                        .getMessage()
                        .equals(
                            "value '9223372036854775807.92233720368547758070' (DECIMAL) cannot be"
                                + " decoded as Byte"))
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
        .createStatement("SELECT t1 FROM DecimalTable WHERE 1 = ? ORDER BY t2 LIMIT 3")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Short.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of((short) 0), Optional.of((short) 1))
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && throwable.getMessage().equals("Short overflow"))
        .verify();
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
        .createStatement("SELECT t1 FROM DecimalTable WHERE 1 = ? ORDER BY t2 LIMIT 3")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Integer.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(0), Optional.of(1))
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && throwable.getMessage().equals("integer overflow"))
        .verify();
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
        .createStatement("SELECT t1 FROM DecimalTable WHERE 1 = ? ORDER BY t2 LIMIT 5")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Long.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(0L), Optional.of(1L), Optional.of(9223372036854775807L), Optional.empty())
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && throwable
                        .getMessage()
                        .equals(
                            "value '19223372036854775807.92233720368547758070' cannot be decoded as"
                                + " Long"))
        .verify();
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
        .createStatement("SELECT t1 FROM DecimalTable WHERE 1 = ? ORDER BY t2")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Float.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(0.1F),
            Optional.of(1F),
            Optional.of(9223372036854775807.9223372036854775807F),
            Optional.empty(),
            Optional.of(19223372036854775807.9223372036854775807F))
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
        .createStatement("SELECT t1 FROM DecimalTable WHERE 1 = ? ORDER BY t2")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Double.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(0.1D),
            Optional.of(1D),
            Optional.of(9223372036854775807.9223372036854775807D),
            Optional.empty(),
            Optional.of(19223372036854775807.9223372036854775807D))
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
        .createStatement("SELECT t1 FROM DecimalTable WHERE 1 = ? ORDER BY t2")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, String.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of("0.10000000000000000000"),
            Optional.of("1.00000000000000000000"),
            Optional.of("9223372036854775807.92233720368547758070"),
            Optional.empty(),
            Optional.of("19223372036854775807.92233720368547758070"))
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
        .createStatement("SELECT t1 FROM DecimalTable WHERE 1 = ? ORDER BY t2")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, BigDecimal.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(new BigDecimal("0.10000000000000000000")),
            Optional.of(new BigDecimal("1.00000000000000000000")),
            Optional.of(new BigDecimal("9223372036854775807.92233720368547758070")),
            Optional.empty(),
            Optional.of(new BigDecimal("19223372036854775807.92233720368547758070")))
        .verifyComplete();
  }

  @Test
  void blobValue() {
    blobValue(sharedConn);
  }

  @Test
  void blobValuePrepare() {
    blobValue(sharedConnPrepare);
  }

  private void blobValue(SingleStoreConnection connection) {
    connection
        .createStatement("SELECT t1 FROM DecimalTable WHERE 1 = ? ORDER BY t2 LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) -> {
                      return row.get(0, Blob.class);
                    }))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type io.r2dbc.spi.Blob and column type DECIMAL"))
        .verify();
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
        .createStatement("SELECT t1 FROM DecimalTable WHERE 1 = ? ORDER BY t2")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, BigInteger.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(BigInteger.ZERO),
            Optional.of(BigInteger.ONE),
            Optional.of(new BigInteger("9223372036854775807")),
            Optional.empty(),
            Optional.of(new BigInteger("19223372036854775807")))
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
        .createStatement("SELECT t1 FROM DecimalTable WHERE 1 = ? ORDER BY t2 LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> metadata.getColumnMetadata(0).getJavaType()))
        .as(StepVerifier::create)
        .expectNextMatches(c -> c.equals(BigDecimal.class))
        .verifyComplete();
    connection
        .createStatement("SELECT t1 FROM DecimalTable WHERE 1 = ? ORDER BY t2 LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> metadata.getColumnMetadata(0).getType()))
        .as(StepVerifier::create)
        .expectNextMatches(c -> c.equals(SingleStoreType.DECIMAL))
        .verifyComplete();
  }
}
