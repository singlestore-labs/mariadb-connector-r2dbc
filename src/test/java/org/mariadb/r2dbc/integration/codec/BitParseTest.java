// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration.codec;

import io.r2dbc.spi.R2dbcTransientResourceException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.BitSet;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.MariadbConnectionFactory;
import org.mariadb.r2dbc.TestConfiguration;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.util.MariadbType;
import reactor.test.StepVerifier;

public class BitParseTest extends BaseConnectionTest {
  @BeforeAll
  public static void before2() {
    afterAll2();
    sharedConn.beginTransaction().block();
    sharedConn
        .createStatement("CREATE TABLE BitTable (t1 BIT(16), t2 int, t3 BIT(3), t4 INT)")
        .execute()
        .blockLast();
    sharedConn.createStatement("CREATE TABLE BitTable2 (t1 BIT(1), t4 INT)").execute().blockLast();
    sharedConn
        .createStatement(
            "INSERT INTO BitTable VALUES (b'0000', 1, b'0', 1), (b'0000000100000000', 2,"
                + " b'1', 2),(b'0000111100000000', 3, b'10', 3),(b'1010', 4, b'11', 4), (null, 5, b'100', 5)")
        .execute()
        .blockLast();
    sharedConn
        .createStatement("INSERT INTO BitTable2 VALUES (b'0', 1), (true, 2), (null, 3)")
        .execute()
        .blockLast();
    sharedConn.createStatement("FLUSH TABLES").execute().blockLast();
    sharedConn.commitTransaction().block();
  }

  @AfterAll
  public static void afterAll2() {
    sharedConn.createStatement("DROP TABLE IF EXISTS BitTable").execute().blockLast();
    sharedConn.createStatement("DROP TABLE IF EXISTS BitTable2").execute().blockLast();
  }

  @Test
  void defaultValue() {
    defaultValue(sharedConn);
  }

  @Test
  void defaultValuePrepare() {
    defaultValue(sharedConnPrepare);
  }

  @Test
  void defaultValueNoTiny() throws Throwable {
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder.clone().tinyInt1isBit(false).build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    try {
      connection
          .createStatement("SELECT t1 FROM BitTable2 WHERE 1 = ? ORDER BY t4")
          .bind(0, 1)
          .execute()
          .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
          .as(StepVerifier::create)
          .expectNext(
              Optional.of(BitSet.valueOf(new byte[] {(byte) 0})),
              Optional.of(BitSet.valueOf(new byte[] {(byte) 1})),
              Optional.empty())
          .verifyComplete();
    } finally {
      connection.close().block();
    }
  }

  private void defaultValue(MariadbConnection connection) {
    connection
        .createStatement("SELECT t1, t2 FROM BitTable WHERE 1 = ? ORDER BY t4")
        .bind(0, 1)
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) -> {
                      row.get(0);
                      row.get(1);
                      row.get(1);
                      return Optional.ofNullable(row.get(0));
                    }))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(BitSet.valueOf(new byte[] {(byte) 0, (byte) 0})),
            Optional.of(BitSet.valueOf(new byte[] {(byte) 0, (byte) 1})),
            Optional.of(BitSet.valueOf(new byte[] {(byte) 0, (byte) 15})),
            Optional.of(BitSet.valueOf(new byte[] {(byte) 10})),
            Optional.empty())
        .verifyComplete();
    connection
        .createStatement("SELECT t1 FROM BitTable2 WHERE 1 = ? ORDER BY t4")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(Boolean.FALSE), Optional.of(Boolean.TRUE), Optional.empty())
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
        .createStatement("SELECT t1 FROM BitTable WHERE 1 = ? ORDER BY t4")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, boolean.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(false), Optional.of(true), Optional.of(true), Optional.of(true))
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable.getMessage().equals("Cannot return null for primitive boolean"))
        .verify();

    connection
        .createStatement("SELECT t3 FROM BitTable WHERE 1 = ? ORDER BY t4 LIMIT 4")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, boolean.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(false), Optional.of(true), Optional.of(true), Optional.of(true))
        .verifyComplete();
  }

  @Test
  void booleanObjectValue() {
    booleanObjectValue(sharedConn);
  }

  @Test
  void booleanObjectValuePrepare() {
    booleanObjectValue(sharedConnPrepare);
  }

  private void booleanObjectValue(MariadbConnection connection) {
    connection
        .createStatement("SELECT t1 FROM BitTable WHERE 1 = ? ORDER BY t4")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Boolean.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(false),
            Optional.of(true),
            Optional.of(true),
            Optional.of(true),
            Optional.empty())
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

  private void byteArrayValue(MariadbConnection connection) {
    connection
        .createStatement("SELECT t1 FROM BitTable WHERE 1 = ? ORDER BY t4 LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, byte[].class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type byte[] and column type BIT"))
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
        .createStatement("SELECT t1 FROM BitTable WHERE 1 = ? ORDER BY t4")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Byte.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of((byte) 0),
            Optional.of((byte) 1),
            Optional.of((byte) 15),
            Optional.of((byte) 10),
            Optional.empty())
        .verifyComplete();

    connection
        .createStatement("SELECT t3 FROM BitTable WHERE 1 = ? ORDER BY t4")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Byte.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of((byte) 0),
            Optional.of((byte) 1),
            Optional.of((byte) 2),
            Optional.of((byte) 3),
            Optional.of((byte) 4))
        .verifyComplete();
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
        .createStatement("SELECT t1 FROM BitTable WHERE 1 = ? ORDER BY t4 LIMIT 4")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, byte.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of((byte) 0),
            Optional.of((byte) 1),
            Optional.of((byte) 15),
            Optional.of((byte) 10))
        .verifyComplete();
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
        .createStatement("SELECT t1 FROM BitTable WHERE 1 = ? ORDER BY t4")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Short.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of((short) 0),
            Optional.of((short) 256),
            Optional.of((short) 3840),
            Optional.of((short) 10),
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
        .createStatement("SELECT t1 FROM BitTable WHERE 1 = ? ORDER BY t4")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Integer.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(0), Optional.of(256), Optional.of(3840), Optional.of(10), Optional.empty())
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
        .createStatement("SELECT t1 FROM BitTable WHERE 1 = ? ORDER BY t4")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Long.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(0L),
            Optional.of(256L),
            Optional.of(3840L),
            Optional.of(10L),
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
        .createStatement("SELECT t1 FROM BitTable WHERE 1 = ? ORDER BY t4 LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Float.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.lang.Float and column type BIT"))
        .verify();
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
        .createStatement("SELECT t1 FROM BitTable WHERE 1 = ? ORDER BY t4 LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Double.class))))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type java.lang.Double and column type BIT"))
        .verify();
  }

  @Test
  void wrongType() {
    sharedConn
        .createStatement("SELECT t1 FROM BitTable WHERE 1 = ? ORDER BY t4")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, this.getClass()))))
        .as(StepVerifier::create)
        .expectError();
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
        .createStatement("SELECT t1 FROM BitTable WHERE 1 = ? ORDER BY t4")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, String.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of("b''"),
            Optional.of("b'100000000'"),
            Optional.of("b'111100000000'"),
            Optional.of("b'1010'"),
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
        .createStatement("SELECT t1 FROM BitTable WHERE 1 = ? ORDER BY t4")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, BigDecimal.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(BigDecimal.valueOf(0L)),
            Optional.of(BigDecimal.valueOf(256L)),
            Optional.of(BigDecimal.valueOf(3840L)),
            Optional.of(BigDecimal.valueOf(10L)),
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
        .createStatement("SELECT t1 FROM BitTable WHERE 1 = ? ORDER BY t4")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, BigInteger.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(BigInteger.valueOf(0L)),
            Optional.of(BigInteger.valueOf(256L)),
            Optional.of(BigInteger.valueOf(3840L)),
            Optional.of(BigInteger.valueOf(10L)),
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
        .createStatement("SELECT t1 FROM BitTable WHERE 1 = ? ORDER BY t4 LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> metadata.getColumnMetadata(0).getJavaType()))
        .as(StepVerifier::create)
        .expectNextMatches(c -> c.equals(BitSet.class))
        .verifyComplete();
    connection
        .createStatement("SELECT t1 FROM BitTable WHERE 1 = ? ORDER BY t4 LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> metadata.getColumnMetadata(0).getType()))
        .as(StepVerifier::create)
        .expectNextMatches(c -> c.equals(MariadbType.BIT))
        .verifyComplete();
    connection
        .createStatement("SELECT t1 FROM BitTable2 WHERE 1 = ? ORDER BY t4 LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> metadata.getColumnMetadata(0).getType()))
        .as(StepVerifier::create)
        .expectNextMatches(c -> c.equals(MariadbType.BOOLEAN))
        .verifyComplete();
  }
}
