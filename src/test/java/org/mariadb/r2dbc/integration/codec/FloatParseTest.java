// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration.codec;

import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcTransientResourceException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.util.MariadbType;
import reactor.test.StepVerifier;

public class FloatParseTest extends BaseConnectionTest {
  @BeforeAll
  public static void before2() {
    afterAll2();
    sharedConn.beginTransaction().block();
    sharedConn.createStatement("CREATE TABLE FloatTable (t1 FLOAT, t2 INT)").execute().blockLast();
    sharedConn
        .createStatement("INSERT INTO FloatTable VALUES (0.1, 1),(1, 2),(922.92233, 3), (null, 4)")
        .execute()
        .blockLast();
    sharedConn.createStatement("FLUSH TABLES").execute().blockLast();
    sharedConn.commitTransaction().block();
  }

  @AfterAll
  public static void afterAll2() {
    sharedConn.createStatement("DROP TABLE IF EXISTS FloatTable").execute().blockLast();
  }

  @Test
  void wrongType() {
    sharedConn
        .createStatement("SELECT t1 FROM FloatTable WHERE 1 = ? ORDER BY t2")
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
        .createStatement("SELECT t1 FROM FloatTable WHERE 1 = ? ORDER BY t2")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNextMatches(
            val -> {
              Assertions.assertEquals(0.1F, (Float) val.get(), 0.0000001);
              return true;
            })
        .expectNextMatches(
            val -> {
              Assertions.assertEquals(1F, (Float) val.get(), 0.0000001);
              return true;
            })
        .expectNextMatches(
            val -> {
              Assertions.assertEquals(922.922F, (Float) val.get(), 0.001);
              return true;
            })
        .expectNext(Optional.empty())
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
        .createStatement("SELECT t1 FROM FloatTable WHERE 1 = ? ORDER BY t2")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Boolean.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(false), Optional.of(true), Optional.of(true), Optional.empty())
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
        .createStatement("SELECT t1 FROM FloatTable WHERE 1 = ? ORDER BY t2 LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, byte[].class)))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable
                        .getMessage()
                        .equals("No decoder for type byte[] and column type FLOAT"))
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
        .createStatement("SELECT t1 FROM FloatTable WHERE 1 = ? ORDER BY t2 LIMIT 3")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Byte.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of((byte) 0), Optional.of((byte) 1))
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && (throwable
                            .getMessage()
                            .equals("value '922.922' (FLOAT) cannot be decoded as Byte")
                        | throwable
                            .getMessage()
                            .equals("value '922.9223' (FLOAT) cannot be decoded as Byte")))
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
        .createStatement("SELECT t1 FROM FloatTable WHERE 1 = ? ORDER BY t2 LIMIT 3")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, byte.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of((byte) 0), Optional.of((byte) 1))
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && (throwable
                            .getMessage()
                            .equals("value '922.922' (FLOAT) cannot be decoded as Byte")
                        | throwable
                            .getMessage()
                            .equals("value '922.9223' (FLOAT) cannot be decoded as Byte")))
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
        .createStatement("SELECT t1 FROM FloatTable WHERE 1 = ? ORDER BY t2")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Short.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of((short) 0),
            Optional.of((short) 1),
            Optional.of((short) 922),
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
        .createStatement("SELECT t1 FROM FloatTable WHERE 1 = ? ORDER BY t2")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Integer.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(0), Optional.of(1), Optional.of(922), Optional.empty())
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
        .createStatement("SELECT t1 FROM FloatTable WHERE 1 = ? ORDER BY t2")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Long.class))))
        .as(StepVerifier::create)
        .expectNext(Optional.of(0L), Optional.of(1L), Optional.of(922L), Optional.empty())
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
        .createStatement("SELECT t1 FROM FloatTable WHERE 1 = ? ORDER BY t2")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, float.class))))
        .as(StepVerifier::create)
        .expectNextMatches(
            val -> {
              Assertions.assertEquals(0.1F, val.get(), 0.0000001);
              return true;
            })
        .expectNextMatches(
            val -> {
              Assertions.assertEquals(1F, val.get(), 0.0000001);
              return true;
            })
        .expectNextMatches(
            val -> {
              Assertions.assertEquals(922.922F, val.get(), 0.001);
              return true;
            })
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTransientResourceException
                    && throwable.getMessage().equals("Cannot return null for primitive float"))
        .verify();
  }

  @Test
  void floatObjectValue() {
    floatObjectValue(sharedConn);
  }

  @Test
  void floatObjectValuePrepare() {
    floatObjectValue(sharedConnPrepare);
  }

  private void floatObjectValue(MariadbConnection connection) {
    connection
        .createStatement("SELECT t1 FROM FloatTable WHERE 1 = ? ORDER BY t2")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Float.class))))
        .as(StepVerifier::create)
        .expectNextMatches(
            val -> {
              Assertions.assertEquals(0.1F, val.get(), 0.0000001);
              return true;
            })
        .expectNextMatches(
            val -> {
              Assertions.assertEquals(1F, val.get(), 0.0000001);
              return true;
            })
        .expectNextMatches(
            val -> {
              Assertions.assertEquals(922.922F, val.get(), 0.001);
              return true;
            })
        .expectNext(Optional.empty())
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
        .createStatement("SELECT t1 FROM FloatTable WHERE 1 = ? ORDER BY t2")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, Double.class))))
        .as(StepVerifier::create)
        .expectNextMatches(
            val -> {
              Assertions.assertEquals(0.1D, val.get(), 0.0000001);
              return true;
            })
        .expectNextMatches(
            val -> {
              Assertions.assertEquals(1D, val.get(), 0.0000001);
              return true;
            })
        .expectNextMatches(
            val -> {
              Assertions.assertEquals(922.922D, val.get(), 0.001);
              return true;
            })
        .expectNext(Optional.empty())
        .verifyComplete();
  }

  @Test
  void stringValue() {
    stringValue(sharedConn, Optional.of("0.1"), Optional.of("1"), Optional.of("922.922"));
  }

  @Test
  void stringValuePrepare() {
    stringValue(sharedConnPrepare, Optional.of("0.1"), Optional.of("1.0"), Optional.of("922.9223"));
  }

  private void stringValue(
      MariadbConnection connection, Optional<String> t1, Optional<String> t2, Optional<String> t3) {
    connection
        .createStatement("SELECT t1 FROM FloatTable WHERE 1 = ? ORDER BY t2")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, String.class))))
        .as(StepVerifier::create)
        .expectNext(t1, t2, t3, Optional.empty())
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
        .createStatement("SELECT t1 FROM FloatTable WHERE 1 = ? ORDER BY t2")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, BigDecimal.class))))
        .as(StepVerifier::create)
        .expectNextMatches(
            val -> {
              Assertions.assertEquals(0.1F, val.get().floatValue(), 0.0000001);
              return true;
            })
        .expectNextMatches(
            val -> {
              Assertions.assertEquals(1F, val.get().floatValue(), 0.0000001);
              return true;
            })
        .expectNextMatches(
            val -> {
              Assertions.assertEquals(922.922F, val.get().floatValue(), 0.001);
              return true;
            })
        .expectNext(Optional.empty())
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
        .createStatement("SELECT t1 FROM FloatTable WHERE 1 = ? ORDER BY t2")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, BigInteger.class))))
        .as(StepVerifier::create)
        .expectNext(
            Optional.of(BigInteger.ZERO),
            Optional.of(BigInteger.ONE),
            Optional.of(BigInteger.valueOf(922)),
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
        .createStatement("SELECT t1 FROM FloatTable WHERE 1 = ? ORDER BY t2 LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> metadata.getColumnMetadata(0).getJavaType()))
        .as(StepVerifier::create)
        .expectNextMatches(c -> c.equals(Float.class))
        .verifyComplete();
    connection
        .createStatement("SELECT t1 FROM FloatTable WHERE 1 = ? ORDER BY t2 LIMIT 1")
        .bind(0, 1)
        .execute()
        .flatMap(r -> r.map((row, metadata) -> metadata.getColumnMetadata(0).getType()))
        .as(StepVerifier::create)
        .expectNextMatches(c -> c.equals(MariadbType.FLOAT))
        .verifyComplete();
  }
}
