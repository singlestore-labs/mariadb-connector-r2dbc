// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.integration.parameter;

import com.singlestore.r2dbc.api.SingleStoreStatement;
import io.r2dbc.spi.Blob;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.Optional;
import org.junit.jupiter.api.*;
import com.singlestore.r2dbc.BaseConnectionTest;
import com.singlestore.r2dbc.api.SingleStoreConnection;
import com.singlestore.r2dbc.api.SingleStoreResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class BitParameterTest extends BaseConnectionTest {
  @BeforeAll
  public static void before2() {
    sharedConn
        .createStatement("CREATE TABLE ByteParam (t1 BIT(4), t2 BIT(20), t3 BIT(2))")
        .execute()
        .blockLast();
  }

  @AfterAll
  public static void afterAll2() {
    sharedConn.createStatement("DROP TABLE ByteParam").execute().blockLast();
  }

  @BeforeEach
  public void beforeEach() {
    sharedConn.createStatement("TRUNCATE TABLE ByteParam").execute().blockLast();
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
        .createStatement("INSERT INTO ByteParam VALUES (?,?,?)")
        .bindNull(0, Byte.class)
        .bindNull(1, Byte.class)
        .bindNull(2, Byte.class)
        .execute()
        .blockLast();
    validate(Optional.empty(), Optional.empty(), Optional.empty());
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
    SingleStoreStatement stmt =
        connection
            .createStatement("INSERT INTO ByteParam VALUES (?,?,?)")
            .bind(0, Boolean.TRUE)
            .bind(1, Boolean.TRUE)
            .bind(2, Boolean.FALSE);

    Assertions.assertTrue(
        stmt.toString()
            .contains(
                "bindings=[[BindValue{codec=BooleanCodec}, BindValue{codec=BooleanCodec},"
                    + " BindValue{codec=BooleanCodec}]]"),
        stmt.toString());
    stmt.execute().blockLast();
    validate(
        Optional.of(BitSet.valueOf(new byte[] {(byte) 1})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 1})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 0})));
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
        .createStatement("INSERT INTO ByteParam VALUES (?,?,?)")
        .bind(0, new BigInteger("11"))
        .bind(1, new BigInteger("512"))
        .bind(2, new BigInteger("1"))
        .execute()
        .blockLast();
    validate(
        Optional.of(BitSet.valueOf(new byte[] {(byte) 11})),
        Optional.of(BitSet.valueOf(new byte[] {0, (byte) 2, 0})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 1})));
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
        .createStatement("INSERT INTO ByteParam VALUES (?,?,?)")
        .bind(0, "\1")
        .bind(1, "A")
        .bind(2, "\0")
        .execute()
        .blockLast();
    validate(
        Optional.of(BitSet.valueOf(new byte[] {(byte) 1})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 65, 0, 0})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 0})));
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
        .createStatement("INSERT INTO ByteParam VALUES (?,?,?)")
        .bind(0, new BigDecimal("11"))
        .bind(1, new BigDecimal("512"))
        .bind(2, new BigDecimal("1"))
        .execute()
        .blockLast();
    validate(
        Optional.of(BitSet.valueOf(new byte[] {(byte) 11})),
        Optional.of(BitSet.valueOf(new byte[] {0, (byte) 2, 0})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 1})));
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
        .createStatement("INSERT INTO ByteParam VALUES (?,?,?)")
        .bind(0, 11)
        .bind(1, 512)
        .bind(2, 1)
        .execute()
        .blockLast();
    validate(
        Optional.of(BitSet.valueOf(new byte[] {(byte) 11})),
        Optional.of(BitSet.valueOf(new byte[] {0, (byte) 2, 0})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 1})));
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
        .createStatement("INSERT INTO ByteParam VALUES (?,?,?)")
        .bind(0, (byte) 15)
        .bind(1, (byte) 127)
        .bind(2, (byte) 0)
        .execute()
        .blockLast();
    validate(
        Optional.of(BitSet.valueOf(new byte[] {(byte) 15})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 127, 0, 0})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 0})));
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
        .createStatement("INSERT INTO ByteParam VALUES (?,?,?)")
        .bind(0, Blob.from(Mono.just(ByteBuffer.wrap(new byte[] {(byte) 15}))))
        .bind(1, Blob.from(Mono.just(ByteBuffer.wrap(new byte[] {(byte) 1, 2}))))
        .bind(2, Blob.from(Mono.just(ByteBuffer.wrap(new byte[] {0}))))
        .execute()
        .blockLast();

    validate(
        Optional.of(BitSet.valueOf(new byte[] {(byte) 15})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 2, (byte) 1})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 0})));

    sharedConn.createStatement("TRUNCATE TABLE ByteParam").execute().blockLast();

    connection
        .createStatement("INSERT INTO ByteParam VALUES (?,?,?)")
        .bind(0, Blob.from(Mono.just(ByteBuffer.wrap(new byte[] {(byte) 15}))))
        .bind(1, Blob.from(Mono.just(ByteBuffer.wrap(new byte[] {(byte) 1, 2}))))
        .bind(2, Blob.from(Mono.just(ByteBuffer.wrap(new byte[] {0}))))
        .execute()
        .blockLast();

    validate(
        Optional.of(BitSet.valueOf(new byte[] {(byte) 15})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 2, (byte) 1})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 0})));
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
        .createStatement("INSERT INTO ByteParam VALUES (? :> BIT(4), ? :> BIT(20), ? :> BIT(2))")
        .bind(0, 11f)
        .bind(1, 512f)
        .bind(2, 1f)
        .execute()
        .blockLast();
    validate(
        Optional.of(BitSet.valueOf(new byte[] {(byte) 11})),
        Optional.of(BitSet.valueOf(new byte[] {0, (byte) 2, 0})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 1})));
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
        .createStatement("INSERT INTO ByteParam VALUES (? :> BIT(4), ? :> BIT(20), ? :> BIT(2))")
        .bind(0, 11d)
        .bind(1, 512d)
        .bind(2, 1d)
        .execute()
        .blockLast();
    validate(
        Optional.of(BitSet.valueOf(new byte[] {(byte) 11})),
        Optional.of(BitSet.valueOf(new byte[] {0, (byte) 2, 0})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 1})));
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
        .createStatement("INSERT INTO ByteParam VALUES (?,?,?)")
        .bind(0, Short.valueOf("11"))
        .bind(1, Short.valueOf("127"))
        .bind(2, Short.valueOf("1"))
        .execute()
        .blockLast();
    validate(
        Optional.of(BitSet.valueOf(new byte[] {(byte) 11})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 127, 0, 0})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 1})));
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
        .createStatement("INSERT INTO ByteParam VALUES (?,?,?)")
        .bind(0, 11L)
        .bind(1, 512L)
        .bind(2, 1L)
        .execute()
        .blockLast();
    validate(
        Optional.of(BitSet.valueOf(new byte[] {(byte) 11})),
        Optional.of(BitSet.valueOf(new byte[] {0, (byte) 2, 0})),
        Optional.of(BitSet.valueOf(new byte[] {(byte) 1})));
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
    Flux<SingleStoreResult> f =
        connection
            .createStatement("INSERT INTO ByteParam VALUES (?,?,?)")
            .bind(0, LocalDateTime.now())
            .bind(1, LocalDateTime.now())
            .bind(2, LocalDateTime.now())
            .execute();
    f.blockLast();
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
    Flux<SingleStoreResult> f =
        connection
            .createStatement("INSERT INTO ByteParam VALUES (?,?,?)")
            .bind(0, LocalDate.now())
            .bind(1, LocalDate.now())
            .bind(2, LocalDate.now())
            .execute();
    f.blockLast();
  }

  @Test
  void localTimeValue() {
    localTimeValue(sharedConn);
  }

  @Test
  void localTimeValuePrepare() {
    localTimeValue(sharedConnPrepare);
  }

  private void localTimeValue(SingleStoreConnection connection) {
    Flux<SingleStoreResult> f =
        connection
            .createStatement("INSERT INTO ByteParam VALUES (?,?,?)")
            .bind(0, LocalTime.now())
            .bind(1, LocalTime.now())
            .bind(2, LocalTime.now())
            .execute();
    f.blockLast();
  }

  private void validate(Optional<BitSet> t1, Optional<BitSet> t2, Optional<BitSet> t3) {
    sharedConn
        .createStatement("SELECT * FROM ByteParam")
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) -> {
                      BitSet obj0 = (BitSet) row.get(0);
                      BitSet obj1 = (BitSet) row.get(1);
                      BitSet obj2 = (BitSet) row.get(2);
                      return Flux.just(
                          Optional.ofNullable(obj0),
                          Optional.ofNullable(obj1),
                          Optional.ofNullable(obj2));
                    }))
        .blockLast()
        .as(StepVerifier::create)
        .expectNext(t1, t2, t3)
        .verifyComplete();
  }
}
