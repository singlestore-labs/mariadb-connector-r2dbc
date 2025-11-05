// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.integration.parameter;

import com.singlestore.r2dbc.api.SingleStoreStatement;
import io.r2dbc.spi.Blob;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.*;
import com.singlestore.r2dbc.BaseConnectionTest;
import com.singlestore.r2dbc.SingleStoreConnectionConfiguration;
import com.singlestore.r2dbc.SingleStoreConnectionFactory;
import com.singlestore.r2dbc.TestConfiguration;
import com.singlestore.r2dbc.api.SingleStoreConnection;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class BlobParameterTest extends BaseConnectionTest {

  @BeforeAll
  public static void before2() {
    sharedConn
        .createStatement("CREATE TABLE BlobParam (t1 BLOB, t2 BLOB, t3 BLOB)")
        .execute()
        .blockLast();
  }

  @AfterAll
  public static void after2() {
    sharedConn.createStatement("DROP TABLE BlobParam").execute().blockLast();
  }

  @BeforeEach
  public void beforeEach() {
    sharedConn.createStatement("TRUNCATE TABLE BlobParam").execute().blockLast();
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
        .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
        .bindNull(0, byte[].class)
        .bindNull(1, byte[].class)
        .bindNull(2, byte[].class)
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
        .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
        .bind(0, new BigInteger("11"))
        .bind(1, new BigInteger("512"))
        .bind(2, new BigInteger("1"))
        .execute()
        .blockLast();
    validateNotNull(
        ByteBuffer.wrap("11".getBytes()),
        ByteBuffer.wrap("512".getBytes()),
        ByteBuffer.wrap("1".getBytes()));
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
        .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
        .bind(0, "\1")
        .bind(1, "A")
        .bind(2, "ô\0你好\\")
        .execute()
        .blockLast();
    validateNotNull(
        ByteBuffer.wrap("\1".getBytes(StandardCharsets.UTF_8)),
        ByteBuffer.wrap("A".getBytes(StandardCharsets.UTF_8)),
        ByteBuffer.wrap("ô\0你好\\".getBytes(StandardCharsets.UTF_8)));
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
        .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
        .bind(0, new BigDecimal("11"))
        .bind(1, new BigDecimal("512"))
        .bind(2, new BigDecimal("1"))
        .execute()
        .blockLast();
    validateNotNull(
        ByteBuffer.wrap("11".getBytes()),
        ByteBuffer.wrap("512".getBytes()),
        ByteBuffer.wrap("1".getBytes()));
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
        .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
        .bind(0, 11)
        .bind(1, 512)
        .bind(2, 1)
        .execute()
        .blockLast();
    validateNotNull(
        ByteBuffer.wrap("11".getBytes()),
        ByteBuffer.wrap("512".getBytes()),
        ByteBuffer.wrap("1".getBytes()));
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
        .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
        .bind(0, (byte) 15)
        .bind(1, (byte) 127)
        .bind(2, (byte) 0)
        .execute()
        .blockLast();
    validateNotNull(
        ByteBuffer.wrap("15".getBytes()),
        ByteBuffer.wrap("127".getBytes()),
        ByteBuffer.wrap("0".getBytes()));
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
    SingleStoreStatement stmt =
        connection
            .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
            .bind(0, Blob.from(Mono.just(ByteBuffer.wrap(new byte[]{(byte) 15}))))
            .bind(
                1,
                Blob.from(
                    Mono.just(ByteBuffer.wrap(new byte[]{(byte) 1, 0, (byte) 127, (byte) 92}))))
            .bind(2, Blob.from(Mono.just(ByteBuffer.wrap(new byte[]{0}))));
    stmt.execute().blockLast();
    validateNotNull(
        ByteBuffer.wrap(new byte[]{(byte) 15}),
        ByteBuffer.wrap(new byte[]{(byte) 1, 0, (byte) 127, (byte) 92}),
        ByteBuffer.wrap(new byte[]{0}));
  }

  @Test
  void streamValue() {
    streamValue(sharedConn);
  }

  @Test
  void streamValuePrepare() {
    streamValue(sharedConnPrepare);
  }

  private void streamValue(SingleStoreConnection connection) {
    SingleStoreStatement stmt =
        connection
            .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
            .bind(0, new ByteArrayInputStream(new byte[]{(byte) 15}))
            .bind(1, new ByteArrayInputStream(new byte[]{(byte) 1, 0, (byte) 127}))
            .bind(2, new ByteArrayInputStream(new byte[]{0}));
    stmt.execute().blockLast();
    validateNotNull(
        ByteBuffer.wrap(new byte[]{(byte) 15}),
        ByteBuffer.wrap(new byte[]{(byte) 1, 0, (byte) 127}),
        ByteBuffer.wrap(new byte[]{0}));
  }

  @Test
  void byteBufferValue() {
    byteBufferValue(sharedConn);
  }

  @Test
  void byteBufferValuePrepare() {
    byteBufferValue(sharedConnPrepare);
  }

  private void byteBufferValue(SingleStoreConnection connection) {
    SingleStoreStatement stmt =
        connection
            .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
            .bind(0, ByteBuffer.wrap(new byte[]{(byte) 15}))
            .bind(1, ByteBuffer.wrap(new byte[]{(byte) 1, 0, (byte) 127}))
            .bind(2, ByteBuffer.wrap(new byte[]{0}));
    stmt.execute().blockLast();
    validateNotNull(
        ByteBuffer.wrap(new byte[]{(byte) 15}),
        ByteBuffer.wrap(new byte[]{(byte) 1, 0, (byte) 127}),
        ByteBuffer.wrap(new byte[]{0}));
  }

  @Test
  void inputStreamValue() {
    inputStreamValue(sharedConn);
  }

  @Test
  void inputStreamValuePrepare() {
    inputStreamValue(sharedConnPrepare);
  }

  @Test
  void inputStreamValueNoBackslash() throws Exception {
    Map<String, Object> sessionMap = new HashMap<>();
    sessionMap.put("SQL_MODE", "NO_BACKSLASH_ESCAPES");
    SingleStoreConnectionConfiguration confNoBackSlash =
        TestConfiguration.defaultBuilder.clone().sessionVariables(sessionMap).build();
    SingleStoreConnection con = new SingleStoreConnectionFactory(confNoBackSlash).create().block();
    inputStreamValue(con);
    con.close().block();
  }

  @Test
  void inputStreamValueNoBackslashPrepare() throws Exception {
    Map<String, Object> sessionMap = new HashMap<>();
    sessionMap.put("SQL_MODE", "NO_BACKSLASH_ESCAPES");
    SingleStoreConnectionConfiguration confNoBackSlash =
        TestConfiguration.defaultBuilder
            .clone()
            .sessionVariables(sessionMap)
            .useServerPrepStmts(true)
            .build();
    SingleStoreConnection con = new SingleStoreConnectionFactory(confNoBackSlash).create().block();
    inputStreamValue(con);
    con.close().block();
  }

  private void inputStreamValue(SingleStoreConnection connection) {
    SingleStoreStatement stmt =
        connection
            .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
            .bind(0, new ByteArrayInputStream(new byte[]{(byte) 15}))
            .bind(1, new ByteArrayInputStream((new byte[]{(byte) 1, 39, (byte) 127})))
            .bind(2, new ByteArrayInputStream((new byte[]{0})));
    stmt.execute().blockLast();
    validateNotNull(
        ByteBuffer.wrap(new byte[]{(byte) 15}),
        ByteBuffer.wrap(new byte[]{(byte) 1, 39, (byte) 127}),
        ByteBuffer.wrap(new byte[]{0}));
  }

  @Test
  void floatValue() {
    floatValue(sharedConn);
    validateNotNull(
        ByteBuffer.wrap("11.0".getBytes()),
        ByteBuffer.wrap("512.0".getBytes()),
        ByteBuffer.wrap("1.0".getBytes()));
  }

  @Test
  void floatValuePrepare() {
    floatValue(sharedConnPrepare);
    validateNotNull(
        ByteBuffer.wrap("11".getBytes()),
        ByteBuffer.wrap("512".getBytes()),
        ByteBuffer.wrap("1".getBytes()));
  }

  private void floatValue(SingleStoreConnection connection) {
    connection
        .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
        .bind(0, 11f)
        .bind(1, 512f)
        .bind(2, 1f)
        .execute()
        .blockLast();
  }

  @Test
  void doubleValue() {
    doubleValue(sharedConn);
    validateNotNull(
        ByteBuffer.wrap("11.0".getBytes()),
        ByteBuffer.wrap("512.0".getBytes()),
        ByteBuffer.wrap("1.0".getBytes()));
  }

  @Test
  void doubleValuePrepare() {
    doubleValue(sharedConnPrepare);
    validateNotNull(
        ByteBuffer.wrap("11".getBytes()),
        ByteBuffer.wrap("512".getBytes()),
        ByteBuffer.wrap("1".getBytes()));
  }

  private void doubleValue(SingleStoreConnection connection) {
    connection
        .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
        .bind(0, 11d)
        .bind(1, 512d)
        .bind(2, 1d)
        .execute()
        .blockLast();
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
        .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
        .bind(0, Short.valueOf("11"))
        .bind(1, Short.valueOf("127"))
        .bind(2, Short.valueOf("1"))
        .execute()
        .blockLast();
    validateNotNull(
        ByteBuffer.wrap("11".getBytes()),
        ByteBuffer.wrap("127".getBytes()),
        ByteBuffer.wrap("1".getBytes()));
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
        .createStatement("INSERT INTO BlobParam VALUES (?,?,?)")
        .bind(0, 11L)
        .bind(1, 512L)
        .bind(2, 1L)
        .execute()
        .blockLast();
    validateNotNull(
        ByteBuffer.wrap("11".getBytes()),
        ByteBuffer.wrap("512".getBytes()),
        ByteBuffer.wrap("1".getBytes()));
  }

  @Test
  void localDateTimeValue() {
    localDateTimeValue(sharedConn, "2013-07-22 12:50:05.012300", "2025-01-31 10:45:01.123000");
  }

  @Test
  void localDateTimeValuePrepare() {
    localDateTimeValue(
        sharedConnPrepare,
        "2013-07-22 12:50:05.012300",
        "2025-01-31 10:45:01.123000");
  }

  private void localDateTimeValue(SingleStoreConnection connection, String t1, String t3) {
    connection
        .createStatement("INSERT INTO BlobParam VALUES (? :> DATETIME(6),? :> DATETIME(6),? :> DATETIME(6))")
        .bind(0, LocalDateTime.parse("2013-07-22T12:50:05.01230"))
        .bind(1, LocalDateTime.parse("2035-01-31T10:45:01"))
        .bind(2, LocalDateTime.parse("2025-01-31T10:45:01.123"))
        .execute()
        .blockLast();
    validateNotNull(
        ByteBuffer.wrap(t1.getBytes()),
        ByteBuffer.wrap("2035-01-31 10:45:01.000000".getBytes()),
        ByteBuffer.wrap(t3.getBytes()));
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
        .createStatement("INSERT INTO BlobParam VALUES (? :> DATE,? :> DATE,? :> DATE)")
        .bind(0, LocalDate.parse("2010-01-12"))
        .bind(1, LocalDate.parse("2019-01-31"))
        .bind(2, LocalDate.parse("2019-12-31"))
        .execute()
        .blockLast();
    validateNotNull(
        ByteBuffer.wrap("2010-01-12".getBytes()),
        ByteBuffer.wrap("2019-01-31".getBytes()),
        ByteBuffer.wrap("2019-12-31".getBytes()));
  }

  @Test
  void localTimeValue() {
    localTimeValue(sharedConn);
    validateNotNull(
        ByteBuffer.wrap("18:00:00.012340".getBytes()),
        ByteBuffer.wrap("08:00:00.123000".getBytes()),
        ByteBuffer.wrap("08:00:00.123000".getBytes()));
  }

  @Test
  void localTimeValuePrepare() {
    localTimeValue(sharedConnPrepare);
    validateNotNull(
        ByteBuffer.wrap(("18:00:00.012340").getBytes()),
        ByteBuffer.wrap(("08:00:00.123000").getBytes()),
        ByteBuffer.wrap(("08:00:00.123000").getBytes()));
  }

  private void localTimeValue(SingleStoreConnection connection) {
    connection
        .createStatement("INSERT INTO BlobParam VALUES (? :> TIME(6),? :> TIME(6),? :> TIME(6))")
        .bind(0, LocalTime.parse("18:00:00.012340"))
        .bind(1, LocalTime.parse("08:00:00.123"))
        .bind(2, LocalTime.parse("08:00:00.123"))
        .execute()
        .blockLast();
  }

  private void validate(Optional<ByteBuffer> t1, Optional<ByteBuffer> t2, Optional<ByteBuffer> t3) {
    sharedConn
        .createStatement("SELECT * FROM BlobParam")
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) -> {
                      Blob obj0 = (Blob) row.get(0);
                      Blob obj1 = (Blob) row.get(1);
                      Blob obj2 = (Blob) row.get(2);
                      return Flux.just(
                          (obj0 == null)
                              ? Optional.empty()
                              : Optional.of(Mono.from((obj0).stream()).block()),
                          (obj1 == null)
                              ? Optional.empty()
                              : Optional.of(Mono.from((obj1).stream()).block()),
                          (obj2 == null)
                              ? Optional.empty()
                              : Optional.of(Mono.from((obj2).stream()).block()));
                    }))
        .blockLast()
        .as(StepVerifier::create)
        .expectNext(t1, t2, t3)
        .verifyComplete();
  }

  private void validateNotNull(ByteBuffer t1, ByteBuffer t2, ByteBuffer t3) {
    validateNotNull(t1, 0);
    validateNotNull(t2, 1);
    validateNotNull(t3, 2);
  }

  private void validateNotNull(ByteBuffer t1, int index) {
    sharedConn
        .createStatement("SELECT * FROM BlobParam")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(index, Blob.class)))
        .flatMap(Blob::stream)
        .as(StepVerifier::create)
        .consumeNextWith(
            actual -> {
              if (actual.hasArray() && t1.hasArray()) {
                Assertions.assertArrayEquals(actual.array(), t1.array());
              } else {
                byte[] res = new byte[actual.remaining()];
                actual.get(res);

                byte[] exp = new byte[t1.remaining()];
                t1.get(exp);
                Assertions.assertArrayEquals(res, exp);
              }
            })
        .verifyComplete();
  }
}
