// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.integration.parameter;

import com.singlestore.r2dbc.api.SingleStoreStatement;
import io.r2dbc.spi.Clob;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.*;
import com.singlestore.r2dbc.BaseConnectionTest;
import com.singlestore.r2dbc.SingleStoreConnectionConfiguration;
import com.singlestore.r2dbc.SingleStoreConnectionFactory;
import com.singlestore.r2dbc.TestConfiguration;
import com.singlestore.r2dbc.api.SingleStoreConnection;
import com.singlestore.r2dbc.api.SingleStoreConnectionMetadata;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class StringParameterTest extends BaseConnectionTest {
  private static final SingleStoreConnectionMetadata meta = sharedConn.getMetadata();

  @BeforeAll
  public static void before2() {
    sharedConn
        .createStatement(
            "CREATE TABLE StringParam (t1 VARCHAR(256), t2 VARCHAR(256), t3 VARCHAR(256)) DEFAULT"
                + " CHARSET=utf8mb4")
        .execute()
        .blockLast();
  }

  @AfterAll
  public static void after2() {
    sharedConn.createStatement("DROP TABLE StringParam").execute().blockLast();
  }

  @BeforeEach
  public void beforeEach() {
    sharedConn.createStatement("TRUNCATE TABLE StringParam").execute().blockLast();
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
        .createStatement("INSERT INTO StringParam VALUES (?,?,?)")
        .bindNull(0, BigInteger.class)
        .bindNull(1, BigInteger.class)
        .bindNull(2, Short.class)
        .execute()
        .blockLast();
    validate(Optional.empty(), Optional.empty(), Optional.empty());
  }

  @Test
  void bitSetValue() {
    bitSetValue(sharedConn);
  }

  @Test
  void bitSetValuePrepare() {
    bitSetValue(sharedConnPrepare);
  }

  public byte[] revertOrder(byte[] array) {
    int i = 0;
    int j = array.length - 1;
    byte tmp;
    while (j > i) {
      tmp = array[j];
      array[j] = array[i];
      array[i] = tmp;
      j--;
      i++;
    }
    return array;
  }

  private void bitSetValue(SingleStoreConnection connection) {
    SingleStoreStatement stmt =
        connection
            .createStatement("INSERT INTO StringParam VALUES (?,?,?)")
            .bind(0, BitSet.valueOf(revertOrder("çà¤\\".getBytes(StandardCharsets.UTF_8))))
            .bind(1, BitSet.valueOf(revertOrder("你好".getBytes(StandardCharsets.UTF_8))))
            .bind(2, BitSet.valueOf(revertOrder("🌟hello\\".getBytes(StandardCharsets.UTF_8))));
    stmt.execute().blockLast();
    validate(Optional.of("çà¤\\"), Optional.of("你好"), Optional.of("🌟hello\\"));
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

    SingleStoreStatement stmt =
        connection
            .createStatement("INSERT INTO StringParam VALUES (?,?,?)")
            .bind(0, "çà¤\\".getBytes(StandardCharsets.UTF_8))
            .bind(1, "你好".getBytes(StandardCharsets.UTF_8))
            .bind(2, "🌟hello\\".getBytes(StandardCharsets.UTF_8));
    stmt.execute().blockLast();
    validate(Optional.of("çà¤\\"), Optional.of("你好"), Optional.of("🌟hello\\"));
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
        .createStatement("INSERT INTO StringParam VALUES (?,?,?)")
        .bind(0, BigInteger.ONE)
        .bind(1, new BigInteger("32767"))
        .bind(2, new BigInteger("-9"))
        .execute()
        .blockLast();
    validate(Optional.of("1"), Optional.of("32767"), Optional.of("-9"));
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
        .createStatement("INSERT INTO StringParam VALUES (?,?,?)")
        .bind(0, "1")
        .bind(1, "32'767")
        .bind(2, "-9")
        .execute()
        .blockLast();
    validate(Optional.of("1"), Optional.of("32'767"), Optional.of("-9"));
  }

  @Test
  void clobValue() {
    clobValue(sharedConn);
  }

  @Test
  void clobValuePrepare() {
    clobValue(sharedConnPrepare);
  }

  @Test
  void clobValueNoBackslash() throws Exception {
    Map<String, Object> sessionMap = new HashMap<>();
    sessionMap.put("SQL_MODE", "NO_BACKSLASH_ESCAPES");
    SingleStoreConnectionConfiguration confNoBackSlash =
        TestConfiguration.defaultBuilder.clone().sessionVariables(sessionMap).build();
    SingleStoreConnection con = new SingleStoreConnectionFactory(confNoBackSlash).create().block();
    clobValue(con);
    con.close().block();
  }

  @Test
  void clobValuePrepareNoBackslash() throws Exception {
    Map<String, Object> sessionMap = new HashMap<>();
    sessionMap.put("SQL_MODE", "NO_BACKSLASH_ESCAPES");
    SingleStoreConnectionConfiguration confNoBackSlash =
        TestConfiguration.defaultBuilder.clone().sessionVariables(sessionMap).build();
    SingleStoreConnection con = new SingleStoreConnectionFactory(confNoBackSlash).create().block();
    clobValue(con);
    con.close().block();
  }

  private void clobValue(SingleStoreConnection connection) {
    SingleStoreStatement stmt =
        connection
            .createStatement("INSERT INTO StringParam VALUES (?,?,?)")
            .bind(0, Clob.from(Mono.just("123")))
            .bind(1, Clob.from(Mono.just("你好")))
            .bind(2, Clob.from(Mono.just("🌟hello\\")));
    stmt.execute().blockLast();
    validate(Optional.of("123"), Optional.of("你好"), Optional.of("🌟hello\\"));
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
        .createStatement("INSERT INTO StringParam VALUES (?,?,?)")
        .bind(0, BigDecimal.ONE)
        .bind(1, new BigDecimal("32767"))
        .bind(2, new BigDecimal("-9"))
        .execute()
        .blockLast();
    validate(Optional.of("1"), Optional.of("32767"), Optional.of("-9"));
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
        .createStatement("INSERT INTO StringParam VALUES (?,?,?)")
        .bind(0, 1)
        .bind(1, 32767)
        .bind(2, -9)
        .execute()
        .blockLast();
    validate(Optional.of("1"), Optional.of("32767"), Optional.of("-9"));
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
        .createStatement("INSERT INTO StringParam VALUES (?,?,?)")
        .bind(0, (byte) 127)
        .bind(1, (byte) -128)
        .bind(2, (byte) 0)
        .execute()
        .blockLast();
    validate(Optional.of("127"), Optional.of("-128"), Optional.of("0"));
  }

  @Test
  void floatValue() {
    floatValue(sharedConn);
    validate(Optional.of("127.0"), Optional.of("-128.0"), Optional.of("0.0"));
  }

  @Test
  void floatValuePrepare() {
    floatValue(sharedConnPrepare);
    validate(Optional.of("127"), Optional.of("-128"), Optional.of("0"));
  }

  private void floatValue(SingleStoreConnection connection) {
    connection
        .createStatement("INSERT INTO StringParam VALUES (?,?,?)")
        .bind(0, 127f)
        .bind(1, -128f)
        .bind(2, 0f)
        .execute()
        .blockLast();
  }

  @Test
  void doubleValue() {
    doubleValue(sharedConn);
    validate(Optional.of("127.0"), Optional.of("-128.0"), Optional.of("0.0"));
  }

  @Test
  void doubleValuePrepare() {
    doubleValue(sharedConnPrepare);
    validate(Optional.of("127"), Optional.of("-128"), Optional.of("0"));
  }

  private void doubleValue(SingleStoreConnection connection) {
    connection
        .createStatement("INSERT INTO StringParam VALUES (?,?,?)")
        .bind(0, 127d)
        .bind(1, -128d)
        .bind(2, 0d)
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
        .createStatement("INSERT INTO StringParam VALUES (?,?,?)")
        .bind(0, "1")
        .bind(1, "-1")
        .bind(2, "0")
        .execute()
        .blockLast();
    validate(Optional.of("1"), Optional.of("-1"), Optional.of("0"));
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
        .createStatement("INSERT INTO StringParam VALUES (?,?,?)")
        .bind(0, Long.valueOf("1"))
        .bind(1, Long.valueOf("-1"))
        .bind(2, Long.valueOf("0"))
        .execute()
        .blockLast();
    validate(Optional.of("1"), Optional.of("-1"), Optional.of("0"));
  }

  @Test
  void localDateTimeValue() {
    localDateTimeValue(
        sharedConn,
        "2010-01-12 05:08:09.001400",
        "2018-12-15 05:08:10.123456",
        "2025-05-12 05:08:11.123000");
  }

  @Test
  void localDateTimeValuePrepare() {
    localDateTimeValue(
        sharedConnPrepare,
        "2010-01-12 05:08:09.001400",
        "2018-12-15 05:08:10.123456",
        "2025-05-12 05:08:11.123000");
  }

  private void localDateTimeValue(SingleStoreConnection connection, String t1, String t2, String t3) {
    connection
        .createStatement("INSERT INTO StringParam VALUES (?,?,?)")
        .bind(0, LocalDateTime.parse("2010-01-12T05:08:09.0014"))
        .bind(1, LocalDateTime.parse("2018-12-15T05:08:10.123456"))
        .bind(2, LocalDateTime.parse("2025-05-12T05:08:11.123"))
        .execute()
        .blockLast();
    validate(Optional.of(t1), Optional.of(t2), Optional.of(t3));
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
        .createStatement("INSERT INTO StringParam VALUES (? :> DATE,? :> DATE,? :> DATE)")
        .bind(0, LocalDate.parse("2010-01-12"))
        .bind(1, LocalDate.parse("2018-12-15"))
        .bind(2, LocalDate.parse("2025-05-12"))
        .execute()
        .blockLast();
    validate(Optional.of("2010-01-12"), Optional.of("2018-12-15"), Optional.of("2025-05-12"));
  }

  @Test
  void localTimeValue() {
    localTimeValue(sharedConn);
    validate(
        Optional.of("05:08:09.001400"),
        Optional.of("05:08:10.123456"),
        Optional.of("05:08:11.123000"));
  }

  @Test
  void localTimeValuePrepare() {
    localTimeValue(sharedConnPrepare);
    validate(
        Optional.of("05:08:09.001400"),
        Optional.of("05:08:10.123456"),
        Optional.of("05:08:11.123000"));
  }

  private void localTimeValue(SingleStoreConnection connection) {
    connection
        .createStatement("INSERT INTO StringParam VALUES (? :> TIME(6),? :> TIME(6),? :> TIME(6))")
        .bind(0, LocalTime.parse("05:08:09.0014"))
        .bind(1, LocalTime.parse("05:08:10.123456"))
        .bind(2, LocalTime.parse("05:08:11.123"))
        .execute()
        .blockLast();
  }

  @Test
  void durationValue() {
    durationValue(sharedConn);
    validate(Optional.of("90:00:00.012340"), Optional.of("00:08:00.000000"), Optional.of("00:00:22.000000"));
  }

  @Test
  void durationValuePrepare() {
    durationValue(sharedConnPrepare);
    validate(
        Optional.of("90:00:00.012340"),
        Optional.of("00:08:00.000000"),
        Optional.of("00:00:22.000000"));
  }

  private void durationValue(SingleStoreConnection connection) {
    SingleStoreStatement stmt =
        connection
            .createStatement("INSERT INTO StringParam VALUES (? :> TIME(6),? :> TIME(6),? :> TIME(6))")
            .bind(0, Duration.parse("P3DT18H0.012340S"))
            .bind(1, Duration.parse("PT8M"))
            .bind(2, Duration.parse("PT22S"));
    stmt.execute().blockLast();
  }

  private void validate(Optional<String> t1, Optional<String> t2, Optional<String> t3) {
    sharedConn
        .createStatement("SELECT * FROM StringParam")
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) ->
                        Flux.just(
                            Optional.ofNullable((String) row.get(0)),
                            Optional.ofNullable(row.get(1)),
                            Optional.ofNullable(row.get(2)))))
        .blockLast()
        .as(StepVerifier::create)
        .expectNext(t1, t2, t3)
        .verifyComplete();
  }
}
