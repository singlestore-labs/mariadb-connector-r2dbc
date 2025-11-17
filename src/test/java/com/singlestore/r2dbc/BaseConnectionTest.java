// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Random;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.*;
import org.junit.jupiter.api.function.Executable;
import com.singlestore.r2dbc.api.SingleStoreConnection;
import com.singlestore.r2dbc.api.SingleStoreConnectionMetadata;
import com.singlestore.r2dbc.tools.TcpProxy;
import com.singlestore.r2dbc.util.HostAddress;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class BaseConnectionTest {
  public static final boolean isWindows =
      System.getProperty("os.name").toLowerCase().contains("win");
  public static final Boolean backslashEscape =
      System.getenv("NO_BACKSLASH_ESCAPES") != null
          ? Boolean.valueOf(System.getenv("NO_BACKSLASH_ESCAPES"))
          : false;
  private static final Random rand = new Random();
  public static SingleStoreConnectionFactory factory = TestConfiguration.defaultFactory;
  public static SingleStoreConnection sharedConn;
  public static SingleStoreConnection sharedConnPrepare;
  public static Integer initialConnectionNumber = -1;
  public static TcpProxy proxy;
  private static Instant initialTest;
  private static String maxscaleVersion = null;

  @RegisterExtension public Extension watcher = new BaseConnectionTest.Follow();

  @BeforeAll
  public static void beforeAll() throws Exception {

    sharedConn = factory.create().block();

    SingleStoreConnectionConfiguration confPipeline =
        TestConfiguration.defaultBuilder.clone().useServerPrepStmts(true).build();
    sharedConnPrepare = new SingleStoreConnectionFactory(confPipeline).create().block();
  }

  @AfterAll
  public static void afterEAll() {
    sharedConn.close().block();
    sharedConnPrepare.close().block();
  }

  public static boolean runLongTest() {
    String runLongTest = System.getenv("RUN_LONG_TEST");
    if (runLongTest != null) {
      return Boolean.parseBoolean(runLongTest);
    }
    return false;
  }

  public static void assertThrowsContains(
      Class<? extends Exception> expectedType, Executable executable, String expected) {
    Exception e = Assertions.assertThrows(expectedType, executable);
    Assertions.assertTrue(e.getMessage().contains(expected), "real message:" + e.getMessage());
  }

  public static boolean isXpand() {
    SingleStoreConnectionMetadata meta = sharedConn.getMetadata();
    return meta.getDatabaseVersion().toLowerCase().contains("xpand");
  }

  public static boolean minVersion(int major, int minor, int patch) {
    SingleStoreConnectionMetadata meta = sharedConn.getMetadata();
    return meta.minVersion(major, minor, patch);
  }

  public static boolean isMaxscale() {
    if (maxscaleVersion == null) {
      return "maxscale".equals(System.getenv("srv")) || "maxscale".equals(System.getenv("DB_TYPE"));
    }
    return true;
  }

  public static String getHostSuffix() {
    return "@'%'";
  }

  public static boolean isEnterprise() {
    return "enterprise".equals(System.getenv("DB_TYPE"));
  }

  public static boolean exactVersion(int major, int minor, int patch) {
    SingleStoreConnectionMetadata meta = sharedConn.getMetadata();
    return meta.getMajorVersion() == major
        && meta.getMinorVersion() == minor
        && meta.getPatchVersion() == patch;
  }

  public static Integer maxAllowedPacket() {
    return sharedConnPrepare
        .createStatement("select @@max_allowed_packet")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, Integer.class)))
        .single()
        .block();
  }

  public void assertThrows(
      Class<? extends Exception> expectedType, Executable executable, String expected) {
    Exception e = Assertions.assertThrows(expectedType, executable);
    Assertions.assertTrue(e.getMessage().contains(expected), "real message:" + e.getMessage());
  }

  public SingleStoreConnection createProxyCon() throws Exception {
    HostAddress hostAddress = TestConfiguration.defaultConf.getHostAddresses().get(0);
    try {
      proxy = new TcpProxy(hostAddress.getHost(), hostAddress.getPort());
    } catch (IOException i) {
      throw new Exception("proxy error", i);
    }
    SingleStoreConnectionConfiguration confProxy =
        TestConfiguration.defaultBuilder
            .clone()
            .port(proxy.getLocalPort())
            .host("localhost")
            .build();
    return new SingleStoreConnectionFactory(confProxy).create().block();
  }

  public boolean haveSsl(SingleStoreConnection connection) {
    return connection
        .createStatement("select @@have_ssl")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class)))
        .blockLast()
        .equals("1");
  }

  private class Follow implements BeforeEachCallback, AfterEachCallback {
    @Override
    public void beforeEach(ExtensionContext extensionContext) throws Exception {
      if (!isXpand()) {
        initialConnectionNumber =
            sharedConn
                .createStatement("SHOW STATUS LIKE 'Threads_connected'")
                .execute()
                .flatMap(r -> r.map((row, metadata) -> row.get(1, Integer.class)))
                .blockLast();
      }
      initialTest = Instant.now();
      System.out.println(
          "       test : " + extensionContext.getTestMethod().get().getName() + " begin");
    }

    @AfterEach
    public void afterEach(ExtensionContext extensionContext) throws Exception {
      if (!isXpand()) {
        Integer finalConnectionNumber =
            sharedConn
                .createStatement("SHOW STATUS LIKE 'Threads_connected'")
                .execute()
                .flatMap(r -> r.map((row, metadata) -> row.get(1, Integer.class)))
                .onErrorResume(
                    t -> {
                      t.printStackTrace();
                      return Mono.just(-999999);
                    })
                .blockLast();
        if (finalConnectionNumber != null && finalConnectionNumber - initialConnectionNumber != 0) {
          int retry = 5;
          do {
            Thread.sleep(50);
            finalConnectionNumber =
                sharedConn
                    .createStatement("SHOW STATUS LIKE 'Threads_connected'")
                    .execute()
                    .flatMap(r -> r.map((row, metadata) -> row.get(1, Integer.class)))
                    .onErrorResume(
                        t -> {
                          t.printStackTrace();
                          return Mono.just(-999999);
                        })
                    .blockLast();
          } while (retry-- > 0 && finalConnectionNumber != initialConnectionNumber);

          if (finalConnectionNumber - initialConnectionNumber != 0) {
            System.err.printf(
                "%s: Error connection not ended : changed=%s (initial:%s ended:%s)%n",
                extensionContext.getTestMethod().get(),
                (finalConnectionNumber - initialConnectionNumber),
                initialConnectionNumber,
                finalConnectionNumber);
          }
        }
      }
      System.out.println(
          "       test : "
              + extensionContext.getTestMethod().get().getName()
              + " "
              + Duration.between(initialTest, Instant.now()).toString());

      int j = rand.nextInt();
      sharedConnPrepare
          .createStatement("SELECT " + j + ", 'b'")
          .execute()
          .flatMap(r -> r.map((row, meta) -> row.get(0, Integer.class)))
          .as(StepVerifier::create)
          .expectNext(j)
          .verifyComplete();
    }
  }

  public static void create_seq(SingleStoreConnection conn, String table, int start, int end) {
    conn
        .createStatement(String.format("CREATE TABLE %s(seq INT)", table))
        .execute()
        .blockLast();

    StringBuilder query = new StringBuilder(String.format("INSERT INTO %s VALUES (%d)", table, start));
    for (int i = start + 1; i <= end; i++) {
      query.append(String.format(", (%d)", i));
    }

    conn
        .createStatement(query.toString())
        .execute()
        .blockLast();
  }

  public static byte[] convertInt32ToBson(int value) {
    byte[] bson = new byte[5]; // 4 (value) + 1 (type)
    bson[4] = 0x10; // Type for int32
    // Convert the integer to a byte array (little-endian format)
    bson[0] = (byte) (value); // Byte 1
    bson[1] = (byte) (value >> 8); // Byte 2
    bson[2] = (byte) (value >> 16); // Byte 3
    bson[3] = (byte) (value >> 24); // Byte 4
    return bson;
  }


  public static byte[] convertLocalDateTimeToBson(LocalDateTime localDateTime) {
    byte[] bson = new byte[9]; // 1 (type) + 4 (value) + 3 (length, 8 total)
    bson[8] = 0x09; // Type for timestamp
    Instant instant = localDateTime.toInstant(ZoneOffset.UTC);
    long dateInMillis = instant.toEpochMilli();
    // Convert the integer to a byte array (little-endian format)
    bson[0] = (byte) (dateInMillis);
    bson[1] = (byte) (dateInMillis >> 8);
    bson[2] = (byte) (dateInMillis >> 16);
    bson[3] = (byte) (dateInMillis >> 24);
    bson[4] = (byte) (dateInMillis >> 32);
    bson[5] = (byte) (dateInMillis >> 40);
    bson[6] = (byte) (dateInMillis >> 48);
    bson[7] = (byte) (dateInMillis >> 56);
    return bson;
  }
}
