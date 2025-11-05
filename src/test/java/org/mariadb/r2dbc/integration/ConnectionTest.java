// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package org.mariadb.r2dbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import io.r2dbc.spi.*;
import java.math.BigInteger;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;
import org.mariadb.r2dbc.*;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.api.MariadbStatement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;
import reactor.test.StepVerifier;

public class ConnectionTest extends BaseConnectionTest {

  @BeforeAll
  public static void before2() {
    dropAll();
    sharedConn.createStatement("CREATE DATABASE test_r2dbc").execute().blockLast();
  }

  @AfterAll
  public static void dropAll() {
    sharedConn.createStatement("DROP DATABASE IF EXISTS test_r2dbc").execute().blockLast();
  }

  @Test
  void localValidation() {
    sharedConn
        .validate(ValidationDepth.LOCAL)
        .as(StepVerifier::create)
        .expectNext(Boolean.TRUE)
        .verifyComplete();
  }

  @Test
  void localValidationClosedConnection() {
    MariadbConnection connection = factory.create().block();
    connection.close().block();
    connection
        .validate(ValidationDepth.LOCAL)
        .as(StepVerifier::create)
        .expectNext(Boolean.FALSE)
        .verifyComplete();
  }

  @Test
  void connectionError() throws Exception {
    Assumptions.assumeTrue(
        !isMaxscale()
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));

    // disableLog();
    MariadbConnection connection = createProxyCon();
    proxy.forceClose();
    connection
        .setAutoCommit(false)
        .as(StepVerifier::create)
        .verifyErrorSatisfies(
            t -> {
              assertTrue(t instanceof R2dbcNonTransientResourceException);
              assertTrue(
                  t.getMessage().contains("The connection is closed. Unable to send anything")
                      || t.getMessage()
                          .contains("Cannot execute command since connection is already closed")
                      || t.getMessage().contains("Connection error")
                      || t.getMessage().contains("Connection closed")
                      || t.getMessage().contains("Connection unexpectedly closed")
                      || t.getMessage().contains("Connection unexpected error"));
            });
    Thread.sleep(100);
  }

  @Test
  void validate() {
    MariadbConnection connection = factory.create().block();
    assertTrue(connection.validate(ValidationDepth.LOCAL).block());
    assertTrue(connection.validate(ValidationDepth.REMOTE).block());
    connection.close().block();
    assertFalse(connection.validate(ValidationDepth.LOCAL).block());
    assertFalse(connection.validate(ValidationDepth.REMOTE).block());
  }

  @Test
  void connectionWithoutErrorOnClose() throws Exception {
    Assumptions.assumeTrue(System.getenv("local") == null || "1".equals(System.getenv("local")));

    Assumptions.assumeTrue(
        !isMaxscale()
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));
    MariadbConnection connection = createProxyCon();
    proxy.stop();
    connection.close().block();
  }

  //    @Test
  //    void perf() {
  //      for (int ii = 0; ii < 1000000; ii++) {
  //        io.r2dbc.spi.Statement statement = sharedConn.createStatement(sql);
  //        for (int i = 0; i < 1000; i++) statement.bind(i, i);
  //
  //        Flux.from(statement.execute()).flatMap(it -> it.getRowsUpdated()).blockLast();
  //      }
  //    }

  //  @Test
  //  void perf() {
  //    for (int ii = 0; ii < 1000000; ii++) {
  //      io.r2dbc.spi.Statement statement = sharedConn.createStatement("DO 1");
  //      Flux.from(statement.execute()).flatMap(it -> it.getRowsUpdated()).blockLast();
  //    }
  //  }

  @Test
  @Timeout(5)
  void connectionDuringError() throws Exception {
    Assumptions.assumeTrue(System.getenv("local") == null || "1".equals(System.getenv("local")));

    Assumptions.assumeTrue(!isMaxscale() && !isEnterprise());
    MariadbConnection connection = createProxyCon();
    new Timer()
        .schedule(
            new TimerTask() {
              @Override
              public void run() {
                proxy.forceClose();
              }
            },
            200);

    connection
        .createStatement(
            "select * from information_schema.columns as c1, "
                + "information_schema.tables, information_schema.tables as t2")
        .execute()
        .flatMap(r -> r.map((rows, meta) -> ""))
        .onErrorComplete()
        .blockLast();
    connection
        .validate(ValidationDepth.REMOTE)
        .as(StepVerifier::create)
        .expectNext(Boolean.FALSE)
        .verifyComplete();
    Thread.sleep(100);
  }

  @Test
  void remoteValidation() {
    sharedConn
        .validate(ValidationDepth.REMOTE)
        .as(StepVerifier::create)
        .expectNext(Boolean.TRUE)
        .verifyComplete();
  }

  @Test
  void remoteValidationClosedConnection() {
    MariadbConnection connection = factory.create().block();
    assert connection != null;
    connection.close().block();
    connection
        .validate(ValidationDepth.REMOTE)
        .as(StepVerifier::create)
        .expectNext(Boolean.FALSE)
        .verifyComplete();
  }

  @Test
  void multipleConnection() {
    for (int i = 0; i < 50; i++) {
      MariadbConnection connection = factory.create().block();
      connection
          .validate(ValidationDepth.REMOTE)
          .as(StepVerifier::create)
          .expectNext(Boolean.TRUE)
          .verifyComplete();
      connection.close().block();
    }
  }

  @Test
  void connectTimeout() throws Exception {
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder.clone().connectTimeout(Duration.ofSeconds(1)).build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    consume(connection);
    connection.close().block();
  }

  @Test
  void timeoutMultiHost() throws Exception {
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .host(
                "128.2.2.2,"
                    + TestConfiguration.defaultBuilder
                        .clone()
                        .build()
                        .getHostAddresses()
                        .get(0)
                        .getHost())
            .connectTimeout(Duration.ofMillis(500))
            .build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    consume(connection);
    connection.close().block();
  }

  @Test
  public void localSocket() {
    Assumptions.assumeTrue(
        System.getenv("local") != null
            && "1".equals(System.getenv("local"))
            && !System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win"));
    String socket =
        sharedConn
            .createStatement("select @@socket")
            .execute()
            .flatMap(r -> r.map((row, metadata) -> row.get(0, String.class)))
            .blockLast();
    sharedConn.createStatement("DROP USER IF EXISTS testSocket@'localhost'").execute().blockLast();
    sharedConn
        .createStatement("CREATE USER testSocket@'localhost' IDENTIFIED BY 'MySup5%rPassw@ord'")
        .execute()
        .blockLast();
    sharedConn
        .createStatement(
            "GRANT SELECT on *.* to testSocket@'localhost' IDENTIFIED BY 'MySup5%rPassw@ord'")
        .execute()
        .blockLast();
    sharedConn.createStatement("FLUSH PRIVILEGES").execute().blockLast();

    MariadbConnectionConfiguration conf =
        MariadbConnectionConfiguration.builder()
            .username("testSocket")
            .password("MySup5%rPassw@ord")
            .database(TestConfiguration.database)
            .socket(socket)
            .build();

    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    consume(connection);
    assert connection != null;
    connection.close().block();

    sharedConn.createStatement("DROP USER testSocket@'localhost'").execute().blockLast();
  }

  @Test
  void socketTcpKeepAlive() throws Exception {
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder.clone().tcpKeepAlive(Boolean.TRUE).build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    consume(connection);
    assert connection != null;
    connection.close().block();
  }

  @Test
  void socketTcpAbortiveClose() throws Exception {
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder.clone().tcpAbortiveClose(Boolean.TRUE).build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    consume(connection);
    assert connection != null;
    connection.close().block();
  }

  @Test
  void basicConnectionWithoutPipeline() throws Exception {
    MariadbConnectionConfiguration noPipeline =
        TestConfiguration.defaultBuilder
            .clone()
            .useServerPrepStmts(true)
            .prepareCacheSize(1)
            .build();
    MariadbConnection connection = new MariadbConnectionFactory(noPipeline).create().block();
    connection
        .createStatement("SELECT 5")
        .execute()
        .flatMap(r -> r.map((row, meta) -> row.get(0, Integer.class)))
        .as(StepVerifier::create)
        .expectNext(5)
        .verifyComplete();
    connection
        .createStatement("SELECT 6")
        .execute()
        .flatMap(r -> r.map((row, meta) -> row.get(0, Integer.class)))
        .as(StepVerifier::create)
        .expectNext(6)
        .verifyComplete();
    connection
        .createStatement("SELECT ?")
        .bind(0, 7)
        .execute()
        .flatMap(r -> r.map((row, meta) -> row.get(0, Integer.class)))
        .as(StepVerifier::create)
        .expectNext(7)
        .verifyComplete();
    connection
        .createStatement("SELECT 1, ?")
        .bind(0, 8)
        .execute()
        .flatMap(r -> r.map((row, meta) -> row.get(1, Integer.class)))
        .as(StepVerifier::create)
        .expectNext(8)
        .verifyComplete();
    connection.close().block();
    connection
        .createStatement("SELECT 7")
        .execute()
        .flatMap(r -> r.map((row, meta) -> row.get(0, Integer.class)))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && (throwable
                            .getMessage()
                            .equals("The connection is closed. Unable to send anything")
                        || throwable
                            .getMessage()
                            .contains("Cannot execute command since connection is already closed")))
        .verify();
  }

  @Test
  void basicConnectionWithSessionVariable() throws Exception {
    Map<String, Object> sessionVariable = new HashMap<>();
    sessionVariable.put("collation_connection", "utf8_slovenian_ci");
    sessionVariable.put("wait_timeout", 3600);
    MariadbConnectionConfiguration cnf =
        TestConfiguration.defaultBuilder.clone().sessionVariables(sessionVariable).build();
    MariadbConnection connection = new MariadbConnectionFactory(cnf).create().block();
    connection
        .createStatement("SELECT 5")
        .execute()
        .flatMap(r -> r.map((row, meta) -> row.get(0, Integer.class)))
        .as(StepVerifier::create)
        .expectNext(5)
        .verifyComplete();
    connection.close().block();

    sessionVariable.put("test", null);
    MariadbConnectionConfiguration cnf2 =
        TestConfiguration.defaultBuilder.clone().sessionVariables(sessionVariable).build();
    try {
      new MariadbConnectionFactory(cnf2).create().block();
      Assertions.fail("must have throw exception");
    } catch (Throwable t) {
      Assertions.assertEquals(R2dbcNonTransientResourceException.class, t.getClass());
      Assertions.assertNotNull(t.getCause());
      Assertions.assertEquals(R2dbcNonTransientResourceException.class, t.getCause().getClass());
      Assertions.assertTrue(
          t.getCause().getMessage().contains("Session variable 'test' has no value"));
    }
  }

  @Test
  void multipleClose() {
    MariadbConnection connection = factory.create().block();
    connection.close().subscribe();
    connection.close().block();
  }

  @Test
  void multipleBegin() throws Exception {
    MariadbConnection connection = factory.create().block();
    assert connection != null;
    multipleBegin(connection);
    connection.close().block();

    connection =
        new MariadbConnectionFactory(
                TestConfiguration.defaultBuilder.clone().build())
            .create()
            .block();
    assert connection != null;
    multipleBegin(connection);
    connection.close().block();
  }

  void multipleBegin(MariadbConnection con) throws Exception {
    con.beginTransaction().subscribe();
    con.beginTransaction().block();
    con.beginTransaction().block();
    con.rollbackTransaction().block();
  }

  @Test
  void multipleAutocommit() throws Exception {
    MariadbConnection connection = factory.create().block();
    assert connection != null;
    multipleAutocommit(connection);
    connection.close().block();

    connection =
        new MariadbConnectionFactory(
                TestConfiguration.defaultBuilder.clone().build())
            .create()
            .block();
    assert connection != null;
    multipleAutocommit(connection);
    connection.close().block();
  }

  void multipleAutocommit(MariadbConnection con) throws Exception {
    con.setAutoCommit(true).subscribe();
    con.setAutoCommit(true).block();
    con.setAutoCommit(false).block();
    con.setAutoCommit(true).block();
  }

  @Test
  void queryAfterClose() throws Exception {
    MariadbConnection connection = factory.create().block();
    MariadbStatement stmt = connection.createStatement("SELECT 1");
    connection.close().block();
    stmt.execute()
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && (throwable
                            .getMessage()
                            .contains("The connection is closed. Unable to send anything")
                        || throwable
                            .getMessage()
                            .contains("Cannot execute command since connection is already closed")))
        .verify();
  }

  private void consume(Connection connection) {
    Statement statement = connection.createStatement("select 1");
    Flux.from(statement.execute())
        .flatMap(it -> it.map((row, rowMetadata) -> row.get(0)))
        .blockLast();
  }

  @Test
  void multiThreading() throws Throwable {
    Assumptions.assumeTrue(
        !"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    List<ExecuteQueries> queries = new ArrayList<>(100);
    try {
      AtomicInteger completed = new AtomicInteger(0);
      ThreadPoolExecutor scheduler =
          new ThreadPoolExecutor(10, 20, 50, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

      for (int i = 0; i < 100; i++) {
        ExecuteQueries exec = new ExecuteQueries(completed);
        queries.add(exec);
        scheduler.execute(exec);
      }
      scheduler.shutdown();
      scheduler.awaitTermination(120, TimeUnit.SECONDS);
      Assertions.assertEquals(100, completed.get());
    } finally {
      for (ExecuteQueries exec : queries) exec.closeConnection();
    }
  }

  @Test
  @Timeout(120)
  void multiThreadingSameConnection() throws Throwable {
    MariadbConnection connection = factory.create().block();
    try {
      AtomicInteger completed = new AtomicInteger(0);
      ThreadPoolExecutor scheduler =
          new ThreadPoolExecutor(10, 20, 50, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

      for (int i = 0; i < 10; i++) {
        scheduler.execute(new ExecuteQueriesOnSameConnection(completed, connection));
      }
      scheduler.shutdown();
      scheduler.awaitTermination(100, TimeUnit.SECONDS);
      Assertions.assertEquals(10, completed.get());
    } finally {
      assert connection != null;
      connection.close().block();
    }
  }

  @Test
  void connectionAttributes() throws Exception {

    Map<String, String> connectionAttributes = new HashMap<>();
    connectionAttributes.put("APPLICATION", "MyApp");
    connectionAttributes.put("OTHER", "OTHER information");

    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder.clone().connectionAttributes(connectionAttributes).build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    assert connection != null;
    connection.close().block();
  }

  @Test
  void sessionVariables() throws Exception {
    BigInteger[] res =
        sharedConn
            .createStatement("SELECT @@lock_wait_timeout, @@multi_statement_xact_idle_timeout")
            .execute()
            .flatMap(
                r ->
                    r.map(
                        (row, metadata) ->
                            new BigInteger[] {
                              row.get(0, BigInteger.class), row.get(1, BigInteger.class)
                            }))
            .blockLast();

    Map<String, Object> sessionVariables = new HashMap<>();
    sessionVariables.put("multi_statement_xact_idle_timeout", 60);
    sessionVariables.put("lock_wait_timeout", 2147483);

    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder.clone().sessionVariables(sessionVariables).build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    assert connection != null;
    connection
        .createStatement("SELECT @@lock_wait_timeout, @@multi_statement_xact_idle_timeout")
        .execute()
        .flatMap(
            r ->
                r.map(
                    (row, metadata) -> {
                      Assertions.assertEquals(row.get(0, BigInteger.class).intValue(), 2147483);
                      Assertions.assertEquals(row.get(1, Integer.class), 60);
                      Assertions.assertNotEquals(
                          row.get(0, BigInteger.class).intValue(), res[0].intValue());
                      Assertions.assertNotEquals(row.get(1, Integer.class), res[1].intValue());
                      return 0;
                    }))
        .blockLast();

    connection.close().block();
  }

  @Test
  void getTransactionIsolationLevel() {
    MariadbConnection connection =
        new MariadbConnectionFactory(TestConfiguration.defaultBuilder.build()).create().block();
    try {
      IsolationLevel defaultValue = IsolationLevel.READ_COMMITTED;
      Assertions.assertEquals(defaultValue, connection.getTransactionIsolationLevel());
      connection.setTransactionIsolationLevel(IsolationLevel.READ_COMMITTED).block();
      connection.createStatement("BEGIN").execute().blockLast();
      Assertions.assertEquals(
          IsolationLevel.READ_COMMITTED, connection.getTransactionIsolationLevel());
      connection.setTransactionIsolationLevel(defaultValue).block();
    } finally {
      connection.close().block();
    }
  }

  @Test
  void getDatabase() {
    Assumptions.assumeFalse(isXpand());
    MariadbConnection connection =
        new MariadbConnectionFactory(TestConfiguration.defaultBuilder.build()).create().block();
    assertEquals(TestConfiguration.database, connection.getDatabase());
    connection.setDatabase("test_r2dbc").block();
    assertEquals("test_r2dbc", connection.getDatabase());
    String db =
        connection
            .createStatement("SELECT DATABASE()")
            .execute()
            .flatMap(it -> it.map((row, rowMetadata) -> row.get(0, String.class)))
            .blockFirst();
    assertEquals("test_r2dbc", db);
    connection.close().block();
  }

  @Test
  void rollbackTransaction() {
    sharedConn.createStatement("DROP TABLE IF EXISTS rollbackTable").execute().blockLast();
    sharedConn
        .createStatement("CREATE TABLE rollbackTable (t1 VARCHAR(256))")
        .execute()
        .blockLast();
    sharedConn.setAutoCommit(false).block();
    sharedConn.rollbackTransaction().block(); // must not do anything
    sharedConn.createStatement("INSERT INTO rollbackTable VALUES ('a')").execute().blockLast();
    sharedConn.rollbackTransaction().block();
    sharedConn
        .createStatement("SELECT * FROM rollbackTable")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .verifyComplete();
    sharedConn.createStatement("DROP TABLE IF EXISTS rollbackTable").execute().blockLast();
    sharedConn.setAutoCommit(true).block();
  }

  @Test
  void commitTransaction() throws Exception {
    MariadbConnection connection = factory.create().block();
    commitTransaction(connection);
    MariadbStatement stmt = connection.createStatement("DO 1");
    connection.close().block();
    assertThrows(R2dbcNonTransientResourceException.class, () -> stmt.execute().blockLast(), "");

    connection =
        new MariadbConnectionFactory(
                TestConfiguration.defaultBuilder.clone().build())
            .create()
            .block();
    commitTransaction(connection);
    MariadbStatement stmt2 = connection.createStatement("DO 1");
    connection.close().block();
    assertThrows(R2dbcNonTransientResourceException.class, () -> stmt2.execute().blockLast(), "");
  }

  void commitTransaction(MariadbConnection con) {
    con.createStatement("DROP TABLE IF EXISTS commitTransaction").execute().blockLast();
    con.createStatement("CREATE TABLE commitTransaction (t1 VARCHAR(256))").execute().blockLast();
    con.setAutoCommit(false).subscribe();
    con.setAutoCommit(false).block();
    con.commitTransaction().block(); // must not do anything
    con.createStatement("INSERT INTO commitTransaction VALUES ('a')").execute().blockLast();
    con.commitTransaction().subscribe();
    con.commitTransaction().block();
    con.createStatement("SELECT * FROM commitTransaction")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(Optional.of("a"))
        .verifyComplete();
    con.createStatement("DROP TABLE IF EXISTS commitTransaction").execute().blockLast();
    con.setAutoCommit(true).block();
  }

  @Test
  void useTransaction() throws Exception {
    MariadbConnection connection = factory.create().block();
    useTransaction(connection);
    MariadbStatement stmt = connection.createStatement("DO 1");
    connection.close().block();
    assertThrows(
        R2dbcNonTransientResourceException.class,
        () -> stmt.execute().blockLast(),
        "The connection is closed. Unable to send anything");

    connection =
        new MariadbConnectionFactory(
                TestConfiguration.defaultBuilder.clone().build())
            .create()
            .block();
    useTransaction(connection);
    MariadbStatement stmt2 = connection.createStatement("DO 1");
    connection.close().block();
    assertThrows(
        R2dbcNonTransientResourceException.class,
        () -> stmt2.execute().blockLast(),
        "The connection is closed. Unable to send anything");
  }

  void useTransaction(MariadbConnection conn) {
    conn.createStatement("DROP TABLE IF EXISTS useTransaction").execute().blockLast();
    conn.createStatement("CREATE TABLE useTransaction (t1 VARCHAR(256))").execute().blockLast();
    conn.beginTransaction().subscribe();
    conn.beginTransaction().block();
    conn.createStatement("INSERT INTO useTransaction VALUES ('a')").execute().blockLast();
    conn.commitTransaction().block();
    conn.createStatement("SELECT * FROM useTransaction")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(Optional.of("a"))
        .verifyComplete();
    conn.createStatement("DROP TABLE IF EXISTS useTransaction").execute().blockLast();
  }

  @Test
  void toStringTest() {
    MariadbConnection connection =
        new MariadbConnectionFactory(TestConfiguration.defaultBuilder.build()).create().block();
    try {
      Assertions.assertTrue(
          connection
              .toString()
              .contains(
                  "MariadbConnection{client=Client{isClosed=false, "
                      + "context=ConnectionContext{"));
    } finally {
      connection.close().block();
    }
  }

  @Test
  public void isolationLevel() {
    MariadbConnection connection =
        new MariadbConnectionFactory(TestConfiguration.defaultBuilder.build()).create().block();

    Assertions.assertThrows(
        Exception.class, () -> connection.setTransactionIsolationLevel(null).block());
    connection.setTransactionIsolationLevel(IsolationLevel.READ_COMMITTED).block();
    assertEquals(IsolationLevel.READ_COMMITTED, connection.getTransactionIsolationLevel());
    connection.close().block();
    Assertions.assertThrows(
        java.lang.IllegalArgumentException.class,
        () -> connection.setTransactionIsolationLevel(IsolationLevel.READ_UNCOMMITTED).block());
  }

  @Test
  public void noDb() throws Throwable {
    MariadbConnection connection =
        new MariadbConnectionFactory(
                TestConfiguration.defaultBuilder.clone().database(null).build())
            .create()
            .block();
    try {
      connection
          .createStatement("SELECT DATABASE()")
          .execute()
          .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0, String.class))))
          .as(StepVerifier::create)
          .expectNext(Optional.empty())
          .verifyComplete();
    } finally {
      connection.close().block();
    }
  }


  @Test
  public void errorOnConnection() throws Throwable {
    sharedConn
        .createStatement("SET GLOBAL max_connections=400")
        .execute()
        .blockLast();
    try {
      BigInteger maxConn =
          sharedConn
              .createStatement("select @@max_connections")
              .execute()
              .flatMap(r -> r.map((row, metadata) -> row.get(0, BigInteger.class)))
              .blockLast();
      Assumptions.assumeTrue(maxConn.intValue() < 600);

      Throwable expected = null;
      Mono<?>[] cons = new Mono<?>[maxConn.intValue()];
      for (int i = 0; i < maxConn.intValue(); i++) {
        cons[i] = new MariadbConnectionFactory(TestConfiguration.defaultBuilder.build()).create();
      }
      MariadbConnection[] connections = new MariadbConnection[maxConn.intValue()];
      for (int i = 0; i < maxConn.intValue(); i++) {
        try {
          connections[i] = (MariadbConnection) cons[i].block();
        } catch (Throwable e) {
          expected = e;
        }
      }

      for (int i = 0; i < maxConn.intValue(); i++) {
        if (connections[i] != null) {
          connections[i].close().block();
        }
      }
      Assertions.assertNotNull(expected);
      Assertions.assertTrue(expected.getMessage().contains("Fail to establish connection to"));
      Assertions.assertTrue(expected.getCause().getMessage().contains("Too many connections"));
      Thread.sleep(1000);
    } finally {
      sharedConn
          .createStatement("SET GLOBAL max_connections=100000")
          .execute()
          .blockLast();
    }
  }

  @Test
  @Timeout(2)
  void killedConnection() {
    Assumptions.assumeTrue(
        !isMaxscale()
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));
    MariadbConnection connection = factory.create().block();
    long threadId = connection.getThreadId();
    assertNotNull(connection.getHost());
    assertEquals(TestConfiguration.defaultBuilder.build().getPort(), connection.getPort());

    Runnable runnable =
        () -> {
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          sharedConn.createStatement("KILL CONNECTION " + threadId).execute().blockLast();
        };
    Thread thread = new Thread(runnable);
    thread.start();

    try {
      connection
          .createStatement(
              "select * from information_schema.columns as c1, "
                  + "information_schema.tables, information_schema.tables as t2")
          .execute()
          .flatMap(r -> r.map((rows, meta) -> ""))
          .blockLast();
      Assertions.fail("must have throw exception");
    } catch (Throwable t) {
      Assertions.assertEquals(R2dbcNonTransientResourceException.class, t.getClass());
      Assertions.assertTrue(
          t.getMessage().contains("The connection is closed. Unable to send anything")
              || t.getMessage().contains("Connection unexpectedly closed")
              || t.getMessage().contains("Connection unexpected error")
              || t.getMessage().contains("Connection error"),
          "real msg:" + t.getMessage());
      connection
          .validate(ValidationDepth.LOCAL)
          .as(StepVerifier::create)
          .expectNext(Boolean.FALSE)
          .verifyComplete();
    }
    connection
        .validate(ValidationDepth.LOCAL)
        .as(StepVerifier::create)
        .expectNext(Boolean.FALSE)
        .verifyComplete();
  }

  @Test
  public void queryTimeout() throws Throwable {
    MariadbConnection connection =
        new MariadbConnectionFactory(TestConfiguration.defaultBuilder.clone().build())
            .create()
            .block();
    assertThrows(
        UnsupportedOperationException.class,
        () -> connection.setStatementTimeout(Duration.ofMillis(500)).block(),
        "Statement timeout is not supported");
  }

  @Test
  public void setLockWaitTimeout() {
    sharedConn.setLockWaitTimeout(Duration.ofMillis(1)).block();
    sharedConn.setLockWaitTimeout(Duration.ofMillis(10000)).block();
  }

  @Test
  public void testPools() throws Throwable {
    boolean hasReactorTcp = false;
    boolean hasMariaDbThreads = false;
    Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
    for (Thread thread : threadSet) {
      if (thread.getName().contains("reactor-tcp")) hasReactorTcp = true;
      if (thread.getName().contains("mariadb")) hasMariaDbThreads = true;
    }
    assertTrue(hasReactorTcp);
    assertFalse(hasMariaDbThreads);

    MariadbConnection connection =
        new MariadbConnectionFactory(
                TestConfiguration.defaultBuilder
                    .clone()
                    .loopResources(LoopResources.create("mariadb"))
                    .build())
            .create()
            .block();

    threadSet = Thread.getAllStackTraces().keySet();
    for (Thread thread : threadSet) {
      if (thread.getName().contains("mariadb")) {
        hasMariaDbThreads = true;
        break;
      }
    }
    assertTrue(hasMariaDbThreads);

    connection.close().block();
  }

  protected class ExecuteQueries implements Runnable {
    private final AtomicInteger i;
    private MariadbConnection connection = null;

    public ExecuteQueries(AtomicInteger i) {
      this.i = i;
    }

    public void run() {
      try {
        connection = factory.create().block();
        int rnd = (int) (Math.random() * 1000);
        Statement statement = connection.createStatement("select " + rnd);
        BigInteger val =
            Flux.from(statement.execute())
                .flatMap(it -> it.map((row, rowMetadata) -> row.get(0, BigInteger.class)))
                .blockLast();
        if (rnd != val.intValue())
          throw new IllegalStateException("ERROR rnd:" + rnd + " different to val:" + val);
        i.incrementAndGet();
      } catch (Throwable e) {
        e.printStackTrace();
      } finally {
        closeConnection();
      }
    }

    public synchronized void closeConnection() {
      if (connection != null) {
        connection.close().block();
        connection = null;
      }
    }
  }

  protected class ExecuteQueriesOnSameConnection implements Runnable {
    private final AtomicInteger i;
    private final MariadbConnection connection;

    public ExecuteQueriesOnSameConnection(AtomicInteger i, MariadbConnection connection) {
      this.i = i;
      this.connection = connection;
    }

    public void run() {
      try {
        int rnd = (int) (Math.random() * 1000);
        Statement statement = connection.createStatement("select " + rnd);
        Integer val =
            Flux.from(statement.execute())
                .flatMap(it -> it.map((row, rowMetadata) -> row.get(0, Integer.class)))
                .blockLast();
        if (rnd != val.intValue())
          throw new IllegalStateException("ERROR rnd:" + rnd + " different to val:" + val);
        i.incrementAndGet();
      } catch (Throwable e) {
        e.printStackTrace();
      }
    }
  }
}
