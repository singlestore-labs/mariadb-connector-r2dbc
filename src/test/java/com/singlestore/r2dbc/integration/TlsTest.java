// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.integration;

import io.r2dbc.spi.R2dbcNonTransientException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcTransientResourceException;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import com.singlestore.r2dbc.*;
import com.singlestore.r2dbc.api.SingleStoreConnection;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class TlsTest extends BaseConnectionTest {

  public static String serverSslCert;
  public static String clientSslCert;
  public static String clientSslKey;
  public static int sslPort;

  @BeforeAll
  public static void before2() {
    Assumptions.assumeTrue(!isXpand());
    serverSslCert = System.getenv("TEST_DB_SERVER_CERT");
    clientSslCert = System.getenv("TEST_DB_CLIENT_CERT");
    clientSslKey = System.getenv("TEST_DB_CLIENT_KEY");
    if ("".equals(serverSslCert)) {
      serverSslCert = null;
      clientSslCert = null;
      clientSslKey = null;
    }
    sslPort =
        System.getenv("TEST_MAXSCALE_TLS_PORT") == null
                || System.getenv("TEST_MAXSCALE_TLS_PORT").isEmpty()
            ? TestConfiguration.port
            : Integer.valueOf(System.getenv("TEST_MAXSCALE_TLS_PORT"));
    // try default if not present
    if (serverSslCert == null) {
      File sslDir = new File(System.getProperty("user.dir") + "/../ssl");
      if (!sslDir.exists() || !sslDir.isDirectory()) {
        sslDir = new File(System.getProperty("user.dir") + "/../../ssl");
      }
      if (sslDir.exists() && sslDir.isDirectory()) {
        serverSslCert = sslDir.getPath() + "/server.crt";
        clientSslCert = sslDir.getPath() + "/client.crt";
        clientSslKey = sslDir.getPath() + "/client.key";
      }
    }
  }

  private static String readLine(String filePath) throws IOException {
    StringBuilder contentBuilder = new StringBuilder();
    try (Stream<String> stream = Files.lines(Paths.get(filePath), StandardCharsets.UTF_8)) {
      stream.forEach(s -> contentBuilder.append(s).append("\n"));
    }
    return contentBuilder.toString();
  }

  @Test
  public void testWithoutPassword() throws Throwable {
    Assumptions.assumeTrue(!isMaxscale() && !isEnterprise());
    Assumptions.assumeTrue(haveSsl(sharedConn));
    sharedConn.createStatement("CREATE USER userWithoutPassword"+getHostSuffix()).execute().blockLast();
    sharedConn
        .createStatement(
            String.format(
                "GRANT SELECT on `%s`.* to userWithoutPassword", TestConfiguration.database)+getHostSuffix())
        .execute()
        .blockLast();
    SingleStoreConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .username("userWithoutPassword")
            .password("")
            .port(sslPort)
            .sslMode(SslMode.TRUST)
            .build();
    SingleStoreConnection connection = new SingleStoreConnectionFactory(conf).create().block();
    connection.close().block();
    sharedConn
        .createStatement("DROP USER IF EXISTS userWithoutPassword"+getHostSuffix())
        .execute()
        .map(res -> res.getRowsUpdated())
        .onErrorReturn(Mono.empty())
        .blockLast();
  }

  @Test
  void defaultHasNoSSL() throws Exception {
    Assumptions.assumeTrue(
        !isMaxscale()
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));
    Assumptions.assumeTrue(haveSsl(sharedConn));
    sharedConn
        .createStatement("SHOW STATUS like 'Ssl_version'")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(1)))
        .as(StepVerifier::create)
        .expectNextMatches(
            val -> {
              String[] values = {"TLSv1", "TLSv1.1", "TLSv1.2", "TLSv1.3"};
              return !Arrays.stream(values).anyMatch(val::equals);
            })
        .verifyComplete();
  }

  @Test
  void trustValidation() throws Exception {
    Assumptions.assumeTrue(!isMaxscale() && !"skysql-ha".equals(System.getenv("srv")));
    Assumptions.assumeTrue(haveSsl(sharedConn));
    SingleStoreConnectionConfiguration conf =
        TestConfiguration.defaultBuilder.clone().port(sslPort).sslMode(SslMode.TRUST).build();
    SingleStoreConnection connection = new SingleStoreConnectionFactory(conf).create().block();
    connection
        .createStatement("SHOW STATUS like 'Ssl_version'")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(1)))
        .as(StepVerifier::create)
        .expectNextMatches(
            val -> {
              String[] values = {"TLSv1", "TLSv1.1", "TLSv1.2", "TLSv1.3"};
              return Arrays.stream(values).anyMatch(val::equals);
            })
        .verifyComplete();
    connection.close().block();
  }

  @Test
  void wrongCertificateFiles() throws Exception {
    Assumptions.assumeTrue(haveSsl(sharedConn) && serverSslCert != null);
    assertThrows(
        R2dbcTransientResourceException.class,
        () ->
            TestConfiguration.defaultBuilder
                .clone()
                .port(sslPort)
                .sslMode(SslMode.VERIFY_CA)
                .serverSslCert("wrongFile")
                .build(),
        "Failed to find serverSslCert file. serverSslCert=wrongFile");
    if (serverSslCert != null) {
      assertThrows(
          R2dbcTransientResourceException.class,
          () ->
              TestConfiguration.defaultBuilder
                  .clone()
                  .port(sslPort)
                  .sslMode(SslMode.VERIFY_CA)
                  .serverSslCert(serverSslCert)
                  .clientSslCert("wrongFile")
                  .clientSslKey("dd")
                  .clientSslPassword(null)
                  .build(),
          "Failed to find clientSslCert file. clientSslCert=wrongFile");
      if (clientSslCert != null) {
        assertThrows(
            R2dbcTransientResourceException.class,
            () ->
                TestConfiguration.defaultBuilder
                    .clone()
                    .port(sslPort)
                    .sslMode(SslMode.VERIFY_CA)
                    .serverSslCert(serverSslCert)
                    .clientSslCert(clientSslCert)
                    .clientSslKey("dd")
                    .build(),
            "Failed to find clientSslKey file. clientSslKey=dd");
      }
    }
  }

  @Test
  void trustForceProtocol() throws Exception {
    String trustProtocol = "TLSv1.2";
    Assumptions.assumeTrue(haveSsl(sharedConn));
    SingleStoreConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .port(sslPort)
            .sslMode(SslMode.TRUST)
            .tlsProtocol(trustProtocol)
            .build();
    SingleStoreConnection connection = new SingleStoreConnectionFactory(conf).create().block();
    connection
        .createStatement("SHOW STATUS like 'Ssl_version'")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(1)))
        .as(StepVerifier::create)
        .expectNext(trustProtocol)
        .verifyComplete();
    connection.close().block();
  }

  @Test
  void withoutHostnameValidation() throws Throwable {
    Assumptions.assumeTrue(haveSsl(sharedConn));
    Assumptions.assumeTrue(serverSslCert != null);
    SingleStoreConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .port(sslPort)
            .sslMode(SslMode.VERIFY_CA)
            .serverSslCert(serverSslCert)
            .build();
    SingleStoreConnection connection = null;
    try {
      connection = new SingleStoreConnectionFactory(conf).create().block();
      connection
          .createStatement("SHOW STATUS like 'Ssl_version'")
          .execute()
          .flatMap(r -> r.map((row, metadata) -> row.get(1)))
          .as(StepVerifier::create)
          .expectNextMatches(
              val -> {
                if (isMaxscale() && !"skysql-ha".equals(System.getenv("srv"))) return true;
                String[] values = {"TLSv1", "TLSv1.1", "TLSv1.2", "TLSv1.3"};
                return Arrays.stream(values).anyMatch(val::equals);
              })
          .verifyComplete();
      connection.close().block();
      connection = null;
      String serverCertString = readLine(serverSslCert);
      SingleStoreConnectionConfiguration conf2 =
          TestConfiguration.defaultBuilder
              .clone()
              .port(sslPort)
              .sslMode(SslMode.VERIFY_CA)
              .serverSslCert(serverCertString)
              .build();
      SingleStoreConnection con2 = new SingleStoreConnectionFactory(conf2).create().block();
      con2.close().block();
    } finally {
      if (connection != null) connection.close().block();
    }
  }

  @Test
  void fullWithoutServerCert() throws Exception {
    Assumptions.assumeTrue(
        !isMaxscale()
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv")));
    Assumptions.assumeTrue(haveSsl(sharedConn));
    assertThrows(
        R2dbcTransientResourceException.class,
        () -> TestConfiguration.defaultBuilder.clone().sslMode(SslMode.VERIFY_FULL).build(),
        "Server certificate needed (option `serverSslCert`) for ssl mode VERIFY_FULL");
  }

  @Test
  void fullValidationFailing() throws Exception {
    Assumptions.assumeTrue(
        !"skysql".equals(System.getenv("srv")) && !"skysql-ha".equals(System.getenv("srv")));
    Assumptions.assumeTrue(haveSsl(sharedConn));
    Assumptions.assumeTrue(serverSslCert != null);
    Assumptions.assumeFalse(
        "localhost".equals(TestConfiguration.host) || "1".equals(System.getenv("local")));
    SingleStoreConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .port(sslPort)
            .host(TestConfiguration.host)
            .sslMode(SslMode.VERIFY_FULL)
            .serverSslCert(serverSslCert)
            .build();
    new SingleStoreConnectionFactory(conf)
        .create()
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && throwable.getMessage().contains("No subject alternative names matching"))
        .verify();
  }

  @Test
  void fullValidation() throws Exception {
    Assumptions.assumeTrue(haveSsl(sharedConn));
    Assumptions.assumeTrue(serverSslCert != null);
    SingleStoreConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .port(sslPort)
            .sslMode(SslMode.VERIFY_FULL)
            .host("localhost")
            .serverSslCert(serverSslCert)
            .build();
    SingleStoreConnection connection = null;
    try {
      connection = new SingleStoreConnectionFactory(conf).create().block();
      connection
          .createStatement("SHOW STATUS like 'Ssl_version'")
          .execute()
          .flatMap(r -> r.map((row, metadata) -> row.get(1)))
          .as(StepVerifier::create)
          .expectNextMatches(
              val -> {
                if (isMaxscale() && !"skysql-ha".equals(System.getenv("srv"))) return true;
                String[] values = {"TLSv1", "TLSv1.1", "TLSv1.2", "TLSv1.3"};
                return Arrays.stream(values).anyMatch(val::equals);
              })
          .verifyComplete();
    } finally {
      if (connection != null) connection.close().block();
    }
  }

  @Test
  void fullValidationCertError() throws Exception {
    Assumptions.assumeTrue(haveSsl(sharedConn));
    Assumptions.assumeTrue(serverSslCert != null);

    SingleStoreConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .port(sslPort)
            .sslMode(SslMode.VERIFY_FULL)
            .host("singlestore2.example.com")
            .serverSslCert(serverSslCert)
            .build();

    new SingleStoreConnectionFactory(conf)
        .create()
        .as(StepVerifier::create)
        .expectErrorMatches(throwable -> throwable instanceof R2dbcNonTransientException)
        .verify();
  }

  @Test
  void fullMutualWithoutClientCerts() throws Exception {
    Assumptions.assumeTrue(!isMaxscale() && !"skysql-ha".equals(System.getenv("srv")));
    Assumptions.assumeTrue(haveSsl(sharedConn));
    Assumptions.assumeTrue(serverSslCert != null && clientSslCert != null & clientSslKey != null);
    SingleStoreConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .sslMode(SslMode.TRUST)
            .port(sslPort)
            .username("root-mutual-ssl")
            .host("localhost")
            .serverSslCert(serverSslCert)
            .clientSslKey(clientSslKey)
            .build();

    new SingleStoreConnectionFactory(conf)
        .create()
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientException
                    && throwable.getMessage().contains("Access denied"))
        .verify();
  }

  @Test
  void fullMutualAuthentication() throws Exception {
    Assumptions.assumeTrue(haveSsl(sharedConn));
    Assumptions.assumeTrue(serverSslCert != null && clientSslCert != null & clientSslKey != null);
    Assumptions.assumeTrue(minVersion(9, 0, 0));
    SingleStoreConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .sslMode(SslMode.TRUST)
            .port(sslPort)
            .username("root-mutual-ssl")
            .password(null)
            .host("localhost")
            .serverSslCert(serverSslCert)
            .clientSslCert(clientSslCert)
            .clientSslKey(clientSslKey)
            .build();
    SingleStoreConnection connection = new SingleStoreConnectionFactory(conf).create().block();
    connection
        .createStatement("SHOW STATUS like 'Ssl_version'")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(1)))
        .as(StepVerifier::create)
        .expectNextMatches(
            val -> {
              if (isMaxscale() && !"skysql-ha".equals(System.getenv("srv"))) return true;
              String[] values = {"TLSv1", "TLSv1.1", "TLSv1.2", "TLSv1.3"};
              return Arrays.stream(values).anyMatch(val::equals);
            })
        .verifyComplete();
    connection.close().block();
  }
}
