// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc;

import io.r2dbc.spi.ConnectionFactoryOptions;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import com.singlestore.r2dbc.api.SingleStoreConnection;
import com.singlestore.r2dbc.api.SingleStoreConnectionMetadata;

public class TestConfiguration {

  public static final String host;
  public static final int port;
  public static final String username;
  public static final String password;
  public static final String database;
  public static final String other;
  public static final SingleStoreConnectionConfiguration.Builder defaultBuilder;
  public static final SingleStoreConnectionConfiguration defaultConf;
  public static final SingleStoreConnectionFactory defaultFactory;

  static {
    String defaultHost = "localhost";
    String defaultPort = "3306";
    String defaultDatabase = "testr2";
    String defaultPassword = "";
    String defaultUser = "root";
    String defaultOther = null;

    try (InputStream inputStream =
        BaseConnectionTest.class.getClassLoader().getResourceAsStream("conf.properties")) {
      Properties prop = new Properties();
      prop.load(inputStream);

      defaultHost = get("DB_HOST", prop);
      defaultPort = get("DB_PORT", prop);
      defaultDatabase = get("DB_DATABASE", prop);
      defaultPassword = get("DB_PASSWORD", prop);
      defaultUser = get("DB_USER", prop);

      String val = System.getenv("TEST_REQUIRE_TLS");
      if ("1".equals(val)) {
        String cert = System.getenv("TEST_DB_SERVER_CERT");
        defaultOther = "sslMode=enable&serverSslCert=" + cert;
      } else {
        defaultOther = get("DB_OTHER", prop);
      }
    } catch (IOException io) {
      io.printStackTrace();
    }
    host = defaultHost;
    port = Integer.parseInt(defaultPort);
    database = defaultDatabase;
    password = defaultPassword;
    username = defaultUser;
    other = defaultOther;
    String encodedUser;
    String encodedPwd;
    try {
      encodedUser = URLEncoder.encode(username, StandardCharsets.UTF_8.toString());
      encodedPwd = URLEncoder.encode(password, StandardCharsets.UTF_8.toString());
    } catch (UnsupportedEncodingException e) {
      encodedUser = username;
      encodedPwd = password;
    }
    String connString =
        String.format(
            "r2dbc:singlestore://%s:%s@%s:%s/%s%s",
            encodedUser,
            encodedPwd,
            host,
            port,
            database,
            other == null ? "" : "?" + other.replace("\n", "\\n"));

    ConnectionFactoryOptions options = ConnectionFactoryOptions.parse(connString);
    defaultBuilder = SingleStoreConnectionConfiguration.fromOptions(options);
    try {
      SingleStoreConnection connection =
          new SingleStoreConnectionFactory(
                  SingleStoreConnectionConfiguration.fromOptions(options)
                      .build())
              .create()
              .block();
      SingleStoreConnectionMetadata meta = connection.getMetadata();
      connection.close().block();
    } catch (Exception e) {
      // eat
      e.printStackTrace();
    }

    defaultConf = defaultBuilder.build();
    defaultFactory = new SingleStoreConnectionFactory(defaultConf);
  }

  private static String get(String name, Properties prop) {
    String val = System.getenv("TEST_" + name);
    if (val == null) val = System.getProperty("TEST_" + name);
    if (val == null) val = prop.getProperty(name);
    return val;
  }
}
