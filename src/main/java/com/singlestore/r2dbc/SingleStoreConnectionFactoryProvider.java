// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc;

import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;

import io.netty.handler.ssl.SslContextBuilder;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ConnectionFactoryProvider;
import io.r2dbc.spi.Option;
import java.util.function.UnaryOperator;
import com.singlestore.r2dbc.util.Assert;
import reactor.netty.resources.LoopResources;

public final class SingleStoreConnectionFactoryProvider implements ConnectionFactoryProvider {
  public static final String MARIADB_DRIVER = "mariadb";
  public static final Option<String> SOCKET = Option.valueOf("socket");
  public static final Option<Boolean> ALLOW_MULTI_QUERIES = Option.valueOf("allowMultiQueries");
  public static final Option<String> TLS_PROTOCOL = Option.valueOf("tlsProtocol");
  public static final Option<String> SERVER_SSL_CERT = Option.valueOf("serverSslCert");
  public static final Option<String> CLIENT_SSL_CERT = Option.valueOf("clientSslCert");
  public static final Option<String> CLIENT_SSL_KEY = Option.valueOf("clientSslKey");
  public static final Option<String> CLIENT_SSL_PWD = Option.valueOf("clientSslPassword");

  public static final Option<Boolean> USE_SERVER_PREPARE = Option.valueOf("useServerPrepStmts");
  public static final Option<Boolean> AUTO_COMMIT = Option.valueOf("autocommit");
  public static final Option<Boolean> SKIP_POST_COMMANDS = Option.valueOf("skipPostCommands");
  public static final Option<Integer> PREPARE_CACHE_SIZE = Option.valueOf("prepareCacheSize");
  public static final Option<String> SSL_MODE = Option.valueOf("sslMode");
  public static final Option<Boolean> TRANSACTION_REPLAY = Option.valueOf("transactionReplay");
  public static final Option<String> HAMODE = Option.valueOf("haMode");

  public static final Option<String> CONNECTION_ATTRIBUTES = Option.valueOf("connectionAttributes");
  public static final Option<String> PAM_OTHER_PASSWORD = Option.valueOf("pamOtherPwd");
  public static final Option<Boolean> TCP_KEEP_ALIVE = Option.valueOf("tcpKeepAlive");
  public static final Option<Boolean> TCP_ABORTIVE_CLOSE = Option.valueOf("tcpAbortiveClose");
  public static final Option<String> SESSION_VARIABLES = Option.valueOf("sessionVariables");
  public static final Option<LoopResources> LOOP_RESOURCES = Option.valueOf("loopResources");

  public static final Option<String> RESTRICTED_AUTH = Option.valueOf("restrictedAuth");

  public static final Option<UnaryOperator<SslContextBuilder>> SSL_CONTEXT_BUILDER_CUSTOMIZER =
      Option.valueOf("sslContextBuilderCustomizer");
  public static final Option<Boolean> SSL_TUNNEL_DISABLE_HOST_VERIFICATION =
      Option.valueOf("sslTunnelDisableHostVerification");

  static SingleStoreConnectionConfiguration createConfiguration(
      ConnectionFactoryOptions connectionFactoryOptions) {
    Assert.requireNonNull(connectionFactoryOptions, "connectionFactoryOptions must not be null");
    return SingleStoreConnectionConfiguration.fromOptions(connectionFactoryOptions).build();
  }

  @Override
  public SingleStoreConnectionFactory create(ConnectionFactoryOptions connectionFactoryOptions) {
    return new SingleStoreConnectionFactory(createConfiguration(connectionFactoryOptions));
  }

  @Override
  public String getDriver() {
    return MARIADB_DRIVER;
  }

  @Override
  public boolean supports(ConnectionFactoryOptions connectionFactoryOptions) {
    Assert.requireNonNull(connectionFactoryOptions, "connectionFactoryOptions must not be null");

    String driver = (String) connectionFactoryOptions.getValue(DRIVER);
    return MARIADB_DRIVER.equals(driver);
  }
}
