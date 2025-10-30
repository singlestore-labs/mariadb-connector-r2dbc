// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.TransactionDefinition;
import io.r2dbc.spi.ValidationDepth;
import java.time.Duration;
import java.util.function.Function;
import org.mariadb.r2dbc.api.MariadbStatement;
import org.mariadb.r2dbc.client.Client;
import org.mariadb.r2dbc.message.client.ChangeSchemaPacket;
import org.mariadb.r2dbc.message.client.PingPacket;
import org.mariadb.r2dbc.message.client.QueryPacket;
import org.mariadb.r2dbc.util.Assert;
import org.mariadb.r2dbc.util.PrepareCache;
import org.mariadb.r2dbc.util.constants.Capabilities;
import org.mariadb.r2dbc.util.constants.ServerStatus;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

public final class MariadbConnection implements org.mariadb.r2dbc.api.MariadbConnection {

  private final Logger logger = Loggers.getLogger(this.getClass());
  private final Client client;
  private final MariadbConnectionConfiguration configuration;
  private volatile String database;

  public MariadbConnection(
      Client client, MariadbConnectionConfiguration configuration) {
    this.client = Assert.requireNonNull(client, "client must not be null");
    this.configuration = Assert.requireNonNull(configuration, "configuration must not be null");
    this.database = configuration.getDatabase();
  }

  @Override
  public Mono<Void> beginTransaction() {
    return this.client.beginTransaction();
  }

  @Override
  public Mono<Void> beginTransaction(TransactionDefinition definition) {
    Mono<Void> request = Mono.empty();

    return request.then(this.client.beginTransaction(definition));
  }

  @Override
  public Mono<Void> close() {
    return this.client.close().then(Mono.empty());
  }

  @Override
  public Mono<Void> commitTransaction() {
    return this.client.commitTransaction();
  }

  @Override
  public MariadbBatch createBatch() {
    return new MariadbBatch(this.client, this.configuration);
  }

  @Override
  public Mono<Void> createSavepoint(String name) {
    throw new UnsupportedOperationException("Savepoints are not supported in SingleStore");
  }

  @Override
  public MariadbStatement createStatement(String sql) {
    Assert.requireNonNull(sql, "sql must not be null");
    if (sql.trim().isEmpty()) {
      throw new IllegalArgumentException("Statement cannot be empty.");
    }

    if ((this.configuration.useServerPrepStmts() || sql.contains("call"))
        && !sql.startsWith("/*text*/")) {
      return new MariadbServerParameterizedQueryStatement(this.client, sql, this.configuration);
    }
    return new MariadbClientParameterizedQueryStatement(this.client, sql, this.configuration);
  }

  @Override
  public MariadbConnectionMetadata getMetadata() {
    return new MariadbConnectionMetadata(this.client.getVersion());
  }

  @Override
  public IsolationLevel getTransactionIsolationLevel() {
    return IsolationLevel.READ_COMMITTED;
  }

  @Override
  public boolean isAutoCommit() {
    return this.client.isAutoCommit() && !this.client.isInTransaction();
  }

  @Override
  public Mono<Void> releaseSavepoint(String name) {
    throw new UnsupportedOperationException("Savepoints are not supported in SingleStore");
  }

  @Override
  public long getThreadId() {
    return this.client.getThreadId();
  }

  @Override
  public boolean isInTransaction() {
    return (this.client.getContext().getServerStatus() & ServerStatus.IN_TRANSACTION) > 0;
  }

  @Override
  public boolean isInReadOnlyTransaction() {
    return (this.client.getContext().getServerStatus() & ServerStatus.STATUS_IN_TRANS_READONLY) > 0;
  }

  @Override
  public String getHost() {
    return this.client.getHostAddress() != null ? this.client.getHostAddress().getHost() : null;
  }

  @Override
  public int getPort() {
    return this.client.getHostAddress() != null ? this.client.getHostAddress().getPort() : 3306;
  }

  @Override
  public Mono<Void> rollbackTransaction() {
    return this.client.rollbackTransaction().then();
  }

  @Override
  public Mono<Void> rollbackTransactionToSavepoint(String name) {
    throw new UnsupportedOperationException("Savepoints are not supported in SingleStore");
  }

  @Override
  public Mono<Void> setAutoCommit(boolean autoCommit) {
    return client
        .setAutoCommit(autoCommit)
        .then();
  }

  @Override
  public Mono<Void> setLockWaitTimeout(Duration timeout) {
    Assert.requireNonNull(timeout, "timeout must not be null");
    long msValue = timeout.toMillis();

    String sql = String.format("SET lock_wait_timeout=%s", Math.max(msValue / 1000, 1));
    ExceptionFactory exceptionFactory = ExceptionFactory.withSql(sql);

    return client
        .sendCommand(new QueryPacket(sql), true)
        .handle(exceptionFactory::handleErrorResponse)
        .then();
  }

  @Override
  public Mono<Void> setStatementTimeout(Duration timeout) {
    return Mono.error(new UnsupportedOperationException("Statement timeout is not supported"));
  }

  @Override
  public Mono<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
    Assert.requireNonNull(isolationLevel, "isolationLevel must not be null");
    if (isolationLevel != IsolationLevel.READ_COMMITTED) {
      throw new IllegalArgumentException(
          "SingleStore supports only the READ_COMMITTED isolation level");
    }

    return Mono.empty();
  }

  @Override
  public String toString() {
    return "MariadbConnection{client="
        + client
        + '}';
  }

  @Override
  public Mono<Boolean> validate(ValidationDepth depth) {
    if (this.client.isCloseRequested()
        || (HaMode.NONE.equals(this.configuration.getHaMode()) && !this.client.isConnected())) {
      return Mono.just(false);
    }

    if (HaMode.NONE.equals(this.configuration.getHaMode()) && depth == ValidationDepth.LOCAL) {
      return Mono.just(this.client.isConnected());
    }

    return Mono.create(
        sink -> {
          // only when using failover, connection might be recreated
          if (HaMode.NONE.equals(this.configuration.getHaMode()) && !this.client.isConnected()) {
            sink.success(false);
            return;
          }

          this.client
              .sendCommand(new PingPacket(), true)
              .windowUntil(it -> it.ending())
              .flatMap(Function.identity())
              .subscribe(
                  msg -> sink.success(true),
                  err -> {
                    logger.debug("Ping error", err);
                    sink.success(false);
                  });
        });
  }

  @Override
  public String getDatabase() {
    if ((client.getContext().getClientCapabilities() | Capabilities.CLIENT_SESSION_TRACK) > 0)
      return client.getContext().getDatabase();
    return this.database;
  }

  public Mono<Void> setDatabase(String database) {
    Assert.requireNonNull(database, "database must not be null");

    if ((client.getContext().getClientCapabilities() | Capabilities.CLIENT_SESSION_TRACK) > 0
        && client.getContext().getDatabase() != null
        && client.getContext().getDatabase().equals(database)) return Mono.empty();

    ExceptionFactory exceptionFactory = ExceptionFactory.withSql("COM_INIT_DB");
    final String newDatabase = database;
    return client
        .sendCommand(new ChangeSchemaPacket(database), true)
        .handle(exceptionFactory::handleErrorResponse)
        .then()
        .doOnSuccess(ignore -> {
          this.database = newDatabase;
          client.getContext().setDatabase(database);
        });
  }

  public PrepareCache _test_prepareCache() {
    return client.getPrepareCache();
  }
}
