// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc;

import com.singlestore.r2dbc.api.SingleStoreStatement;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.TransactionDefinition;
import io.r2dbc.spi.ValidationDepth;
import java.time.Duration;
import java.util.function.Function;

import com.singlestore.r2dbc.client.Client;
import com.singlestore.r2dbc.message.client.ChangeSchemaPacket;
import com.singlestore.r2dbc.message.client.PingPacket;
import com.singlestore.r2dbc.message.client.QueryPacket;
import com.singlestore.r2dbc.util.Assert;
import com.singlestore.r2dbc.util.PrepareCache;
import com.singlestore.r2dbc.util.constants.ServerStatus;
import reactor.core.publisher.Mono;
import reactor.util.Logger;
import reactor.util.Loggers;

public final class SingleStoreConnection implements com.singlestore.r2dbc.api.SingleStoreConnection {

  private final Logger logger = Loggers.getLogger(this.getClass());
  private final Client client;
  private final SingleStoreConnectionConfiguration configuration;
  private volatile String database;

  public SingleStoreConnection(
      Client client, SingleStoreConnectionConfiguration configuration) {
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
  public SingleStoreBatch createBatch() {
    return new SingleStoreBatch(this.client, this.configuration);
  }

  @Override
  public Mono<Void> createSavepoint(String name) {
    throw new UnsupportedOperationException("Savepoints are not supported in SingleStore");
  }

  @Override
  public SingleStoreStatement createStatement(String sql) {
    Assert.requireNonNull(sql, "sql must not be null");
    if (sql.trim().isEmpty()) {
      throw new IllegalArgumentException("Statement cannot be empty.");
    }

    if ((this.configuration.useServerPrepStmts() || sql.contains("call"))
        && !sql.startsWith("/*text*/")) {
      return new SingleStoreServerParameterizedQueryStatement(this.client, sql, this.configuration);
    }
    return new SingleStoreClientParameterizedQueryStatement(this.client, sql, this.configuration);
  }

  @Override
  public SingleStoreConnectionMetadata getMetadata() {
    return new SingleStoreConnectionMetadata(this.client.getVersion());
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

    String sql = String.format("SET lock_wait_timeout=%s", (long) Math.ceil(msValue / 1000.0));
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
    return this.database;
  }

  public Mono<Void> setDatabase(String database) {
    Assert.requireNonNull(database, "database must not be null");

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
