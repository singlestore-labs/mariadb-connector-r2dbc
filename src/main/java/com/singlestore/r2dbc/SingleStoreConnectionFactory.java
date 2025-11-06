// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc;

import com.singlestore.r2dbc.api.SingleStoreConnection;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import io.r2dbc.spi.*;
import java.net.SocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import com.singlestore.r2dbc.client.Client;
import com.singlestore.r2dbc.client.FailoverClient;
import com.singlestore.r2dbc.client.SingleStoreResult;
import com.singlestore.r2dbc.client.SimpleClient;
import com.singlestore.r2dbc.message.Protocol;
import com.singlestore.r2dbc.message.ServerMessage;
import com.singlestore.r2dbc.message.client.QueryPacket;
import com.singlestore.r2dbc.message.flow.AuthenticationFlow;
import com.singlestore.r2dbc.util.Assert;
import com.singlestore.r2dbc.util.HostAddress;
import reactor.core.publisher.Mono;
import reactor.netty.resources.ConnectionProvider;

public final class SingleStoreConnectionFactory implements ConnectionFactory {

  private final SingleStoreConnectionConfiguration configuration;

  public SingleStoreConnectionFactory(SingleStoreConnectionConfiguration configuration) {
    this.configuration = Assert.requireNonNull(configuration, "configuration must not be null");
  }

  public static SingleStoreConnectionFactory from(SingleStoreConnectionConfiguration configuration) {
    return new SingleStoreConnectionFactory(configuration);
  }

  private static Mono<Client> connectToSocket(
      final SingleStoreConnectionConfiguration configuration,
      SocketAddress endpoint,
      HostAddress hostAddress,
      ReentrantLock lock) {
    return SimpleClient.connect(
            ConnectionProvider.newConnection(), endpoint, hostAddress, configuration, lock)
        .delayUntil(client -> AuthenticationFlow.exchange(client, configuration, hostAddress))
        .flatMap(client -> retrieveSingleStoreVersion(configuration, client))
        .cast(Client.class)
        .flatMap(client -> setSessionVariables(configuration, client).thenReturn(client))
        .onErrorMap(e -> cannotConnect(e, endpoint));
  }

  public static Mono<SimpleClient> retrieveSingleStoreVersion(final SingleStoreConnectionConfiguration configuration, SimpleClient client) {
    return client.sendCommand(new QueryPacket("SELECT @@memsql_version"), true)
        .doOnDiscard(ReferenceCounted.class, ReferenceCountUtil::release)
        .windowUntil(ServerMessage::resultSetEnd)
        .map(
            dataRow ->
                new SingleStoreResult(
                    Protocol.TEXT,
                    null,
                    dataRow,
                    ExceptionFactory.INSTANCE,
                    null,
                    false,
                    configuration))
        .flatMap(result -> result.map((row, md) -> row.get(0, String.class)))
        .single()
        .doOnNext(client::setVersion)
        .thenReturn(client);
    }

  public static Mono<Void> setSessionVariables(
      final SingleStoreConnectionConfiguration configuration, Client client) {
    if (configuration.skipPostCommands()) return Mono.empty();
    // set default autocommit value
    StringBuilder sql = new StringBuilder("SET ");
    sql.append(" names UTF8MB4");
    if (configuration.autocommit() != null) {
      sql.append(",autocommit=").append((configuration.autocommit() ? "1" : "0"));
    }

    // set session variables if defined
    if (configuration.getSessionVariables() != null
        && configuration.getSessionVariables().size() > 0) {
      Map<String, Object> sessionVariable = configuration.getSessionVariables();
      Iterator<String> keys = sessionVariable.keySet().iterator();
      for (int i = 0; i < sessionVariable.size(); i++) {
        String key = keys.next();
        Object value = sessionVariable.get(key);
        if (value == null) {
          client.close().subscribe();
          return Mono.error(
              new R2dbcNonTransientResourceException(
                  String.format("Session variable '%s' has no value", key)));
        }
        sql.append(",").append(key).append("=");
        if (value instanceof String) {
          sql.append("'").append(value).append("'");
        } else if (value instanceof Integer
            || value instanceof Boolean
            || value instanceof Double) {
          sql.append(value);
        } else {
          client.close().subscribe();
          return Mono.error(
              new R2dbcNonTransientResourceException(
                  String.format(
                      "Session variable '%s' type can only be of type String, Integer, Double or"
                          + " Boolean",
                      key)));
        }
      }
    }

    return client
        .sendCommand(new QueryPacket(sql.toString()), true)
        .doOnDiscard(ReferenceCounted.class, ReferenceCountUtil::release)
        .windowUntil(ServerMessage::resultSetEnd)
        .map(
            dataRow ->
                new SingleStoreResult(
                    Protocol.TEXT,
                    null,
                    dataRow,
                    ExceptionFactory.INSTANCE,
                    null,
                    false,
                    configuration))
        .last()
        .then();
  }

  public static Mono<com.singlestore.r2dbc.SingleStoreConnection> closeWithError(Client client, Throwable throwable) {
    return client.close().then(Mono.error(throwable));
  }

  public static Throwable cannotConnect(Throwable throwable, SocketAddress endpoint) {

    if (throwable instanceof R2dbcException) {
      return throwable;
    }

    return new R2dbcNonTransientResourceException(
        String.format("Cannot connect to %s", endpoint), throwable);
  }

  @Override
  public Mono<SingleStoreConnection> create() {
    ReentrantLock lock = new ReentrantLock();
    return ((configuration.getSocket() != null)
            ? connectToSocket(
                configuration, new DomainSocketAddress(configuration.getSocket()), null, lock)
            : (configuration.getHaMode().equals(HaMode.NONE)
                ? configuration.getHaMode().connectHost(configuration, lock, false)
                : configuration
                    .getHaMode()
                    .connectHost(configuration, lock, false)
                    .flatMap(c -> Mono.just(new FailoverClient(configuration, lock, c)))))
        .flatMap(
            client ->
                Mono.just(
                        new com.singlestore.r2dbc.SingleStoreConnection(
                            client,
                            configuration))
                    .onErrorResume(throwable -> closeWithError(client, throwable)))
        .cast(SingleStoreConnection.class);
  }

  @Override
  public ConnectionFactoryMetadata getMetadata() {
    return SingleStoreConnectionFactoryMetadata.INSTANCE;
  }

  @Override
  public String toString() {
    return "SingleStoreConnectionFactory{configuration=" + this.configuration + '}';
  }
}
