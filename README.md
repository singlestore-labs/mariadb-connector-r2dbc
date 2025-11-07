<p align="center">
	<a href="https://www.singlestore.com/">
		<img src="logo.png">
	</a>
</p>

# SingleStore R2DBC connector

[![Maven Central][maven-image]][maven-url]
[![CI Tests](https://github.com/singlestore-labs/singlestore-connector-r2dbc/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/singlestore-labs/singlestore-connector-r2dbc/actions/workflows/ci.yml)
[![License][license-image]][license-url]

**Non-blocking SingleStore client.**

SingleStore client, 100% Java, compatible with Java8+, apache 2.0 licensed.
Driver permits PAM authentication that comes with SingleStore.

Driver follows [R2DBC 1.0.0 specifications](https://r2dbc.io/spec/1.0.0.RELEASE/spec/html/)

## Quick Start

The SingleStore Connector is available through maven using :

```
		<dependency>
				<groupId>com.singlestore</groupId>
				<artifactId>r2dbc-singlestore</artifactId>
				<version>0.0.1-beta</version>
		</dependency>
```

Factory can be created using ConnectionFactory or using connection URL.

Using builder

```java

SingleStoreConnectionConfiguration conf = SingleStoreConnectionConfiguration.builder()
						.host("localhost")
						.port(3306)
						.username("myUser")
						.password("MySuperPassword")
						.database("db")
						.build();
SingleStoreConnectionFactory factory = new SingleStoreConnectionFactory(conf);

//OR

ConnectionFactory factory = ConnectionFactories.get("r2dbc:singlestore://user:password@host:3306,host2:3302/myDB?option1=value");
```

Basic example:

```java
		SingleStoreConnectionConfiguration conf = SingleStoreConnectionConfiguration.builder()
						.host("localhost")
						.port(3306)
						.username("myUser")
						.password("MySuperPassword")
						.database("db")
						.build();
		SingleStoreConnectionFactory factory = new SingleStoreConnectionFactory(conf);

		SingleStoreConnection connection = factory.create().block();
		connection.createStatement("SELECT * FROM myTable WHERE val = ?")
						.bind(0, "myVal") // setting parameter
						.execute()
						.flatMap(r -> r.map((row, metadata) -> {
							return "value=" + row.get(0, String.class);
						}));
		connection.close().subscribe();
```

### Connection options

|                                     option | description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |                type                |       default       |
|-------------------------------------------:|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:----------------------------------:|:-------------------:|
|                             **`username`** | User to access database.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |              *string*              |
|                             **`password`** | User password.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |              *string*              |
|                                 **`host`** | IP address or DNS of the database server. Multiple host can be set, separate by comma. If first host is not reachable (timeout is connectTimeout), driver use next hosts.*Not used when using option `socket`*.                                                                                                                                                                                                                                                                                          |              *string*              |     "localhost"     |
|                                 **`port`** | Database server port number. *Not used when using option `socket`*                                                                                                                                                                                                                                                                                                                                                                                                                                       |             *integer*              |        3306         |
|                             **`database`** | Default database to use when establishing the connection.                                                                                                                                                                                                                                                                                                                                                                                                                                                |              *string*              |
|                       **`connectTimeout`** | Sets the connection timeout                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |             *Duration*             |         10s         |
|                         **`tcpKeepAlive`** | Sets socket keep alive                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |             *Boolean*              |        false        |
|                     **`tcpAbortiveClose`** | This option can be used in environments where connections are created and closed in rapid succession. Often, it is not possible to create a socket in such an environment after a while, since all local “ephemeral” ports are used up by TCP connections in TCP_WAIT state. Using tcpAbortiveClose works around this problem by resetting TCP connections (abortive or hard close) rather than doing an orderly close.                                                                                  |             *boolean*              |        false        |
|                               **`socket`** | Permits connections to the database through the Unix domain socket for faster connection whe server is local.                                                                                                                                                                                                                                                                                                                                                                                            |              *string*              |
|                    **`allowMultiQueries`** | Allows you to issue several SQL statements in a single call. (That is, `INSERT INTO a VALUES('b'); INSERT INTO c VALUES('d');`).  <br/><br/>This may be a **security risk** as it allows for SQL Injection attacks.                                                                                                                                                                                                                                                                                      |             *boolean*              |        false        |
|                 **`connectionAttributes`** | Permit to send server some client information. <br>Those information can be retrieved on server within tables information_schema.mv_connection_attributes. This can permit from server an identification of client/application per connection                                                                                                                                                                                                                                                            |        *Map<String,String>*        |
|                     **`sessionVariables`** | Permits to set session variables upon successful connection                                                                                                                                                                                                                                                                                                                                                                                                                                              |        *Map<String,String>*        |
|                          **`tlsProtocol`** | Force TLS/SSL protocol to a specific set of TLS versions (example "TLSv1.2", "TLSv1.3").                                                                                                                                                                                                                                                                                                                                                                                                                 |           *List<String>*           | <i>java default</i> |
|                        **`serverSslCert`** | Permits providing server's certificate in DER form, or server's CA certificate. <br/>This permits a self-signed certificate to be trusted. Can be used in one of 3 forms : <ul><li> serverSslCert=/path/to/cert.pem (full path to certificate)</li><li> serverSslCert=classpath:relative/cert.pem (relative to current classpath)</li><li> as verbatim DER-encoded certificate string "------BEGIN CERTIFICATE-----"</li></ul>                                                                           |              *String*              |                     |
|                        **`clientSslCert`** | Permits providing client's certificate in DER form (use only for mutual authentication). Can be used in one of 3 forms : <ul><li>clientSslCert=/path/to/cert.pem (full path to certificate)</li><li> clientSslCert=classpath:relative/cert.pem (relative to current classpath)</li><li> as verbatim DER-encoded certificate string "------BEGIN CERTIFICATE-----"</li></ul>                                                                                                                              |              *String*              |                     |
|                         **`clientSslKey`** | client private key path(for mutual authentication)                                                                                                                                                                                                                                                                                                                                                                                                                                                       |              *String*              |                     |
|                    **`clientSslPassword`** | client private key password                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |           *charsequence*           |                     |
|                              **`sslMode`** | ssl requirement. Possible value are <ul><li>DISABLE, // NO SSL</li><li>TRUST, // Encryption, but no certificate and hostname validation  (DEVELOPMENT ONLY)</li><li>VERIFY_CA, // Encryption, certificates validation, BUT no hostname validation</li><li>VERIFY_FULL, // Standard SSL use: Encryption, certificate validation and hostname validation</li><li>TUNNEL, // Connect over a pre-created SSL tunnel. See sslContextBuilderCustomizer options and sslTunnelDisableHostVerification </li></ul> |              SslMode               |       DISABLE       |
|                   **`useServerPrepStmts`** | Permit to indicate to use text or binary protocol for query with parameter                                                                                                                                                                                                                                                                                                                                                                                                                               |             *boolean*              |        false        |
|                     **`prepareCacheSize`** | if useServerPrepStmts = true, cache the prepared informations in a LRU cache to avoid re-preparation of command. Next use of that command, only prepared identifier and parameters (if any) will be sent to server. This mainly permit for server to avoid reparsing query.                                                                                                                                                                                                                              |               *int*                |         256         |
|                          **`pamOtherPwd`** | Permit to provide additional password for PAM authentication with multiple authentication step. If multiple passwords, value must be URL encoded.                                                                                                                                                                                                                                                                                                                                                        |              *string*              |                     |
|                           **`autocommit`** | Set default autocommit value on connection initialization"                                                                                                                                                                                                                                                                                                                                                                                                                                               |             *Boolean*              |        true         |
|                        **`loopResources`** | permits to share netty EventLoopGroup among multiple async libraries/framework                                                                                                                                                                                                                                                                                                                                                                                                                           |          *LoopResources*           |                     |
|          **`sslContextBuilderCustomizer`** | Permits to customized SSL context builder.                                                                                                                                                                                                                                                                                                                                                                                                                                                               | *UnaryOperator<SslContextBuilder>* |                     |
|     **`sslTunnelDisableHostVerification`** | Disable hostname verification during SSLHandshake when SslMode.TUNNEL is set                                                                                                                                                                                                                                                                                                                                                                                                                             |             *boolean*              |                     |
|                    **`skipPostCommands`**  | Permit to indicate that commands after connections must be skipped. This permit to avoid unnecessary command on connection creation, and when using RDV proxy not to have session pinning. Use with care, because connector expects server to have : 1.connection exchanges to be UT8(mb3/mb4). 2.autocommit set to true.                                                                                                                                                                                |             *boolean*              |       'false'       |


## Failover

Failover occurs when a connection to a primary database server fails and the connector opens up a connection to another
database server.
For example, server A has the current connection. After a failure (server crash, network down …) the connection will
switch to another server (B).

Load balancing allows load to be distributed over multiple servers:
When initializing a connection or after a failed connection, the connector will attempt to connect to a host. The
connection is selected randomly among the valid hosts. Thereafter, all statements will run on that database server until
the connection will be closed (or fails).
Example: when creating a pool of 60 connections, each one will use a random host. With 3 master hosts, the pool will
have about 20 connections to each host.

```java
ConnectionFactory factory = ConnectionFactories.get("r2dbc:singlestore:sequential://user:password@host:3306,host2:3302/myDB?option1=value");
```

### Failover behaviour

Failover parameter is set (i.e. prefixing connection string with `r2dbc:singlestore:[sequential|loadbalancing]://...` or
using HaMode builder).

There can be multiple fail causes. When a failure occurs many things will be done:

* connection recovery (re-establishing connection transparently)
* re-execute command/transaction if possible

During failover, the fail host address will be put on a blacklist (shared by JVM) for 60 seconds. Connector will always
try to connect non blacklisted host first, but can retry to connect blacklisted host before 60s if all hosts are
blacklisted.

### re-execution

The driver will try to reconnect to any valid host (not blasklisted, or if all primary host are blacklisted trying
blacklisted hosts). If reconnection fail, an Exception with be thrown with SQLState "08XXX". If using a pool, this
connection will be discarded.

on successful reconnection, there will be different cases.

If driver identify that command can be replayed without issue (for example connection.isValid(), a PREPARE/ROLLBACK
command), driver will execute command without throwing any error.

Driver cannot transparently handle all cases: imagine that the failover occurs when executing an INSERT command without
a transaction: driver cannot know that command has been received and executed on server. In those case, an SQLException
with be thrown with SQLState "25S03".

#### Option `transactionReplay` :

Most of the time, queries occurs in transaction (ORM for example doesn't permit using auto-commit), so redo transaction
implementation will solve most of failover cases transparently for user point of view.

Redo transaction approach is to save commands in transaction. When a failover occurs during a transaction, the connector
can automatically reconnect and replay transaction, making failover completely transparent.

There is some limitations.
 - Driver will buffer up commands in a transaction until some inner limit.
 - Huge command will temporarily disable transaction buffering for current transaction.
 - Commands must be idempotent only (queries can be "replayable")


[maven-image]:https://img.shields.io/maven-central/v/com.singlestore/r2dbc-singlestore.svg

[maven-url]:https://maven-badges.herokuapp.com/maven-central/com.singlestore/r2dbc-singlestore

[license-image]:https://img.shields.io/badge/License-Apache%202.0-blue.svg

[license-url]:https://opensource.org/licenses/Apache-2.0
