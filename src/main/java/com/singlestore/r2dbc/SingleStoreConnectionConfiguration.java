// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;

import io.netty.handler.ssl.SslContextBuilder;
import io.r2dbc.spi.ConnectionFactoryOptions;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.function.UnaryOperator;
import com.singlestore.r2dbc.util.Assert;
import com.singlestore.r2dbc.util.HostAddress;
import com.singlestore.r2dbc.util.Security;
import com.singlestore.r2dbc.util.SslConfig;
import reactor.netty.resources.LoopResources;
import reactor.netty.tcp.TcpResources;
import reactor.util.annotation.Nullable;

public final class SingleStoreConnectionConfiguration {

  public static final int DEFAULT_PORT = 3306;
  private final String database;
  private final List<HostAddress> hostAddresses;
  private final HaMode haMode;
  private final Duration connectTimeout;
  private final boolean tcpKeepAlive;
  private final boolean tcpAbortiveClose;
  private final boolean transactionReplay;
  private final CharSequence password;
  private final CharSequence[] pamOtherPwd;
  private final int port;
  private final int prepareCacheSize;
  private final String socket;
  private final String username;
  private final boolean allowMultiQueries;
  private final Map<String, String> connectionAttributes;
  private final Map<String, Object> sessionVariables;
  private final SslConfig sslConfig;
  private final boolean useServerPrepStmts;
  private final Boolean autocommit;
  private final String[] restrictedAuth;
  private final LoopResources loopResources;
  private final UnaryOperator<SslContextBuilder> sslContextBuilderCustomizer;
  private final boolean skipPostCommands;

  private SingleStoreConnectionConfiguration(
      String haMode,
      @Nullable Duration connectTimeout,
      @Nullable Boolean tcpKeepAlive,
      @Nullable Boolean tcpAbortiveClose,
      @Nullable Boolean transactionReplay,
      @Nullable String database,
      @Nullable String host,
      @Nullable Map<String, String> connectionAttributes,
      @Nullable Map<String, Object> sessionVariables,
      @Nullable CharSequence password,
      int port,
      @Nullable List<HostAddress> hostAddresses,
      @Nullable String socket,
      @Nullable String username,
      boolean allowMultiQueries,
      @Nullable List<String> tlsProtocol,
      @Nullable String serverSslCert,
      @Nullable String clientSslCert,
      @Nullable String clientSslKey,
      @Nullable CharSequence clientSslPassword,
      SslMode sslMode,
      boolean useServerPrepStmts,
      Boolean autocommit,
      boolean skipPostCommands,
      @Nullable Integer prepareCacheSize,
      @Nullable CharSequence[] pamOtherPwd,
      String restrictedAuth,
      @Nullable LoopResources loopResources,
      @Nullable UnaryOperator<SslContextBuilder> sslContextBuilderCustomizer,
      boolean sslTunnelDisableHostVerification) {
    this.haMode = haMode == null ? HaMode.NONE : HaMode.from(haMode);
    this.connectTimeout = connectTimeout == null ? Duration.ofSeconds(10) : connectTimeout;
    this.tcpKeepAlive = tcpKeepAlive == null ? Boolean.FALSE : tcpKeepAlive;
    this.tcpAbortiveClose = tcpAbortiveClose == null ? Boolean.FALSE : tcpAbortiveClose;
    this.transactionReplay = transactionReplay == null ? Boolean.FALSE : transactionReplay;
    this.database = database != null && !database.isEmpty() ? database : null;
    this.restrictedAuth = restrictedAuth != null ? restrictedAuth.split(",") : null;
    if (hostAddresses != null) {
      this.hostAddresses = hostAddresses;
    } else {
      this.hostAddresses = HostAddress.parse(host, port);
    }
    this.connectionAttributes = connectionAttributes;
    this.sessionVariables = sessionVariables;
    this.password = password != null && !password.toString().isEmpty() ? password : null;
    this.port = port;
    this.socket = socket;
    this.username = username;
    this.allowMultiQueries = allowMultiQueries;
    if (sslMode == SslMode.DISABLE) {
      this.sslConfig = SslConfig.DISABLE_INSTANCE;
    } else {
      this.sslConfig =
          new SslConfig(
              sslMode,
              serverSslCert,
              clientSslCert,
              clientSslKey,
              clientSslPassword,
              tlsProtocol,
              sslTunnelDisableHostVerification,
              sslContextBuilderCustomizer);
    }
    this.prepareCacheSize = (prepareCacheSize == null) ? 250 : prepareCacheSize;
    this.pamOtherPwd = pamOtherPwd;
    this.autocommit = (autocommit != null) ? autocommit : Boolean.TRUE;
    this.skipPostCommands = skipPostCommands;
    this.loopResources = loopResources != null ? loopResources : TcpResources.get();
    this.useServerPrepStmts = !this.allowMultiQueries && useServerPrepStmts;
    this.sslContextBuilderCustomizer = sslContextBuilderCustomizer;
  }

  private SingleStoreConnectionConfiguration(
      String database,
      List<HostAddress> hostAddresses,
      HaMode haMode,
      Duration connectTimeout,
      boolean tcpKeepAlive,
      boolean tcpAbortiveClose,
      boolean transactionReplay,
      CharSequence password,
      CharSequence[] pamOtherPwd,
      int port,
      int prepareCacheSize,
      String socket,
      String username,
      boolean allowMultiQueries,
      Map<String, String> connectionAttributes,
      Map<String, Object> sessionVariables,
      SslConfig sslConfig,
      boolean useServerPrepStmts,
      Boolean autocommit,
      boolean skipPostCommands,
      String[] restrictedAuth,
      LoopResources loopResources,
      UnaryOperator<SslContextBuilder> sslContextBuilderCustomizer) {
    this.database = database;
    this.hostAddresses = hostAddresses;
    this.haMode = haMode;
    this.connectTimeout = connectTimeout;
    this.tcpKeepAlive = tcpKeepAlive;
    this.tcpAbortiveClose = tcpAbortiveClose;
    this.transactionReplay = transactionReplay;
    this.password = password;
    this.pamOtherPwd = pamOtherPwd;
    this.port = port;
    this.prepareCacheSize = prepareCacheSize;
    this.socket = socket;
    this.username = username;
    this.allowMultiQueries = allowMultiQueries;
    this.connectionAttributes = connectionAttributes;
    this.sessionVariables = sessionVariables;
    this.sslConfig = sslConfig;
    this.useServerPrepStmts = useServerPrepStmts;
    this.autocommit = (autocommit != null) ? autocommit : Boolean.TRUE;
    this.skipPostCommands = skipPostCommands;
    this.restrictedAuth = restrictedAuth;
    this.loopResources = loopResources;
    this.sslContextBuilderCustomizer = sslContextBuilderCustomizer;
  }

  static boolean boolValue(Object value) {
    if (value instanceof Boolean) {
      return (Boolean) value;
    }
    return Boolean.parseBoolean(value.toString()) || "1".equals(value);
  }

  static Duration durationValue(Object value) {
    if (value instanceof Duration) {
      return ((Duration) value);
    }
    return Duration.parse(value.toString());
  }

  static int intValue(Object value) {
    if (value instanceof Number) {
      return ((Number) value).intValue();
    }
    return Integer.parseInt(value.toString());
  }

  @SuppressWarnings("unchecked")
  public static Builder fromOptions(ConnectionFactoryOptions connectionFactoryOptions) {
    Builder builder = new Builder();
    builder.database((String) connectionFactoryOptions.getValue(DATABASE));

    if (connectionFactoryOptions.hasOption(SingleStoreConnectionFactoryProvider.SOCKET)) {
      builder.socket(
          (String)
              connectionFactoryOptions.getRequiredValue(SingleStoreConnectionFactoryProvider.SOCKET));
    } else {
      builder.host((String) connectionFactoryOptions.getRequiredValue(HOST));
    }

    if (connectionFactoryOptions.hasOption(SingleStoreConnectionFactoryProvider.ALLOW_MULTI_QUERIES)) {
      builder.allowMultiQueries(
          boolValue(
              connectionFactoryOptions.getValue(
                  SingleStoreConnectionFactoryProvider.ALLOW_MULTI_QUERIES)));
    }

    if (connectionFactoryOptions.hasOption(ConnectionFactoryOptions.CONNECT_TIMEOUT)) {
      builder.connectTimeout(
          durationValue(
              connectionFactoryOptions.getValue(ConnectionFactoryOptions.CONNECT_TIMEOUT)));
    }

    if (connectionFactoryOptions.hasOption(SingleStoreConnectionFactoryProvider.TCP_KEEP_ALIVE)) {
      builder.tcpKeepAlive(
          boolValue(
              connectionFactoryOptions.getValue(SingleStoreConnectionFactoryProvider.TCP_KEEP_ALIVE)));
    }

    if (connectionFactoryOptions.hasOption(SingleStoreConnectionFactoryProvider.TCP_ABORTIVE_CLOSE)) {
      builder.tcpAbortiveClose(
          boolValue(
              connectionFactoryOptions.getValue(
                  SingleStoreConnectionFactoryProvider.TCP_ABORTIVE_CLOSE)));
    }

    if (connectionFactoryOptions.hasOption(SingleStoreConnectionFactoryProvider.TRANSACTION_REPLAY)) {
      builder.transactionReplay(
          boolValue(
              connectionFactoryOptions.getValue(
                  SingleStoreConnectionFactoryProvider.TRANSACTION_REPLAY)));
    }

    if (connectionFactoryOptions.hasOption(SingleStoreConnectionFactoryProvider.SESSION_VARIABLES)) {
      String sessionVarString =
          (String)
              connectionFactoryOptions.getValue(SingleStoreConnectionFactoryProvider.SESSION_VARIABLES);
      builder.sessionVariables(Security.parseSessionVariables(sessionVarString));
    }

    if (connectionFactoryOptions.hasOption(SingleStoreConnectionFactoryProvider.HAMODE)) {
      String haMode =
          (String) connectionFactoryOptions.getValue(SingleStoreConnectionFactoryProvider.HAMODE);
      builder.haMode(haMode);
    }

    if (connectionFactoryOptions.hasOption(SingleStoreConnectionFactoryProvider.USE_SERVER_PREPARE)) {
      builder.useServerPrepStmts(
          boolValue(
              connectionFactoryOptions.getValue(
                  SingleStoreConnectionFactoryProvider.USE_SERVER_PREPARE)));
    }

    if (connectionFactoryOptions.hasOption(SingleStoreConnectionFactoryProvider.AUTO_COMMIT)) {
      Object value =
          connectionFactoryOptions.getValue(SingleStoreConnectionFactoryProvider.AUTO_COMMIT);
      if (value == null) {
        builder.autocommit(null);
      } else if (value instanceof Boolean) {
        builder.autocommit((Boolean) value);
      } else {
        builder.autocommit(Boolean.parseBoolean(value.toString()) || "1".equals(value));
      }
    }

    if (connectionFactoryOptions.hasOption(SingleStoreConnectionFactoryProvider.SKIP_POST_COMMANDS)) {
      builder.skipPostCommands(
          boolValue(
              connectionFactoryOptions.getValue(
                  SingleStoreConnectionFactoryProvider.SKIP_POST_COMMANDS)));
    }

    if (connectionFactoryOptions.hasOption(
        SingleStoreConnectionFactoryProvider.CONNECTION_ATTRIBUTES)) {
      String connAttributes =
          (String)
              connectionFactoryOptions.getValue(
                  SingleStoreConnectionFactoryProvider.CONNECTION_ATTRIBUTES);
      builder.connectionAttributes(getMapFromString(connAttributes));
    }

    if (connectionFactoryOptions.hasOption(SingleStoreConnectionFactoryProvider.PREPARE_CACHE_SIZE)) {
      builder.prepareCacheSize(
          intValue(
              connectionFactoryOptions.getValue(
                  SingleStoreConnectionFactoryProvider.PREPARE_CACHE_SIZE)));
    }

    if (connectionFactoryOptions.hasOption(SingleStoreConnectionFactoryProvider.SSL_MODE)) {
      builder.sslMode(
          SslMode.from(
              (String)
                  connectionFactoryOptions.getValue(SingleStoreConnectionFactoryProvider.SSL_MODE)));
    }
    builder.serverSslCert(
        (String)
            connectionFactoryOptions.getValue(SingleStoreConnectionFactoryProvider.SERVER_SSL_CERT));
    builder.clientSslCert(
        (String)
            connectionFactoryOptions.getValue(SingleStoreConnectionFactoryProvider.CLIENT_SSL_CERT));
    builder.clientSslKey(
        (String)
            connectionFactoryOptions.getValue(SingleStoreConnectionFactoryProvider.CLIENT_SSL_KEY));
    builder.clientSslPassword(
        (String)
            connectionFactoryOptions.getValue(SingleStoreConnectionFactoryProvider.CLIENT_SSL_PWD));

    if (connectionFactoryOptions.hasOption(SingleStoreConnectionFactoryProvider.TLS_PROTOCOL)) {
      String[] protocols =
          ((String)
                  connectionFactoryOptions.getValue(SingleStoreConnectionFactoryProvider.TLS_PROTOCOL))
              .split("[,;\\s]+");
      builder.tlsProtocol(protocols);
    }
    if (connectionFactoryOptions.hasOption(ConnectionFactoryOptions.PROTOCOL)) {
      builder.haMode((String) connectionFactoryOptions.getValue(ConnectionFactoryOptions.PROTOCOL));
    }
    builder.password((CharSequence) connectionFactoryOptions.getValue(PASSWORD));

    builder.username((String) connectionFactoryOptions.getRequiredValue(USER));
    if (connectionFactoryOptions.hasOption(PORT)) {
      builder.port(intValue(connectionFactoryOptions.getValue(PORT)));
    }
    if (connectionFactoryOptions.hasOption(SingleStoreConnectionFactoryProvider.PAM_OTHER_PASSWORD)) {
      String s =
          (String)
              connectionFactoryOptions.getValue(
                  SingleStoreConnectionFactoryProvider.PAM_OTHER_PASSWORD);
      String[] pairs = s.split(",");
      try {
        for (int i = 0; i < pairs.length; i++) {
          pairs[i] = URLDecoder.decode(pairs[i], StandardCharsets.UTF_8.toString());
        }
      } catch (UnsupportedEncodingException e) {
        // eat, StandardCharsets.UTF_8 is always supported
      }
      builder.pamOtherPwd(pairs);
    }

    if (connectionFactoryOptions.hasOption(SingleStoreConnectionFactoryProvider.RESTRICTED_AUTH)) {
      builder.restrictedAuth(
          (String)
              connectionFactoryOptions.getValue(SingleStoreConnectionFactoryProvider.RESTRICTED_AUTH));
    }

    if (connectionFactoryOptions.hasOption(SingleStoreConnectionFactoryProvider.LOOP_RESOURCES)) {
      LoopResources loopResources =
          (LoopResources)
              connectionFactoryOptions.getValue(SingleStoreConnectionFactoryProvider.LOOP_RESOURCES);
      builder.loopResources(loopResources);
    }

    if (connectionFactoryOptions.hasOption(
        SingleStoreConnectionFactoryProvider.SSL_TUNNEL_DISABLE_HOST_VERIFICATION)) {
      builder.sslTunnelDisableHostVerification(
          boolValue(
              connectionFactoryOptions.getValue(
                  SingleStoreConnectionFactoryProvider.SSL_TUNNEL_DISABLE_HOST_VERIFICATION)));
    }

    if (connectionFactoryOptions.hasOption(
        SingleStoreConnectionFactoryProvider.SSL_CONTEXT_BUILDER_CUSTOMIZER)) {
      builder.sslContextBuilderCustomizer(
          (UnaryOperator<SslContextBuilder>)
              connectionFactoryOptions.getValue(
                  SingleStoreConnectionFactoryProvider.SSL_CONTEXT_BUILDER_CUSTOMIZER));
    }

    return builder;
  }

  private static Map<String, String> getMapFromString(String s) {
    Map<String, String> map = new HashMap<>();
    if (s != null && !s.isEmpty()) {
      String[] pairs = s.split(",");
      for (String pair : pairs) {
        String[] keyValue = pair.split("=");
        map.put(keyValue[0], (keyValue.length > 1) ? keyValue[1] : "");
      }
    }
    return map;
  }

  public static Builder builder() {
    return new Builder();
  }

  @Nullable
  public Duration getConnectTimeout() {
    return this.connectTimeout;
  }

  public CharSequence[] getPamOtherPwd() {
    return pamOtherPwd;
  }

  @Nullable
  public String getDatabase() {
    return this.database;
  }

  public HaMode getHaMode() {
    return this.haMode;
  }

  @Nullable
  public List<HostAddress> getHostAddresses() {
    return this.hostAddresses;
  }

  @Nullable
  public Map<String, String> getConnectionAttributes() {
    return this.connectionAttributes;
  }

  @Nullable
  public Map<String, Object> getSessionVariables() {
    return this.sessionVariables;
  }

  @Nullable
  public CharSequence getPassword() {
    return this.password;
  }

  public int getPort() {
    return this.port;
  }

  @Nullable
  public String getSocket() {
    return this.socket;
  }

  public String getUsername() {
    return this.username;
  }

  public boolean allowMultiQueries() {
    return allowMultiQueries;
  }

  public SslConfig getSslConfig() {
    return sslConfig;
  }

  public boolean useServerPrepStmts() {
    return useServerPrepStmts;
  }

  public Boolean autocommit() {
    return autocommit;
  }

  public boolean skipPostCommands() {
    return skipPostCommands;
  }

  public int getPrepareCacheSize() {
    return prepareCacheSize;
  }

  public boolean isTcpKeepAlive() {
    return tcpKeepAlive;
  }

  public boolean isTcpAbortiveClose() {
    return tcpAbortiveClose;
  }

  public boolean isTransactionReplay() {
    return transactionReplay;
  }

  public String[] getRestrictedAuth() {
    return restrictedAuth;
  }

  public LoopResources loopResources() {
    return loopResources;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public String toString() {

    SingleStoreConnectionConfiguration defaultConf = new Builder().build(false);
    StringBuilder sb = new StringBuilder();
    sb.append("r2dbc:singlestore:");
    if (this.haMode != HaMode.NONE) {
      sb.append(this.haMode.toString().toLowerCase(Locale.ROOT)).append(":");
    }
    sb.append("//");
    for (int i = 0; i < this.getHostAddresses().size(); i++) {
      HostAddress hostAddress = this.getHostAddresses().get(i);
      if (i > 0) {
        sb.append(",");
      }
      sb.append(hostAddress.getHost());
      if (hostAddress.getPort() != 3306) sb.append(":").append(hostAddress.getPort());
    }

    sb.append("/");
    if (this.database != null) {
      sb.append(this.database);
    }

    try {
      // Option object is already initialized to default values.
      // loop on properties,
      // - check DefaultOption to check that property value correspond to type (and range)
      // - set values
      boolean first = true;

      Field[] fields = SingleStoreConnectionConfiguration.class.getDeclaredFields();
      for (Field field : fields) {
        if ("database".equals(field.getName())
            || "haMode".equals(field.getName())
            || "$jacocoData".equals(field.getName())
            || "addresses".equals(field.getName())
            || "sslContextBuilderCustomizer".equals(field.getName())
            || "loopResources".equals(field.getName())
            || "hostAddresses".equals(field.getName())
            || "port".equals(field.getName())) {
          continue;
        }
        Object obj = field.get(this);

        if (obj != null && (!(obj instanceof Properties) || ((Properties) obj).size() > 0)) {

          if ("password".equals(field.getName())) {
            sb.append(first ? '?' : '&');
            first = false;
            sb.append(field.getName()).append('=');
            sb.append("***");
            continue;
          }

          if (field.getType().equals(String.class)) {
            String defaultValue = (String) field.get(defaultConf);
            if (!obj.equals(defaultValue)) {
              sb.append(first ? '?' : '&');
              first = false;
              sb.append(field.getName()).append('=');
              sb.append((String) obj);
            }
          } else if (field.getType().equals(SslConfig.class)) {
            String sslConfigString = ((SslConfig) obj).toString();
            if (!sslConfigString.isEmpty()) {
              sb.append(first ? '?' : '&');
              first = false;
              sb.append(sslConfigString);
            }
          } else if (field.getType().equals(boolean.class)) {
            boolean defaultValue = field.getBoolean(defaultConf);
            if (!obj.equals(defaultValue)) {
              sb.append(first ? '?' : '&');
              first = false;
              sb.append(field.getName()).append('=');
              sb.append(obj);
            }
          } else if (field.getType().equals(int.class)) {
            try {
              int defaultValue = field.getInt(defaultConf);
              if (!obj.equals(defaultValue)) {
                sb.append(first ? '?' : '&');
                sb.append(field.getName()).append('=').append(obj);
                first = false;
              }
            } catch (IllegalAccessException n) {
              // eat
            }
          } else if (field.getType().equals(Properties.class)) {
            sb.append(first ? '?' : '&');
            first = false;
            boolean firstProp = true;
            Properties properties = (Properties) obj;
            for (Object key : properties.keySet()) {
              if (firstProp) {
                firstProp = false;
              } else {
                sb.append('&');
              }
              sb.append(key).append('=');
              sb.append(properties.get(key));
            }
          } else if (field.getType().isArray()) {
            Object defaultValue = field.get(defaultConf);
            if (field.getType().getComponentType().equals(CharSequence.class)) {
              if (obj != null) obj = String.join(",", ((CharSequence[]) obj));
            }
            if (field.getType().getComponentType().equals(String.class)) {
              if (obj != null) obj = String.join(",", ((String[]) obj));
            }
            if (!obj.equals(defaultValue)) {
              sb.append(first ? '?' : '&');
              first = false;
              sb.append(field.getName()).append('=');
              sb.append(obj);
            }
          } else if (field.getType().equals(Map.class)) {
            Object defaultValue = field.get(defaultConf);
            if (!obj.equals(defaultValue)) {
              sb.append(first ? '?' : '&');
              first = false;
              sb.append(field.getName()).append("=");
              Map objMap = (Map) obj;
              boolean firstMapEntry = true;
              for (Object entry : objMap.entrySet()) {
                if (!firstMapEntry) sb.append(',');
                sb.append(((Map.Entry) entry).getKey())
                    .append("=")
                    .append(((Map.Entry) entry).getValue());
                firstMapEntry = false;
              }
            }
          } else {
            Object defaultValue = field.get(defaultConf);
            if (!obj.equals(defaultValue)) {
              sb.append(first ? '?' : '&');
              first = false;
              sb.append(field.getName()).append('=');
              sb.append(obj);
            }
          }
        }
      }

    } catch (IllegalAccessException n) {
      n.printStackTrace();
    } catch (SecurityException s) {
      // only for jws, so never thrown
      throw new IllegalArgumentException("Security too restrictive : " + s.getMessage());
    }
    return sb.toString();
  }

  /**
   * A builder for {@link SingleStoreConnectionConfiguration} instances.
   *
   * <p><i>This class is not threadsafe</i>
   */
  public static final class Builder implements Cloneable {

    @Nullable Integer prepareCacheSize;
    @Nullable private String haMode;
    @Nullable private String username;
    @Nullable private Duration connectTimeout;
    @Nullable private Boolean tcpKeepAlive;
    @Nullable private Boolean tcpAbortiveClose;
    @Nullable private Boolean transactionReplay;
    @Nullable private String database;
    @Nullable private List<HostAddress> hostAddresses;
    @Nullable private String host;
    @Nullable private Map<String, Object> sessionVariables;
    @Nullable private Map<String, String> connectionAttributes;
    @Nullable private CharSequence password;
    private int port = DEFAULT_PORT;
    @Nullable private String socket;
    private boolean allowMultiQueries = false;
    private boolean useServerPrepStmts = false;
    private Boolean autocommit = Boolean.TRUE;
    private boolean skipPostCommands = false;
    @Nullable private List<String> tlsProtocol;
    @Nullable private String serverSslCert;
    @Nullable private String clientSslCert;
    @Nullable private String clientSslKey;
    @Nullable private CharSequence clientSslPassword;
    private SslMode sslMode = SslMode.DISABLE;
    private CharSequence[] pamOtherPwd;
    private String restrictedAuth;
    @Nullable private LoopResources loopResources;
    @Nullable private UnaryOperator<SslContextBuilder> sslContextBuilderCustomizer;
    private boolean sslTunnelDisableHostVerification;

    private Builder() {}

    /**
     * Returns a configured {@link SingleStoreConnectionConfiguration}.
     *
     * @return a configured {@link SingleStoreConnectionConfiguration}
     */
    public SingleStoreConnectionConfiguration build() {
      return build(true);
    }

    private SingleStoreConnectionConfiguration build(boolean checkMandatory) {
      if (checkMandatory) {
        if (this.host == null && this.socket == null) {
          throw new IllegalArgumentException("host or socket must not be null");
        }

        if (this.host != null && this.socket != null) {
          throw new IllegalArgumentException(
              "Connection must be configured for either host/port or socket usage but not both");
        }

        if (this.username == null) {
          throw new IllegalArgumentException("username must not be null");
        }
      }
      return new SingleStoreConnectionConfiguration(
          this.haMode,
          this.connectTimeout,
          this.tcpKeepAlive,
          this.tcpAbortiveClose,
          this.transactionReplay,
          this.database,
          this.host,
          this.connectionAttributes,
          this.sessionVariables,
          this.password,
          this.port,
          this.hostAddresses,
          this.socket,
          this.username,
          this.allowMultiQueries,
          this.tlsProtocol,
          this.serverSslCert,
          this.clientSslCert,
          this.clientSslKey,
          this.clientSslPassword,
          this.sslMode,
          this.useServerPrepStmts,
          this.autocommit,
          this.skipPostCommands,
          this.prepareCacheSize,
          this.pamOtherPwd,
          this.restrictedAuth,
          this.loopResources,
          this.sslContextBuilderCustomizer,
          this.sslTunnelDisableHostVerification);
    }

    /**
     * Configures the connection timeout. Default unconfigured.
     *
     * @param connectTimeout the connection timeout
     * @return this {@link Builder}
     */
    public Builder connectTimeout(@Nullable Duration connectTimeout) {
      this.connectTimeout = connectTimeout;
      return this;
    }

    public Builder haMode(@Nullable String haMode) {
      this.haMode = haMode;
      return this;
    }

    public Builder hostAddresses(@Nullable List<HostAddress> hostAddresses) {
      this.hostAddresses = hostAddresses;
      return this;
    }

    public Builder restrictedAuth(@Nullable String restrictedAuth) {
      this.restrictedAuth = restrictedAuth;
      return this;
    }

    public Builder tcpKeepAlive(@Nullable Boolean tcpKeepAlive) {
      this.tcpKeepAlive = tcpKeepAlive;
      return this;
    }

    public Builder tcpAbortiveClose(@Nullable Boolean tcpAbortiveClose) {
      this.tcpAbortiveClose = tcpAbortiveClose;
      return this;
    }

    public Builder transactionReplay(@Nullable Boolean transactionReplay) {
      this.transactionReplay = transactionReplay;
      return this;
    }

    public Builder connectionAttributes(@Nullable Map<String, String> connectionAttributes) {
      this.connectionAttributes = connectionAttributes;
      return this;
    }

    /**
     * Set session variable
     *
     * @param sessionVariables map containing session variables
     * @return this {@link Builder}
     */
    public Builder sessionVariables(@Nullable Map<String, Object> sessionVariables) {
      this.sessionVariables = sessionVariables;
      return this;
    }

    public Builder pamOtherPwd(@Nullable CharSequence[] pamOtherPwd) {
      this.pamOtherPwd = pamOtherPwd;
      return this;
    }

    /**
     * Configure the database.
     *
     * @param database the database
     * @return this {@link Builder}
     */
    public Builder database(@Nullable String database) {
      this.database = database;
      return this;
    }

    /**
     * Configure the host.
     *
     * @param host the host
     * @return this {@link Builder}
     * @throws IllegalArgumentException if {@code host} is {@code null}
     */
    public Builder host(String host) {
      this.host = Assert.requireNonNull(host, "host must not be null");
      return this;
    }

    /**
     * Configure the password.
     *
     * @param password the password
     * @return this {@link Builder}
     */
    public Builder password(@Nullable CharSequence password) {
      this.password = password;
      return this;
    }

    /**
     * Set protocol to a specific set of TLS version
     *
     * @param tlsProtocol Strings listing possible protocol, like "TLSv1.2"
     * @return this {@link Builder}
     */
    public Builder tlsProtocol(String... tlsProtocol) {
      if (tlsProtocol == null) {
        this.tlsProtocol = null;
        return this;
      }
      List<String> tmp = new ArrayList<>();
      for (String protocol : tlsProtocol) {
        if (protocol != null) tmp.add(protocol);
      }
      if (!tmp.isEmpty()) this.tlsProtocol = tmp;
      return this;
    }

    /**
     * Permits providing server's certificate in DER form, or server's CA certificate. The server
     * will be added to trustStore. This permits a self-signed certificate to be trusted.
     *
     * <p>Can be used in one of 3 forms :
     *
     * <ul>
     *   <li>serverSslCert=/path/to/cert.pem (full path to certificate)
     *   <li>serverSslCert=classpath:relative/cert.pem (relative to current classpath)
     *   <li>or as verbatim DER-encoded certificate string \"------BEGIN CERTIFICATE-----\" .".
     * </ul>
     *
     * @param serverSslCert certificate
     * @return this {@link Builder}
     */
    public Builder serverSslCert(String serverSslCert) {
      this.serverSslCert = serverSslCert;
      return this;
    }

    /**
     * Prepare result cache size.
     *
     * <ul>
     *   <li>0 = no cache
     *   <li>null = use default size
     *   <li>other indicate cache size
     * </ul>
     *
     * @param prepareCacheSize prepare cache size
     * @return this {@link Builder}
     */
    public Builder prepareCacheSize(Integer prepareCacheSize) {
      this.prepareCacheSize = prepareCacheSize;
      return this;
    }

    /**
     * Permits providing client's certificate for mutual authentication
     *
     * <p>Can be used in one of 3 forms :
     *
     * <ul>
     *   <li>clientSslCert=/path/to/cert.pem (full path to certificate)
     *   <li>clientSslCert=classpath:relative/cert.pem (relative to current classpath)
     *   <li>or as verbatim DER-encoded certificate string \"------BEGIN CERTIFICATE-----\" .".
     * </ul>
     *
     * @param clientSslCert certificate
     * @return this {@link Builder}
     */
    public Builder clientSslCert(String clientSslCert) {
      this.clientSslCert = clientSslCert;
      return this;
    }

    /**
     * Client private key (PKCS#8 private key file in PEM format)
     *
     * @param clientSslKey Client Private key path.
     * @return this {@link Builder}
     */
    public Builder clientSslKey(String clientSslKey) {
      this.clientSslKey = clientSslKey;
      return this;
    }

    /**
     * Client private key password if any. null if no password.
     *
     * @param clientSslPassword client private key password
     * @return this {@link Builder}
     */
    public Builder clientSslPassword(CharSequence clientSslPassword) {
      this.clientSslPassword = clientSslPassword;
      return this;
    }

    public Builder sslMode(SslMode sslMode) {
      this.sslMode = sslMode;
      if (sslMode == null) this.sslMode = SslMode.DISABLE;
      return this;
    }

    /**
     * Permit to indicate to use text or binary protocol.
     *
     * @param useServerPrepStmts use server param
     * @return this {@link Builder}
     */
    public Builder useServerPrepStmts(boolean useServerPrepStmts) {
      this.useServerPrepStmts = useServerPrepStmts;
      return this;
    }

    /**
     * Permit to indicate default autocommit value. Default value True.
     *
     * @param autocommit use autocommit
     * @return this {@link Builder}
     */
    public Builder autocommit(Boolean autocommit) {
      this.autocommit = autocommit;
      return this;
    }

    /**
     * Permit to indicate that commands after connections must be skipped. This permit to avoid
     * unnecessary command on connection creation, and when using RDV proxy not to have session
     * pinning
     *
     * <p>Use with care, because connector expects server to have :
     *
     * <ul>
     *   <li>connection exchanges to be UT8(mb3/mb4)
     *   <li>autocommit set to true
     * </ul>
     *
     * Default value False.
     *
     * @param skipPostCommands skip post commands
     * @return this {@link Builder}
     */
    public Builder skipPostCommands(boolean skipPostCommands) {
      this.skipPostCommands = skipPostCommands;
      return this;
    }

    /**
     * Configure the port. Defaults to {@code 3306}.
     *
     * @param port the port
     * @return this {@link Builder}
     */
    public Builder port(int port) {
      this.port = port;
      return this;
    }

    /**
     * Configure if multi-queries are allowed. Defaults to {@code false}.
     *
     * @param allowMultiQueries are multi-queries allowed
     * @return this {@link Builder}
     */
    public Builder allowMultiQueries(boolean allowMultiQueries) {
      this.allowMultiQueries = allowMultiQueries;
      return this;
    }

    /**
     * Configure the unix domain socket to connect to.
     *
     * @param socket the socket path
     * @return this {@link Builder}
     * @throws IllegalArgumentException if {@code socket} is {@code null}
     */
    public Builder socket(String socket) {
      this.socket = Assert.requireNonNull(socket, "host must not be null");
      return this;
    }

    public Builder username(String username) {
      this.username = Assert.requireNonNull(username, "username must not be null");
      return this;
    }

    public Builder loopResources(LoopResources loopResources) {
      this.loopResources = Assert.requireNonNull(loopResources, "loopResources must not be null");
      return this;
    }

    public Builder sslContextBuilderCustomizer(
        UnaryOperator<SslContextBuilder> sslContextBuilderCustomizer) {
      this.sslContextBuilderCustomizer = sslContextBuilderCustomizer;
      return this;
    }

    public Builder sslTunnelDisableHostVerification(boolean sslTunnelDisableHostVerification) {
      this.sslTunnelDisableHostVerification = sslTunnelDisableHostVerification;
      return this;
    }

    @Override
    public Builder clone() throws CloneNotSupportedException {
      return (Builder) super.clone();
    }

    @Override
    public String toString() {
      StringBuilder hiddenPwd = new StringBuilder();
      if (password != null) {
        hiddenPwd.append("*");
      }
      StringBuilder hiddenPamPwd = new StringBuilder();
      if (pamOtherPwd != null) {
        for (CharSequence s : pamOtherPwd) {
          hiddenPamPwd.append("*");
          hiddenPamPwd.append(",");
        }
        hiddenPamPwd.deleteCharAt(hiddenPamPwd.length() - 1);
      }

      return "Builder{"
          + "haMode="
          + haMode
          + ", username="
          + username
          + ", connectTimeout="
          + connectTimeout
          + ", tcpKeepAlive="
          + tcpKeepAlive
          + ", tcpAbortiveClose="
          + tcpAbortiveClose
          + ", transactionReplay="
          + transactionReplay
          + ", database="
          + database
          + ", host="
          + host
          + ", sessionVariables="
          + sessionVariables
          + ", connectionAttributes="
          + connectionAttributes
          + ", password="
          + hiddenPwd
          + ", restrictedAuth="
          + restrictedAuth
          + ", port="
          + port
          + ", hosts={"
          + (hostAddresses == null ? "" : Arrays.toString(hostAddresses.toArray()))
          + '}'
          + ", socket="
          + socket
          + ", allowMultiQueries="
          + allowMultiQueries
          + ", useServerPrepStmts="
          + useServerPrepStmts
          + ", prepareCacheSize="
          + prepareCacheSize
          + ", tlsProtocol="
          + tlsProtocol
          + ", serverSslCert="
          + serverSslCert
          + ", clientSslCert="
          + clientSslCert
          + ", clientSslKey="
          + clientSslKey
          + ", clientSslPassword="
          + clientSslPassword
          + ", sslMode="
          + sslMode
          + ", sslTunnelDisableHostVerification="
          + sslTunnelDisableHostVerification
          + ", pamOtherPwd="
          + hiddenPamPwd
          + ", autoCommit="
          + autocommit
          + '}';
    }
  }
}
