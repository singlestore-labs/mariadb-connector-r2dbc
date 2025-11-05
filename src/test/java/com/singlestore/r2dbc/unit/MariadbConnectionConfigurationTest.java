// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.unit;

import io.r2dbc.spi.ConnectionFactoryOptions;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import com.singlestore.r2dbc.MariadbConnectionConfiguration;
import com.singlestore.r2dbc.SslMode;
import com.singlestore.r2dbc.util.Security;
import reactor.netty.resources.LoopResources;

public class MariadbConnectionConfigurationTest {
  @Test
  public void builder() {
    TreeMap<String, String> connectionAttributes = new TreeMap<>();
    connectionAttributes.put("entry1", "val1");
    connectionAttributes.put("entry2", "val2");
    Map<String, Object> tzMap = new HashMap<>();
    tzMap.put("timezone", "Europe/Paris");
    MariadbConnectionConfiguration conf =
        MariadbConnectionConfiguration.builder()
            .connectTimeout(Duration.ofMillis(150))
            .haMode("LOADBALANCE")
            .restrictedAuth("mysql_native_password,client_ed25519")
            .tcpKeepAlive(true)
            .tcpAbortiveClose(true)
            .transactionReplay(true)
            .connectionAttributes(connectionAttributes)
            .sessionVariables(tzMap)
            .pamOtherPwd(new String[] {"otherPwd"})
            .database("MyDB")
            .password("MyPassword")
            .tlsProtocol("TLSv1.2", "TLSv1.3")
            .serverSslCert("/path/to/serverCert")
            .prepareCacheSize(125)
            .clientSslKey("clientSecretKey")
            .clientSslPassword("ClientSecretPwd")
            .sslMode(SslMode.TRUST)
            .useServerPrepStmts(true)
            .autocommit(false)
            .allowMultiQueries(false)
            .socket("/path/to/mysocket")
            .username("MyUSer")
            .loopResources(LoopResources.create("mariadb"))
            .sslContextBuilderCustomizer((b) -> b)
            .sslTunnelDisableHostVerification(true)
            .build();
    Assertions.assertEquals(
        "r2dbc:mariadb:loadbalance://localhost/MyDB?connectTimeout=PT0.15S&tcpKeepAlive=true&tcpAbortiveClose=true&transactionReplay=true&password=***&pamOtherPwd=otherPwd&prepareCacheSize=125&socket=/path/to/mysocket&username=MyUSer&connectionAttributes=entry1=val1,entry2=val2&sessionVariables=timezone=Europe/Paris&sslMode=trust&serverSslCert=/path/to/serverCert&tlsProtocol=TLSv1.2,TLSv1.3&clientSslKey=clientSecretKey&clientSslPassword=***&sslTunnelDisableHostVerification=true&useServerPrepStmts=true&autocommit=false&restrictedAuth=mysql_native_password,client_ed25519",
        conf.toString());
  }

  @Test
  public void haMode() {
    TreeMap<String, String> connectionAttributes = new TreeMap<>();
    connectionAttributes.put("entry1", "val1");
    connectionAttributes.put("entry2", "val2");
    MariadbConnectionConfiguration conf =
        MariadbConnectionConfiguration.builder()
            .haMode("")
            .host("localhost")
            .username("user")
            .connectionAttributes(connectionAttributes)
            .build();
    Assertions.assertEquals(
        "r2dbc:mariadb://localhost/?username=user&connectionAttributes=entry1=val1,entry2=val2",
        conf.toString());
  }

  @Test
  public void connectionString() {
    ConnectionFactoryOptions options =
        ConnectionFactoryOptions.parse(
            "r2dbc:mariadb://ro%3Aot:pw%3Ad@localhost:3306/db?connectTimeout=PT0.15S"
                + "&haMode=LOADBALANCE"
                + "&restrictedAuth=mysql_native_password,client_ed25519"
                + "&tcpKeepAlive=true"
                + "&tcpAbortiveClose=true"
                + "&transactionReplay=true"
                + "&connectionAttributes=entry1=val1,entry2=val2"
                + "&sessionVariables=timezone='Europe/Paris'"
                + "&pamOtherPwd=otherPwd"
                + "&tlsProtocol=TLSv1.2,TLSv1.3"
                + "&serverSslCert=/path/to/serverCert"
                + "&prepareCacheSize=125"
                + "&clientSslKey=clientSecretKey"
                + "&clientSslPassword=ClientSecretPwd"
                + "&sslMode=TRUST"
                + "&useServerPrepStmts=true"
                + "&autocommit=false"
                + "&allowMultiQueries=true"
                + "&socket=/path/to/mysocket"
                + "&sslTunnelDisableHostVerification=true");
    MariadbConnectionConfiguration conf =
        MariadbConnectionConfiguration.fromOptions(options).build();
    Assertions.assertEquals(
        "r2dbc:mariadb:loadbalance://localhost/db?connectTimeout=PT0.15S"
            + "&tcpKeepAlive=true"
            + "&tcpAbortiveClose=true"
            + "&transactionReplay=true"
            + "&password=***"
            + "&pamOtherPwd=otherPwd"
            + "&prepareCacheSize=125"
            + "&socket=/path/to/mysocket"
            + "&username=ro:ot"
            + "&allowMultiQueries=true"
            + "&connectionAttributes=entry1=val1,entry2=val2"
            + "&sessionVariables=timezone='Europe/Paris'"
            + "&sslMode=trust"
            + "&serverSslCert=/path/to/serverCert"
            + "&tlsProtocol=TLSv1.2,TLSv1.3"
            + "&clientSslKey=clientSecretKey"
            + "&clientSslPassword=***"
            + "&sslTunnelDisableHostVerification=true"
            + "&autocommit=false"
            + "&restrictedAuth=mysql_native_password,client_ed25519",
        conf.toString());
  }

  @Test
  public void connectionSessionVariablesString() {
    ConnectionFactoryOptions options =
        ConnectionFactoryOptions.parse(
            "r2dbc:mariadb://ro%3Aot:pw%3Ad@localhost:3306/db?sessionVariables=wait_timeout=1,sql_mode='TRADITIONAL,NO_AUTO_VALUE_ON_ZERO,ONLY_FULL_GROUP_BY'");
    MariadbConnectionConfiguration conf =
        MariadbConnectionConfiguration.fromOptions(options).build();
    Assertions.assertEquals(
        "r2dbc:mariadb://localhost/db?password=***&username=ro:ot&sessionVariables=sql_mode='TRADITIONAL,NO_AUTO_VALUE_ON_ZERO,ONLY_FULL_GROUP_BY',wait_timeout=1",
        conf.toString());
  }

  @Test
  public void connectionStringLoadBalance() {
    ConnectionFactoryOptions options =
        ConnectionFactoryOptions.parse(
            "r2dbc:mariadb:loadbalancing://ro%3Aot:pw%3Ad@localhost:3306/db?connectTimeout=PT0.15S"
                + "&restrictedAuth=mysql_native_password,client_ed25519"
                + "&tcpKeepAlive=true"
                + "&tcpAbortiveClose=true"
                + "&transactionReplay=true"
                + "&connectionAttributes=entry1=val1,entry2=val2"
                + "&sessionVariables=timezone='Europe/Paris'"
                + "&pamOtherPwd=otherPwd"
                + "&tlsProtocol=TLSv1.2,TLSv1.3"
                + "&serverSslCert=/path/to/serverCert"
                + "&prepareCacheSize=125"
                + "&clientSslKey=clientSecretKey"
                + "&clientSslPassword=ClientSecretPwd"
                + "&sslMode=TRUST"
                + "&useServerPrepStmts=true"
                + "&autocommit=false"
                + "&allowMultiQueries=true"
                + "&socket=/path/to/mysocket"
                + "&sslTunnelDisableHostVerification=true");
    MariadbConnectionConfiguration conf =
        MariadbConnectionConfiguration.fromOptions(options).build();
    Assertions.assertEquals(
        "r2dbc:mariadb:loadbalance://localhost/db?connectTimeout=PT0.15S&tcpKeepAlive=true&tcpAbortiveClose=true&transactionReplay=true&password=***&pamOtherPwd=otherPwd&prepareCacheSize=125&socket=/path/to/mysocket&username=ro:ot&allowMultiQueries=true&connectionAttributes=entry1=val1,entry2=val2&sessionVariables=timezone='Europe/Paris'&sslMode=trust&serverSslCert=/path/to/serverCert&tlsProtocol=TLSv1.2,TLSv1.3&clientSslKey=clientSecretKey&clientSslPassword=***&sslTunnelDisableHostVerification=true&autocommit=false&restrictedAuth=mysql_native_password,client_ed25519",
        conf.toString());
  }

  @Test
  public void testSessionVariableParsing() {
    Assertions.assertEquals(
        "{wait_timeout=1}", Security.parseSessionVariables("wait_timeout=1").toString());
    Assertions.assertEquals(
        "{sql_mode='TRADITIONAL,NO_AUTO_VALUE_ON_ZERO,ONLY_FULL_GROUP_BY'}",
        Security.parseSessionVariables(
                "sql_mode='TRADITIONAL,NO_AUTO_VALUE_ON_ZERO,ONLY_FULL_GROUP_BY'")
            .toString());
    Assertions.assertEquals(
        "{sql_mode='TRADITIONAL,NO_AUTO_VALUE_ON_ZERO,ONLY_FULL_GROUP_BY', wait_timeout=1}",
        Security.parseSessionVariables(
                "wait_timeout=1,sql_mode='TRADITIONAL,NO_AUTO_VALUE_ON_ZERO,ONLY_FULL_GROUP_BY'")
            .toString());
    Assertions.assertEquals(
        "{sql_mode='TRADITIONAL,NO_AUTO_VALUE_ON_ZERO,ONLY_FULL_GROUP_BY', wait_timeout=1}",
        Security.parseSessionVariables(
                "sql_mode='TRADITIONAL,NO_AUTO_VALUE_ON_ZERO,ONLY_FULL_GROUP_BY',wait_timeout=1")
            .toString());
  }
}
