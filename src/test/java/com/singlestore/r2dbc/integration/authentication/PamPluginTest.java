// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.integration.authentication;

import org.junit.jupiter.api.Test;
import com.singlestore.r2dbc.BaseConnectionTest;
import com.singlestore.r2dbc.MariadbConnectionConfiguration;
import com.singlestore.r2dbc.MariadbConnectionFactory;
import com.singlestore.r2dbc.TestConfiguration;
import com.singlestore.r2dbc.api.MariadbConnection;

public class PamPluginTest extends BaseConnectionTest {

  @Test
  public void pamAuthPlugin() throws Throwable {
    String pamUser = "test_pam";
    sharedConn.createStatement("DROP USER IF EXISTS '" + pamUser + "'@'%'").execute().blockLast();
    sharedConn
        .createStatement("CREATE USER '" + pamUser + "'@'%' IDENTIFIED WITH authentication_pam as 's2_pam_test'")
        .execute()
        .blockLast();
    sharedConn
        .createStatement("GRANT SELECT ON *.* TO '" + pamUser + "'@'%'")
        .execute()
        .blockLast();

    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .username(pamUser)
            .password("test_pass")
            .build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection.close().block();
  }
}
