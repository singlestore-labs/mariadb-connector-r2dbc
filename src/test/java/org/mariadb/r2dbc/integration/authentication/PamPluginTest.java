// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-2025 SingleStore, Inc.

package org.mariadb.r2dbc.integration.authentication;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.MariadbConnectionFactory;
import org.mariadb.r2dbc.TestConfiguration;
import org.mariadb.r2dbc.api.MariadbConnection;

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
