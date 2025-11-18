// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab
// Copyright (c) 2025-2025 SingleStore, Inc.

package com.singlestore.r2dbc.integration;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import com.singlestore.r2dbc.BaseConnectionTest;
import com.singlestore.r2dbc.SingleStoreConnectionConfiguration;
import com.singlestore.r2dbc.SingleStoreConnectionFactory;
import com.singlestore.r2dbc.TestConfiguration;
import com.singlestore.r2dbc.api.SingleStoreConnection;
import com.singlestore.r2dbc.api.SingleStoreConnectionMetadata;
import org.slf4j.LoggerFactory;
import reactor.test.StepVerifier;

public class LoggingTest extends BaseConnectionTest {

  @Test
  void basicLogging() throws IOException {
    Assumptions.assumeFalse(isXpand());
    File tempFile = File.createTempFile("log", ".tmp");

    Logger logger = (Logger) LoggerFactory.getLogger("com.singlestore.r2dbc");
    Level initialLevel = logger.getLevel();
    logger.setLevel(Level.TRACE);
    logger.setAdditive(false);
    // logger.detachAndStopAllAppenders();

    LoggerContext context = new LoggerContext();
    FileAppender<ILoggingEvent> fa = new FileAppender<ILoggingEvent>();
    fa.setName("FILE");
    fa.setImmediateFlush(true);
    PatternLayoutEncoder pa = new PatternLayoutEncoder();
    pa.setPattern("%r %5p %c [%t] - %m%n");
    pa.setContext(context);
    pa.start();
    fa.setEncoder(pa);

    fa.setFile(tempFile.getPath());
    fa.setAppend(true);
    fa.setContext(context);
    fa.start();

    logger.addAppender(fa);

    SingleStoreConnectionConfiguration conf = TestConfiguration.defaultBuilder.build();
    SingleStoreConnection connection = new SingleStoreConnectionFactory(conf).create().block();
    connection
        .createStatement("SELECT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, Integer.class)))
        .as(StepVerifier::create)
        .expectNext(1)
        .verifyComplete();
    SingleStoreConnectionMetadata meta = connection.getMetadata();
    connection.close().block();

    String contents = new String(Files.readAllBytes(Paths.get(tempFile.getPath())));
    String selectIsolation;
    if (minVersion(8, 7, 1)) {
      selectIsolation =
        "         +-------------------------------------------------+\n" +
          "         |  0  1  2  3  4  5  6  7  8  9  a  b  c  d  e  f |\n" +
          "+--------+-------------------------------------------------+----------------+\n" +
          "|00000000| 4d 00 00 00 03 53 45 54 20 20 6e 61 6d 65 73 20 |M....SET  names |\n" +
          "|00000010| 55 54 46 38 4d 42 34 2c 61 75 74 6f 63 6f 6d 6d |UTF8MB4,autocomm|\n" +
          "|00000020| 69 74 3d 31 2c 40 40 53 45 53 53 49 4f 4e 2e 65 |it=1,@@SESSION.e|\n" +
          "|00000030| 6e 61 62 6c 65 5f 65 78 74 65 6e 64 65 64 5f 74 |nable_extended_t|\n" +
          "|00000040| 79 70 65 73 5f 6d 65 74 61 64 61 74 61 3d 6f 66 |ypes_metadata=of|\n" +
          "|00000050| 66                                              |f               |\n" +
          "+--------+-------------------------------------------------+----------------+";
    } else {
      selectIsolation =
        "         +-------------------------------------------------+\n" +
          "         |  0  1  2  3  4  5  6  7  8  9  a  b  c  d  e  f |\n" +
          "+--------+-------------------------------------------------+----------------+\n" +
         "|00000000| 20 00 00 00 03 53 45 54 20 20 6e 61 6d 65 73 20 | ....SET  names |\n" +
         "|00000010| 55 54 46 38 4d 42 34 2c 61 75 74 6f 63 6f 6d 6d |UTF8MB4,autocomm|\n" +
         "|00000020| 69 74 3d 31                                     |it=1            |\n" +
         "+--------+-------------------------------------------------+----------------+";
    }
    Assertions.assertTrue(
        contents.contains(selectIsolation)
            || contents.contains(selectIsolation.replace("\r\n", "\n")),
        contents);

    String selectOne =
        "         +-------------------------------------------------+\r\n"
            + "         |  0  1  2  3  4  5  6  7  8  9  a  b  c  d  e  f |\r\n"
            + "+--------+-------------------------------------------------+----------------+\r\n"
            + "|00000000| 09 00 00 00 03 53 45 4c 45 43 54 20 31          |.....SELECT 1   |\r\n"
            + "+--------+-------------------------------------------------+----------------+";
    Assertions.assertTrue(
        contents.contains(selectOne) || contents.contains(selectOne.replace("\r\n", "\n")));
    logger.setLevel(initialLevel);
    logger.detachAppender(fa);
    logger.setAdditive(true);
  }

  public String encodeHexString(byte[] byteArray) {
    StringBuffer hexStringBuffer = new StringBuffer();
    for (int i = 0; i < byteArray.length; i++) {
      hexStringBuffer.append(byteToHex(byteArray[i]));
    }
    return hexStringBuffer.toString();
  }

  public String byteToHex(byte num) {
    char[] hexDigits = new char[2];
    hexDigits[0] = Character.forDigit((num >> 4) & 0xF, 16);
    hexDigits[1] = Character.forDigit((num & 0xF), 16);
    return new String(hexDigits);
  }
}
