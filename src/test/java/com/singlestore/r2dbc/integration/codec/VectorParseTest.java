package com.singlestore.r2dbc.integration.codec;

import com.singlestore.r2dbc.BaseConnectionTest;
import com.singlestore.r2dbc.SingleStoreConnectionConfiguration;
import com.singlestore.r2dbc.SingleStoreConnectionFactory;
import com.singlestore.r2dbc.TestConfiguration;
import com.singlestore.r2dbc.api.SingleStoreConnection;
import com.singlestore.r2dbc.client.util.VectorType;
import com.singlestore.r2dbc.codec.DataType;
import com.singlestore.r2dbc.message.server.ColumnDefinitionPacket;
import com.singlestore.r2dbc.type.Vector;
import com.singlestore.r2dbc.unit.util.VectorDataUtilsTest;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class VectorParseTest extends BaseConnectionTest {
  private static final String F_VECTOR_TABLE_NAME = "VectorFloatCodec";
  private static final String I_VECTOR_TABLE_NAME = "VectorIntCodec";
  private static final List<Vector> I_VECTOR_VALUES = new ArrayList<>();
  private static final List<Vector> F_VECTOR_VALUES = new ArrayList<>();

  @BeforeAll
  public static void before2() {
    afterAll2();
    F_VECTOR_VALUES.add(generateVector(VectorType.F32, 3));
    F_VECTOR_VALUES.add(generateVector(VectorType.F32, 4));
    F_VECTOR_VALUES.add(generateVector(VectorType.F64, 2));

    I_VECTOR_VALUES.add(generateVector(VectorType.I8, 3));
    I_VECTOR_VALUES.add(generateVector(VectorType.I16, 4));
    I_VECTOR_VALUES.add(generateVector(VectorType.I32, 3));
    I_VECTOR_VALUES.add(generateVector(VectorType.I64, 2));

    sharedConn.createStatement(String.format(
      "CREATE TABLE %s (t1 VECTOR(3), t2 VECTOR(4, F32), t3 VECTOR(2, F64), t4 VECTOR(10), id INT)",
      F_VECTOR_TABLE_NAME
    )).execute().blockLast();
    sharedConn
      .createStatement(String.format(
        "CREATE TABLE %s (t1 VECTOR(3, I8), t2 VECTOR(4, I16), t3 VECTOR(3, I32), t4 VECTOR(2, I64), id INT)",
        I_VECTOR_TABLE_NAME
      )).execute().blockLast();
    sharedConn
      .createStatement(String.format(
        "INSERT INTO %s VALUES ('%s', '%s', '%s', null, 1)",
        F_VECTOR_TABLE_NAME,
        F_VECTOR_VALUES.get(0).stringValue(),
        F_VECTOR_VALUES.get(1).stringValue(),
        F_VECTOR_VALUES.get(2).stringValue()
      )).execute().blockLast();
    sharedConn
      .createStatement(String.format(
        "INSERT INTO %s VALUES ('%s', '%s', '%s', '%s', 1)",
        I_VECTOR_TABLE_NAME,
        I_VECTOR_VALUES.get(0).stringValue(),
        I_VECTOR_VALUES.get(1).stringValue(),
        I_VECTOR_VALUES.get(2).stringValue(),
        I_VECTOR_VALUES.get(3).stringValue()
      )).execute().blockLast();
  }

  @AfterAll
  public static void afterAll2() {
    Assumptions.assumeTrue(minVersion(8, 7, 1));
    sharedConn.createStatement(String.format("DROP TABLE IF EXISTS %s", F_VECTOR_TABLE_NAME)).execute().blockLast();
    sharedConn.createStatement(String.format("DROP TABLE IF EXISTS %s", I_VECTOR_TABLE_NAME)).execute().blockLast();
  }

  @Test
  public void getObject() throws Exception {
    getObject(TestConfiguration.defaultBuilder.clone()
      .enableExtendedDataTypes(true)
    );
  }

  @Test
  public void getObjectPrepare() throws Exception {
    getObject(TestConfiguration.defaultBuilder.clone()
      .useServerPrepStmts(true)
      .enableExtendedDataTypes(true)
    );
  }

  @Test
  public void getMetaData() throws Exception {
    SingleStoreConnection connection = new SingleStoreConnectionFactory(
      TestConfiguration.defaultBuilder.clone()
        .useServerPrepStmts(true)
        .enableExtendedDataTypes(true)
        .sessionVariables(Collections.singletonMap("vector_type_project_format", "JSON"))
        .build()
    ).create().block();
    SingleStoreConnection binaryConnection = new SingleStoreConnectionFactory(
      TestConfiguration.defaultBuilder.clone()
        .useServerPrepStmts(true)
        .enableExtendedDataTypes(true)
        .sessionVariables(Collections.singletonMap("vector_type_project_format", "BINARY"))
        .build()
    ).create().block();

    try {
      getMetaData(connection);
      getMetaData(binaryConnection);
    } finally {
      connection.close().block();
      binaryConnection.close().block();
    }
  }

  private void getMetaData(SingleStoreConnection conn) throws SQLException {
    checkMeta(conn, I_VECTOR_TABLE_NAME, meta -> {
      Assertions.assertEquals("VECTOR", meta.getColumnMetadata(0).getType().getName());
      Assertions.assertEquals(Vector.class.getName(), meta.getColumnMetadata(0).getJavaType().getName());

      ColumnDefinitionPacket meta1 = (ColumnDefinitionPacket) meta.getColumnMetadata(0);
      Assertions.assertEquals("t1alias", meta1.getName());
      Assertions.assertEquals("t1", meta1.getColumn());
      Assertions.assertEquals(DataType.INT8_VECTOR, meta1.getDataType());
      Assertions.assertEquals(4, meta.getColumnMetadatas().size());
      Assertions.assertEquals(0, meta1.getScale());
      Assertions.assertEquals("testr2", meta1.getSchema());
      Assertions.assertEquals(I_VECTOR_TABLE_NAME, meta1.getTable());

      Assertions.assertEquals("VECTOR", meta.getColumnMetadata(1).getType().getName());
      Assertions.assertEquals("VECTOR", meta.getColumnMetadata(2).getType().getName());
      Assertions.assertEquals("VECTOR", meta.getColumnMetadata(3).getType().getName());
    });

    checkMeta(conn, F_VECTOR_TABLE_NAME, meta -> {
      Assertions.assertEquals("VECTOR", meta.getColumnMetadata(0).getType().getName());
      Assertions.assertEquals(Vector.class.getName(), meta.getColumnMetadata(0).getJavaType().getName());

      ColumnDefinitionPacket meta1 = (ColumnDefinitionPacket) meta.getColumnMetadata(0);
      Assertions.assertEquals("t1alias", meta1.getName());
      Assertions.assertEquals("t1", meta1.getColumn());
      Assertions.assertEquals(DataType.FLOAT32_VECTOR, meta1.getDataType());
      Assertions.assertEquals(4, meta.getColumnMetadatas().size());
      Assertions.assertEquals(0, meta1.getScale());
      Assertions.assertEquals("testr2", meta1.getSchema());
      Assertions.assertEquals(F_VECTOR_TABLE_NAME, meta1.getTable());

      Assertions.assertEquals("VECTOR", meta.getColumnMetadata(1).getType().getName());
      Assertions.assertEquals("VECTOR", meta.getColumnMetadata(2).getType().getName());
      Assertions.assertEquals("VECTOR", meta.getColumnMetadata(3).getType().getName());
    });
  }

  public void getObject(SingleStoreConnectionConfiguration.Builder confBuilder) throws SQLException {
    SingleStoreConnection connection = new SingleStoreConnectionFactory(
      confBuilder
        .sessionVariables(Collections.singletonMap("vector_type_project_format", "JSON"))
        .build()
    ).create().block();
    SingleStoreConnection binaryConnection = new SingleStoreConnectionFactory(
      confBuilder
        .sessionVariables(Collections.singletonMap("vector_type_project_format", "BINARY"))
        .build()
    ).create().block();
    try {
      checkData(connection, I_VECTOR_TABLE_NAME, intChecker(false));
      checkData(binaryConnection, I_VECTOR_TABLE_NAME, intChecker(true));
      checkData(connection, F_VECTOR_TABLE_NAME, floatChecker());
      checkData(binaryConnection, F_VECTOR_TABLE_NAME, floatChecker());
    } finally {
      connection.close().block();
      binaryConnection.close().block();
    }
  }

  public void checkData(SingleStoreConnection conn, String table, Consumer<Row> checker) {
    conn.createStatement(String.format("select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from %s ORDER BY id", table))
      .execute()
      .flatMap(r -> r.map((row, metadata) -> row))
      .as(StepVerifier::create)
      .consumeNextWith(checker)
      .verifyComplete();
  }

  public void checkMeta(SingleStoreConnection conn, String table, Consumer<RowMetadata> checker) {
    conn.createStatement(String.format("select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from %s ORDER BY id", table))
      .execute()
      .flatMap(r -> r.map((row, metadata) -> metadata))
      .as(StepVerifier::create)
      .consumeNextWith(checker)
      .verifyComplete();
  }

  private Consumer<Row> intChecker(Boolean isBinary) {
    return row -> {
      Vector vector1 = (Vector) row.get(0);
      Vector expectedVector1 = I_VECTOR_VALUES.get(0);

      Assertions.assertEquals(isBinary ? convertToBinary(expectedVector1) : expectedVector1, vector1);
      Assertions.assertArrayEquals(expectedVector1.toFloatArray(), vector1.toFloatArray());
      Assertions.assertArrayEquals(expectedVector1.toDoubleArray(), vector1.toDoubleArray());
      Assertions.assertArrayEquals(expectedVector1.toIntArray(), vector1.toIntArray());
      Assertions.assertArrayEquals(expectedVector1.toShortArray(), vector1.toShortArray());
      Assertions.assertArrayEquals(expectedVector1.toLongArray(), vector1.toLongArray());
      Assertions.assertArrayEquals(expectedVector1.toStringArray(), vector1.toStringArray());

      if (!isBinary) {
        Assertions.assertEquals(expectedVector1.stringValue(), row.get(0, String.class));
      }

      vector1 = (Vector) row.get(1);
      expectedVector1 = I_VECTOR_VALUES.get(1);
      Assertions.assertEquals(isBinary ? convertToBinary(expectedVector1) : expectedVector1, vector1);

      vector1 = (Vector) row.get("t2alias");
      Assertions.assertEquals(isBinary ? convertToBinary(expectedVector1) : expectedVector1, vector1);

      vector1 = row.get(2, Vector.class);
      expectedVector1 = I_VECTOR_VALUES.get(2);
      Assertions.assertEquals(isBinary ? convertToBinary(expectedVector1) : expectedVector1, vector1);

      vector1 = (Vector) row.get(3);
      expectedVector1 = I_VECTOR_VALUES.get(3);
      Assertions.assertEquals(isBinary ? convertToBinary(expectedVector1) : expectedVector1, vector1);
    };
  }

  private Consumer<Row> floatChecker() {
    return row -> {
      Vector vector1 = (Vector) row.get(0);
      Vector expectedVector1 = F_VECTOR_VALUES.get(0);
      Assertions.assertArrayEquals(expectedVector1.toFloatArray(), vector1.toFloatArray(), 0.0000001f);
      Assertions.assertArrayEquals(
        expectedVector1.toDoubleArray(), vector1.toDoubleArray(), 0.00000000000000000d);
      Assertions.assertEquals(expectedVector1.getType(), vector1.getType());

      vector1 = (Vector) row.get(1);
      expectedVector1 = F_VECTOR_VALUES.get(1);
      Assertions.assertArrayEquals(expectedVector1.toFloatArray(), vector1.toFloatArray(), 0.0000001f);
      Assertions.assertArrayEquals(
        expectedVector1.toDoubleArray(), vector1.toDoubleArray(), 0.00000000000000000d);

      vector1 = (Vector) row.get("t2alias");
      Assertions.assertArrayEquals(expectedVector1.toFloatArray(), vector1.toFloatArray(), 0.0000001f);
      Assertions.assertArrayEquals(
        expectedVector1.toDoubleArray(), vector1.toDoubleArray(), 0.00000000000000000d);

      vector1 = row.get(2, Vector.class);
      expectedVector1 = F_VECTOR_VALUES.get(2);
      Assertions.assertArrayEquals(
        expectedVector1.toDoubleArray(), vector1.toDoubleArray(), 0.00000000000000000d);

      vector1 = (Vector) row.get(3);
      Assertions.assertNull(vector1);
    };
  }

  public static Vector convertToBinary(Vector vector) {
    switch (vector.getType()) {
      case INT8_VECTOR:
        return Vector.fromData(vector.toByteArray(), vector.getLength(), vector.getType(), true);
      case INT16_VECTOR:
        return Vector.fromData(
          VectorDataUtilsTest.encodeShortArray(vector.toShortArray()),
          vector.getLength(),
          vector.getType(),
          true);
      case INT32_VECTOR:
        return Vector.fromData(
          VectorDataUtilsTest.encodeIntArray(vector.toIntArray()),
          vector.getLength(),
          vector.getType(),
          true);
      case INT64_VECTOR:
        return Vector.fromData(
          VectorDataUtilsTest.encodeLongArray(vector.toLongArray()),
          vector.getLength(),
          vector.getType(),
          true);
      case FLOAT32_VECTOR:
        return Vector.fromData(
          VectorDataUtilsTest.encodeFloatArray(vector.toFloatArray()),
          vector.getLength(),
          vector.getType(),
          true);
      case FLOAT64_VECTOR:
        return Vector.fromData(
          VectorDataUtilsTest.encodeDoubleArray(vector.toDoubleArray()),
          vector.getLength(),
          vector.getType(),
          true);
      default:
        throw new IllegalArgumentException(vector.getType() + " is not supported.");
    }
  }

  public static Vector generateVector(VectorType type, int length) {
    Number[] arr;
    Supplier<Number> supplier;
    final Random random = new Random();
    switch (type) {
      case I8:
        supplier = () -> (byte) random.nextInt(Byte.MAX_VALUE);
        arr = new Byte[length];
        break;
      case I16:
        supplier = () -> (short) random.nextInt(Short.MAX_VALUE);
        arr = new Short[length];
        break;
      case I32:
        supplier = () -> random.nextInt(Integer.MAX_VALUE);
        arr = new Integer[length];
        break;
      case I64:
        supplier = random::nextLong;
        arr = new Long[length];
        break;
      case F32:
        supplier = () -> (float) random.nextFloat();
        arr = new Float[length];
        break;
      case F64:
        supplier = random::nextDouble;
        arr = new Double[length];
        break;
      default:
        throw new IllegalArgumentException(type + " is not supported.");
    }
    for (int i = 0; i < length; i++) {
      arr[i] = supplier.get();
    }
    String strVal = Arrays.toString(arr).replace(" ", "");
    return Vector.fromData(strVal.getBytes(StandardCharsets.UTF_8), length, type.getType(), false);
  }
}
