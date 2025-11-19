package com.singlestore.r2dbc.integration.parameter;

import com.singlestore.r2dbc.BaseConnectionTest;
import com.singlestore.r2dbc.SingleStoreConnectionFactory;
import com.singlestore.r2dbc.TestConfiguration;
import com.singlestore.r2dbc.api.SingleStoreConnection;
import com.singlestore.r2dbc.client.util.VectorType;
import com.singlestore.r2dbc.type.Vector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.singlestore.r2dbc.integration.codec.VectorParseTest.convertToBinary;
import static com.singlestore.r2dbc.integration.codec.VectorParseTest.generateVector;

public class VectorParameterTest extends BaseConnectionTest {
  @Test
  public void sendParam() throws Exception {
    Assumptions.assumeTrue(minVersion(8, 7, 1));
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
      sendParam(connection, false);
      sendParam(binaryConnection, true);
    } finally {
      connection.close().block();
      binaryConnection.close().block();
    }
  }

  private void sendParam(SingleStoreConnection conn, boolean isBinary) throws Exception {
    List<Vector> vectors = new ArrayList<>(4);
    vectors.add(
      isBinary
        ? convertToBinary(generateVector(VectorType.I64, 2))
        : generateVector(VectorType.I64, 2));
    vectors.add(
      isBinary
        ? convertToBinary(generateVector(VectorType.I64, 2))
        : generateVector(VectorType.I64, 2));
    vectors.add(
      isBinary
        ? convertToBinary(generateVector(VectorType.I64, 2))
        : generateVector(VectorType.I64, 2));
    Assertions.assertEquals(3, vectors.size());
    Vector vector1 = vectors.get(0);
    Vector vector2 = vectors.get(1);
    Vector vector3 = vectors.get(2);


    conn.createStatement("DROP TABLE IF EXISTS VectorCodec2").execute().blockLast();
    conn.createStatement(
  "CREATE TABLE VectorCodec2 (id int not null primary key auto_increment, t1 VECTOR(2, I64))"
    ).execute().blockLast();
    conn.createStatement("INSERT INTO VectorCodec2(id, t1) VALUES (?, ?)")
      .bind(0, 1)
      .bind(1, vector1)
      .execute()
      .blockLast();
    conn.createStatement("INSERT INTO VectorCodec2(id, t1) VALUES (?, ?)")
      .bind(0, 2)
      .bindNull(1, Vector.class)
      .execute()
      .blockLast();
    conn.createStatement("INSERT INTO VectorCodec2(id, t1) VALUES (?, ?)")
      .bind(0, 3)
      .bind(1, vector2)
      .execute()
      .blockLast();
    conn.createStatement("INSERT INTO VectorCodec2(id, t1) VALUES (?, ?)")
      .bind(0, 4)
      .bind(1, vector3)
      .execute()
      .blockLast();

    conn.createStatement("SELECT * FROM VectorCodec2 ORDER BY id").execute()
    .flatMap(result -> result.map((row, metadata) -> Optional.ofNullable(row.get("t1", Vector.class))))
    .collectList()
    .doOnNext(vectorsFromDb -> {
        Assertions.assertEquals(vector1, vectorsFromDb.get(0).get());
        Assertions.assertFalse(vectorsFromDb.get(1).isPresent());
        Assertions.assertEquals(vector2, vectorsFromDb.get(2).get());
        Assertions.assertEquals(vector3, vectorsFromDb.get(3).get());
    })
    .block();
  }
}
