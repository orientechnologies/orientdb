package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import java.util.List;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

public class OrientGraphComplexIndexTest extends OrientGraphBaseTest {

  @Test
  public void compositeIndexSingleSecondFieldTest() {

    OrientGraph noTx = factory.getNoTx();

    try {

      String className = noTx.createVertexClass("Foo");
      OClass foo = noTx.getRawDatabase().getMetadata().getSchema().getClass(className);
      foo.createProperty("prop1", OType.LONG);
      foo.createProperty("prop2", OType.STRING);

      foo.createIndex("V_Foo", OClass.INDEX_TYPE.UNIQUE, "prop1", "prop2");

      noTx.addVertex(T.label, "Foo", "prop1", 1, "prop2", "4ab25da0-3602-4f4a-bc5e-28bfefa5ca4c");

      GraphTraversal<Vertex, Vertex> traversal =
          noTx.traversal().V().has("Foo", "prop2", "4ab25da0-3602-4f4a-bc5e-28bfefa5ca4c");

      Assert.assertEquals(0, usedIndexes(noTx, traversal));

      List<Vertex> vertices = traversal.toList();

      Assert.assertEquals(1, vertices.size());

    } finally {
      noTx.close();
    }
  }

  @Test
  public void compositeIndexSingleFirstFieldTest() {

    OrientGraph noTx = factory.getNoTx();

    try {

      String className = noTx.createVertexClass("Foo");
      OClass foo = noTx.getRawDatabase().getMetadata().getSchema().getClass(className);
      foo.createProperty("prop1", OType.LONG);
      foo.createProperty("prop2", OType.STRING);

      foo.createIndex("V_Foo", OClass.INDEX_TYPE.UNIQUE, "prop1", "prop2");

      noTx.addVertex(T.label, "Foo", "prop1", 1, "prop2", "4ab25da0-3602-4f4a-bc5e-28bfefa5ca4c");

      GraphTraversal<Vertex, Vertex> traversal = noTx.traversal().V().has("Foo", "prop1", 1);

      Assert.assertEquals(1, usedIndexes(noTx, traversal));

      List<Vertex> vertices = traversal.toList();

      Assert.assertEquals(1, vertices.size());

    } finally {
      noTx.close();
    }
  }

  @Test
  public void compositeIndexTest() {

    OrientGraph noTx = factory.getNoTx();

    try {

      String className = noTx.createVertexClass("Foo");
      OClass foo = noTx.getRawDatabase().getMetadata().getSchema().getClass(className);
      foo.createProperty("prop1", OType.LONG);
      foo.createProperty("prop2", OType.STRING);

      foo.createIndex("V_Foo", OClass.INDEX_TYPE.UNIQUE, "prop1", "prop2");

      noTx.addVertex(T.label, "Foo", "prop1", 1, "prop2", "4ab25da0-3602-4f4a-bc5e-28bfefa5ca4c");

      GraphTraversal<Vertex, Vertex> traversal =
          noTx.traversal()
              .V()
              .hasLabel("Foo")
              .has("prop1", 1)
              .has("prop2", "4ab25da0-3602-4f4a-bc5e-28bfefa5ca4c");

      Assert.assertEquals(1, usedIndexes(noTx, traversal));

      List<Vertex> vertices = traversal.toList();

      Assert.assertEquals(1, vertices.size());

    } finally {
      noTx.close();
    }
  }
}
