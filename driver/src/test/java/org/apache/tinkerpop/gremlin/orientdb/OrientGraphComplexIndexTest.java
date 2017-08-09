package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class OrientGraphComplexIndexTest extends OrientGraphBaseTest {

    // TODO Enable when it's fixed
    //  @Test
    public void compositeIndexSingleSecondFieldTest() {

        OrientGraph noTx = factory.getNoTx();

        try {

            String className = noTx.createVertexClass("Foo");
            OClass foo = noTx.getRawDatabase().getMetadata().getSchema().getClass(className);
            foo.createProperty("prop1", OType.LONG);
            foo.createProperty("prop2", OType.STRING);

            foo.createIndex("V_Foo", OClass.INDEX_TYPE.UNIQUE, "prop1", "prop2");

            noTx.addVertex(T.label, "Foo", "prop1", 1, "prop2", "4ab25da0-3602-4f4a-bc5e-28bfefa5ca4c");

            List<Vertex> vertices = noTx.traversal().V().has("Foo", "prop2", "4ab25da0-3602-4f4a-bc5e-28bfefa5ca4c").toList();

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

            List<Vertex> vertices = noTx.traversal().V().has("Foo", "prop1", 1).toList();

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

            List<Vertex> vertices = noTx.traversal().V().hasLabel("Foo").has("prop1", 1)
                    .has("prop2", "4ab25da0-3602-4f4a-bc5e-28bfefa5ca4c").toList();

            Assert.assertEquals(1, vertices.size());

        } finally {
            noTx.close();
        }

    }

}
