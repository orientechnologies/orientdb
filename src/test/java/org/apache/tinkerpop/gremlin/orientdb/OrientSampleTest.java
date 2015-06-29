package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.db.ODatabaseFactory;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.lang.System;
import java.util.Collection;

import static org.junit.Assert.*;

public class OrientSampleTest {

    @Test
    public void labelTest() {
        // remote/local work, memory doesn't
        String graphUri = "memory:test";
//        String graphUri = "plocal:target/graph" + Math.random();
//        String graphUri = "remote:localhost/test";
//        OrientGraph graph = new OrientGraphFactory(graphUri, "root", "root").getTx();

//        ODatabaseDocumentTx db = new ODatabaseDocumentTx(graphUri);
//        ODatabaseDocumentTx db = new ODatabaseFactory().createDatabase("graph", graphUri);
//        db.create();
//        db.open("root", "root");
//        System.out.println(db.getMetadata().getSchema().getClasses());


//        try {
//            Thread.sleep(1000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

//        OrientVertex v1 = (OrientVertex) g.addVertex("label1");
//
//        OClass clazz = v1.getRawDocument().getSchemaClass();
//        Collection<OClass> subclasses = clazz.getSubclasses();
//        System.out.println("Subclasses of vertex: " + subclasses.size());
//        subclasses.forEach(c -> System.out.println(c));
        System.out.println("blub");
    }

}