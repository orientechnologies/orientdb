package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.OVertex;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by Enrico Risa on 19/05/2017.
 */
public class OrientGraphTxTest extends OrientGraphBaseTest {

    @Override
    public void setupDB() {
        super.setupDB();

        OrientGraph noTx = factory.getNoTx();

        noTx.executeSql("CREATE CLASS Person EXTENDS V");
        noTx.executeSql("CREATE CLASS HasFriend EXTENDS E");
        noTx.executeSql("CREATE SEQUENCE personIdSequence TYPE ORDERED;");
        noTx.executeSql("CREATE PROPERTY Person.id LONG (MANDATORY TRUE, default \"sequence('personIdSequence').next()\");");
        noTx.executeSql("CREATE INDEX Person.id ON Person (id) UNIQUE");

        noTx.close();
    }

    @Test
    public void txSequenceTest() {

        OrientGraph tx = factory.getTx();
        Vertex vertex = tx.addVertex(T.label, "Person", "name", "John");
        for (int i = 0; i < 10; i++) {
            Vertex vertex1 = tx.addVertex(T.label, "Person", "name", "Frank" + i);
            vertex.addEdge("HasFriend", vertex1);
        }
        tx.commit();

        Assert.assertEquals(11, tx.getRawDatabase().countClass("Person"));

        tx.close();
    }

    @Test(expected = IllegalStateException.class)
    public void txManualOpenExceptionTest() {

        OrientGraph tx = factory.getTx();

        tx.tx().onReadWrite(Transaction.READ_WRITE_BEHAVIOR.MANUAL);

        tx.addVertex(T.label, "Person", "name", "John");

    }

    @Test
    public void txManualOpen() {

        OrientGraph tx = factory.getTx();

        tx.tx().onReadWrite(Transaction.READ_WRITE_BEHAVIOR.MANUAL);

        tx.tx().open();

        tx.addVertex(T.label, "Person", "name", "John");

        tx.close();

        tx = factory.getTx();

        Assert.assertEquals(0, tx.getRawDatabase().countClass("Person"));

    }

    @Test
    public void txManualOpenCommitOnClose() {

        OrientGraph tx = factory.getTx();

        tx.tx().onReadWrite(Transaction.READ_WRITE_BEHAVIOR.MANUAL);

        tx.tx().onClose(Transaction.CLOSE_BEHAVIOR.COMMIT);

        tx.tx().open();

        tx.addVertex(T.label, "Person", "name", "John");

        tx.close();

        tx = factory.getTx();

        Assert.assertEquals(1, tx.getRawDatabase().countClass("Person"));

    }

    @Test
    public void txCommitOnClose() {

        OrientGraph tx = factory.getTx();

        tx.tx().onClose(Transaction.CLOSE_BEHAVIOR.COMMIT);

        tx.addVertex(T.label, "Person", "name", "John");

        tx.close();

        tx = factory.getTx();

        Assert.assertEquals(1, tx.getRawDatabase().countClass("Person"));

    }

    @Test
    public void txSequenceTestRollback() {

        OrientGraph tx = factory.getTx();
        Vertex vertex = tx.addVertex(T.label, "Person", "name", "John");
        for (int i = 0; i < 10; i++) {
            Vertex vertex1 = tx.addVertex(T.label, "Person", "name", "Frank" + i);
            vertex.addEdge("HasFriend", vertex1);
        }
        tx.rollback();

        Assert.assertEquals(0, tx.getRawDatabase().countClass("Person"));

        tx.close();
    }

    @Test
    public void testAutoStartTX() {

        OrientGraph tx = factory.getTx();

        Assert.assertEquals(false, tx.tx().isOpen());

        tx.addVertex("Person");

        Assert.assertEquals(true, tx.tx().isOpen());

        tx.close();

        tx = factory.getTx();

        Assert.assertEquals(0, tx.getRawDatabase().countClass("Person"));
    }

    @Test
    public void testOrientDBTX() {

        OrientDB orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
        orientDB.create("testTX", ODatabaseType.MEMORY);
        ODatabaseDocument db = orientDB.open("testTX", "admin", "admin");

        db.begin();
        OVertex v = db.newVertex("V");
        v.setProperty("name", "Foo");
        db.save(v);
        db.commit();

        db.begin();
        v.setProperty("name", "Bar");
        db.save(v);
        db.rollback();

        Assert.assertEquals("Foo", v.getProperty("name"));
        db.close();
        orientDB.close();
    }
}
