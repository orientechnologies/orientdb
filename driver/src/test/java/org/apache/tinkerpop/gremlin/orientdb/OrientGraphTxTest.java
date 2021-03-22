package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.OVertex;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Assert;
import org.junit.Test;

/** Created by Enrico Risa on 19/05/2017. */
public class OrientGraphTxTest extends OrientGraphBaseTest {

  @Override
  public void setupDB() {
    super.setupDB();

    OrientGraph noTx = factory.getNoTx();

    noTx.executeSql("CREATE CLASS Person EXTENDS V");
    noTx.executeSql("CREATE CLASS HasFriend EXTENDS E");
    noTx.executeSql("CREATE SEQUENCE personIdSequence TYPE ORDERED;");
    noTx.executeSql(
        "CREATE PROPERTY Person.id LONG (MANDATORY TRUE, default \"sequence('personIdSequence').next()\");");
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

  @Test
  public void testSequencesParallel() {
    System.out.println("testSequencesParallel");
    ODatabaseDocument db = factory.getDatabase(true, true);
    db.activateOnCurrentThread();
    db.execute(
        "sql",
        "CREATE CLASS TestSequence EXTENDS V;\n"
            + " CREATE SEQUENCE TestSequenceIdSequence TYPE CACHED;\n"
            + "CREATE PROPERTY TestSequence.mm LONG (MANDATORY TRUE, default \"sequence('TestSequenceIdSequence').next()\");\n");

    OrientGraph graph = null;
    final int recCount = 50;
    final int threadCount = 100;
    try {
      Thread[] threads = new Thread[threadCount];
      for (int j = 0; j < threadCount; j++) {
        final int index = j;
        Thread thread =
            new Thread(
                new Runnable() {
                  @Override
                  public void run() {
                    OrientGraph graph = null;
                    try {
                      if (index % 2 == 0) {
                        graph = factory.getNoTx();
                      } else {
                        graph = factory.getTx();
                      }
                      for (int i = 0; i < recCount; i++) {
                        graph.addVertex("TestSequence");
                      }
                      if (index % 2 != 0) {
                        graph.commit();
                      }
                    } finally {
                      if (graph != null) {
                        graph.close();
                      }
                    }
                  }
                });
        threads[j] = thread;
        thread.start();
      }

      for (int i = 0; i < threads.length; i++) {
        try {
          threads[i].join();
        } catch (InterruptedException exc) {
          exc.printStackTrace();
        }
      }

      graph = factory.getNoTx();
      Iterator<Vertex> iter = graph.vertices();

      int counter = 0;
      Set<Long> vals = new HashSet<>();
      while (iter.hasNext()) {
        Vertex v = iter.next();
        VertexProperty<Long> vp = v.property("mm");
        long a = vp.value();
        Assert.assertFalse(vals.contains(a));
        vals.add(a);
        counter++;
      }
      Assert.assertEquals(counter, threadCount * recCount);
    } finally {
      if (graph != null) {
        graph.close();
      }
      db.activateOnCurrentThread();
      db.execute("sql", "DROP CLASS TestSequence UNSAFE");
    }
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
    orientDB.execute(
        "create database testTX memory users (admin identified by 'admin' role admin)");
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
