package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.stream.Collectors.toMap;
import static org.apache.tinkerpop.gremlin.structure.Transaction.CLOSE_BEHAVIOR.COMMIT;
import static org.apache.tinkerpop.gremlin.structure.Transaction.CLOSE_BEHAVIOR.ROLLBACK;
import static org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality.single;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class OrientGraphPoolTest {

  protected OrientGraphFactory graphFactory() {
    return new OrientGraphFactory("memory:tinkerpop-" + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE)).setupPool(1, 10);
  }

  public static final String TEST_VALUE = "SomeValue";

  class TestData {
    String id;
    String str;

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public String getStr() {
      return str;
    }

    public void setStr(String str) {
      this.str = str;
    }
  }

  @Test
  public void testConcurrentSave() throws Exception {

    ExecutorService service = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    OrientGraphFactory factory = graphFactory();

    try (OrientGraph noTx = factory.getNoTx()) {

      ODatabaseDocument db = noTx.getRawDatabase();
      OClass testData = db.createVertexClass("TestData");
      OProperty id = testData.createProperty("id", OType.STRING);
      id.createIndex(OClass.INDEX_TYPE.UNIQUE);

    }

    List<TestData> ts = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      TestData testData = new TestData();
      testData.setId(1 + "");
      testData.setStr("1");
      ts.add(testData);
    }
    int count = 50;
    CountDownLatch countDownLatch = new CountDownLatch(count);
    for (int i = 0; i < count; i++) {
      int finalI = i;
      CompletableFuture.supplyAsync(() -> {

        try (OrientGraph graph = factory.getNoTx()) {
          GraphTraversalSource traversal = graph.traversal();
          for (TestData t : ts) {
            try {
              traversal.addV("TestData").property("id", t.getId()).property("str", t.getStr()).next();
            } catch (ORecordDuplicatedException e) {

            }
          }
          return finalI;
        }
      }, service).whenComplete((exeTask, throwable) -> {
        System.out.println(exeTask);
        countDownLatch.countDown();
        if (throwable != null) {
          throwable.printStackTrace();
        }
      });
    }
    countDownLatch.await();

    OrientGraph graph = factory.getNoTx();

    graph.getRawDatabase().reload();

    assertEquals(1, graph.getRawDatabase().countClass("TestData"));
  }
}
