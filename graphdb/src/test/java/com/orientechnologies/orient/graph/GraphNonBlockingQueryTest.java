package com.orientechnologies.orient.graph;

import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLNonBlockingQuery;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

/** Created by tglman on 01/07/16. */
public class GraphNonBlockingQueryTest {

  @Test
  public void testNonBlockingClose() throws ExecutionException, InterruptedException {
    OrientGraph database =
        new OrientGraph("memory:" + GraphNonBlockingQueryTest.class.getSimpleName());
    database.createVertexType("Prod").createProperty("something", OType.STRING);
    for (int i = 0; i < 21; i++) {
      OrientVertex vertex = database.addVertex(null);
      vertex.setProperty("something", "value");
      vertex.save();
    }
    try {
      OSQLNonBlockingQuery<Object> test =
          new OSQLNonBlockingQuery<Object>(
              "select * from Prod ",
              new OCommandResultListener() {
                int resultCount = 0;

                @Override
                public boolean result(Object iRecord) {
                  resultCount++;

                  ODocument odoc = ((ODocument) iRecord);
                  ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().get();
                  System.out.println(db);
                  for (String name : odoc.fieldNames()) { //  <----------- PROBLEM
                    System.out.println("Name:" + name);
                  }

                  System.out.println("callback " + resultCount + " invoked");
                  return resultCount > 20 ? false : true;
                }

                @Override
                public void end() {}

                @Override
                public Object getResult() {
                  return true;
                }
              });

      database.command(test).execute();
      System.out.println("query executed");
    } finally {
      System.out.println("Original" + database);
      database.shutdown();
    }
  }
}
