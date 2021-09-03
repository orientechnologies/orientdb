package com.tinkerpop.blueprints.impls.orient;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.Vertex;
import org.junit.Test;

/** Created by tglman on 04/01/16. */
public class OrientGraphHookCall {
  private static int vertexCreatedCnt;
  private static int vertexUpdatedCnt;
  private static int edgeCreatedCnt;
  private static int edgeUpdatedCnt;

  @Test
  public void testHook() {
    OrientBaseGraph graph = new OrientGraph("memory:" + OrientGraphHookCall.class.getSimpleName());
    graph
        .getRawGraph()
        .registerHook(
            new ORecordHook() {
              @Override
              public void onUnregister() {}

              @Override
              public RESULT onTrigger(TYPE iType, ORecord iRecord) {
                switch (iType) {
                  case AFTER_CREATE:
                    {
                      if (((ODocument) iRecord)
                          .getSchemaClass()
                          .isSubClassOf(OrientEdgeType.CLASS_NAME)) {
                        edgeCreatedCnt++;
                      } else {
                        vertexCreatedCnt++;
                      }
                      break;
                    }
                  case AFTER_UPDATE:
                    {
                      if (((ODocument) iRecord)
                          .getSchemaClass()
                          .isSubClassOf(OrientEdgeType.CLASS_NAME)) {
                        edgeUpdatedCnt++;
                      } else {
                        vertexUpdatedCnt++;
                      }
                      break;
                    }
                  default:
                    {
                    }
                }
                return null;
              }

              @Override
              public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
                return null;
              }
            });
    try {
      Vertex v1 = graph.addVertex("v", (String) null);
      graph.commit();
      assertEquals(1, vertexCreatedCnt);
      assertEquals(0, vertexUpdatedCnt);
      assertEquals(0, edgeCreatedCnt);
      assertEquals(0, edgeUpdatedCnt);

      v1.setProperty("p1a", "v1a");
      graph.commit();
      assertEquals(1, vertexCreatedCnt);
      assertEquals(1, vertexUpdatedCnt);
      assertEquals(0, edgeCreatedCnt);
      assertEquals(0, edgeUpdatedCnt);

      Vertex v2 = graph.addVertex("v", (String) null);
      graph.commit();
      assertEquals(2, vertexCreatedCnt);
      assertEquals(1, vertexUpdatedCnt);
      assertEquals(0, edgeCreatedCnt);
      assertEquals(0, edgeUpdatedCnt);

      v2.setProperty("p2a", "v2a");
      graph.commit();
      assertEquals(2, vertexCreatedCnt);
      assertEquals(2, vertexUpdatedCnt);
      assertEquals(0, edgeCreatedCnt);
      assertEquals(0, edgeUpdatedCnt);

      v1.addEdge("e", v2);
      graph.commit();
      assertEquals(2, vertexCreatedCnt);
      assertEquals(4, vertexUpdatedCnt);
      assertEquals(1, edgeCreatedCnt);
      assertEquals(0, edgeUpdatedCnt);
    } finally {
      graph.shutdown();
    }
  }
}
