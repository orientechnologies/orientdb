package com.tinkerpop.blueprints.impls.orient;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/** Created by tglman on 03/05/16. */
public class DirtyTrackingTreeRidBagTest {

  private OrientGraphFactory factory;

  @Before
  public void before() {
    factory = new OrientGraphFactory("memory:" + DirtyTrackingTreeRidBagTest.class.getSimpleName());
  }

  @After
  public void after() {
    factory.close();
  }

  @Test
  @Ignore
  public void testConcurrentEdges() {
    final int max =
        OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger() * 2;
    final OrientBaseGraph graph = factory.getTx();
    graph.executeOutsideTx(
        new OCallable<Object, OrientBaseGraph>() {
          @Override
          public Object call(OrientBaseGraph iArgument) {
            graph.createVertexType("Vertex1");
            graph.createVertexType("Vertex2");
            graph.createEdgeType("Edge1");
            return null;
          }
        });
    Vertex rootVertex = graph.addVertex("Vertex1", (String) null);
    graph.commit();
    final Object rootId = rootVertex.getId();
    Runnable tableRun =
        new Runnable() {
          @Override
          public void run() {
            OrientBaseGraph innerGraph = DirtyTrackingTreeRidBagTest.this.factory.getTx();
            Vertex innerRoot = innerGraph.getVertex(rootId);
            for (int i = 0; i < max; i++) {
              Vertex v = innerGraph.addVertex("Vertex2", (String) null);
              innerGraph.commit();
              graph.addEdge(null, innerRoot, v, "Edge1");
              graph.commit();
            }
          }
        };
    Thread tableT = new Thread(tableRun);
    tableT.start();
    try {
      tableT.join();
    } catch (InterruptedException e) {
      System.out.println("Join interrupted " + e);
    }
    graph.getRawGraph().getLocalCache().clear();
    rootVertex = graph.getVertex(rootId);
    assertEquals(new GremlinPipeline<Vertex, Long>().start(rootVertex).out("Edge1").count(), max);
    graph.shutdown();
  }
}
