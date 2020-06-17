package com.orientechnologies.orient.graph;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.junit.Test;

public class DirtyManagerGraphTest {

  @Test
  public void testLoopOfNew() {
    OrientGraph graph = new OrientGraph("memory:" + DirtyManagerGraphTest.class.getSimpleName());
    try {
      graph.createEdgeType("next");
      OrientVertex vertex = graph.addVertex(null);
      OrientVertex vertex1 = graph.addVertex(null);
      OrientVertex vertex2 = graph.addVertex(null);
      OrientVertex vertex3 = graph.addVertex(null);

      OrientEdge edge1 = (OrientEdge) vertex.addEdge("next", vertex1);
      OrientEdge edge2 = (OrientEdge) vertex1.addEdge("next", vertex2);
      OrientEdge edge3 = (OrientEdge) vertex2.addEdge("next", vertex3);
      OrientEdge edge4 = (OrientEdge) vertex3.addEdge("next", vertex);
      graph.commit();

      assertTrue(vertex.getIdentity().isPersistent());
      assertTrue(vertex1.getIdentity().isPersistent());
      assertTrue(vertex2.getIdentity().isPersistent());
      assertTrue(vertex3.getIdentity().isPersistent());
      assertTrue(edge1.getIdentity().isPersistent());
      assertTrue(edge2.getIdentity().isPersistent());
      assertTrue(edge3.getIdentity().isPersistent());
      assertTrue(edge4.getIdentity().isPersistent());

    } finally {
      graph.drop();
    }
  }

  @Test
  public void testLoopOfNewTree() {
    OrientGraph graph = new OrientGraph("memory:" + DirtyManagerGraphTest.class.getSimpleName());
    Object prev = OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValue();
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(-1);
    try {
      graph.createEdgeType("next");
      OrientVertex vertex = graph.addVertex(null);
      OrientVertex vertex1 = graph.addVertex(null);
      OrientVertex vertex2 = graph.addVertex(null);
      OrientVertex vertex3 = graph.addVertex(null);

      OrientEdge edge1 = (OrientEdge) vertex.addEdge("next", vertex1);
      OrientEdge edge2 = (OrientEdge) vertex1.addEdge("next", vertex2);
      OrientEdge edge3 = (OrientEdge) vertex2.addEdge("next", vertex3);
      OrientEdge edge4 = (OrientEdge) vertex3.addEdge("next", vertex);
      graph.commit();

      assertTrue(vertex.getIdentity().isPersistent());
      assertTrue(vertex1.getIdentity().isPersistent());
      assertTrue(vertex2.getIdentity().isPersistent());
      assertTrue(vertex3.getIdentity().isPersistent());
      assertTrue(edge1.getIdentity().isPersistent());
      assertTrue(edge2.getIdentity().isPersistent());
      assertTrue(edge3.getIdentity().isPersistent());
      assertTrue(edge4.getIdentity().isPersistent());

    } finally {
      OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(prev);
      graph.drop();
    }
  }
}
