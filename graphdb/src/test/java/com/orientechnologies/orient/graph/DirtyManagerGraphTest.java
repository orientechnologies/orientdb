package com.orientechnologies.orient.graph;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODirtyManager;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DirtyManagerGraphTest {

  @Test
  @Ignore
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

      ODocument rec = vertex.getRecord();
      ODirtyManager manager = ORecordInternal.getDirtyManager(rec);

      List<OIdentifiable> pointed = manager.getPointed(vertex.getRecord());
      assertThat(pointed).isNotNull();
      assertEquals(2, pointed.size());
      assertTrue(pointed.contains(edge1.getRecord()));
      assertTrue(pointed.contains(edge4.getRecord()));

      pointed = manager.getPointed(vertex1.getRecord());
      assertEquals(2, pointed.size());
      assertTrue(pointed.contains(edge1.getRecord()));
      assertTrue(pointed.contains(edge2.getRecord()));

      pointed = manager.getPointed(vertex2.getRecord());
      assertEquals(2, pointed.size());
      assertTrue(pointed.contains(edge2.getRecord()));
      assertTrue(pointed.contains(edge3.getRecord()));

      pointed = manager.getPointed(vertex3.getRecord());
      assertEquals(2, pointed.size());
      assertTrue(pointed.contains(edge3.getRecord()));
      assertTrue(pointed.contains(edge4.getRecord()));

      pointed = manager.getPointed(edge1.getRecord());
      assertEquals(2, pointed.size());
      assertTrue(pointed.contains(vertex.getRecord()));
      assertTrue(pointed.contains(vertex1.getRecord()));

      pointed = manager.getPointed(edge2.getRecord());
      assertEquals(2, pointed.size());
      assertTrue(pointed.contains(vertex1.getRecord()));
      assertTrue(pointed.contains(vertex2.getRecord()));

      pointed = manager.getPointed(edge3.getRecord());
      assertEquals(2, pointed.size());
      assertTrue(pointed.contains(vertex2.getRecord()));
      assertTrue(pointed.contains(vertex3.getRecord()));

      pointed = manager.getPointed(edge4.getRecord());
      assertEquals(2, pointed.size());
      assertTrue(pointed.contains(vertex3.getRecord()));
      assertTrue(pointed.contains(vertex.getRecord()));

    } finally {
      graph.drop();
    }
  }

  @Test
  @Ignore
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

      ODocument rec = vertex.getRecord();
      ODirtyManager manager = ORecordInternal.getDirtyManager(rec);

      List<OIdentifiable> pointed = manager.getPointed(vertex.getRecord());
      assertThat(pointed).isNotNull();
      assertEquals(2, pointed.size());
      assertTrue(pointed.contains(edge1.getRecord()));
      assertTrue(pointed.contains(edge4.getRecord()));

      pointed = manager.getPointed(vertex1.getRecord());
      assertEquals(2, pointed.size());
      assertTrue(pointed.contains(edge1.getRecord()));
      assertTrue(pointed.contains(edge2.getRecord()));

      pointed = manager.getPointed(vertex2.getRecord());
      assertEquals(2, pointed.size());
      assertTrue(pointed.contains(edge2.getRecord()));
      assertTrue(pointed.contains(edge3.getRecord()));

      pointed = manager.getPointed(vertex3.getRecord());
      assertEquals(2, pointed.size());
      assertTrue(pointed.contains(edge3.getRecord()));
      assertTrue(pointed.contains(edge4.getRecord()));

      pointed = manager.getPointed(edge1.getRecord());
      assertEquals(2, pointed.size());
      assertTrue(pointed.contains(vertex.getRecord()));
      assertTrue(pointed.contains(vertex1.getRecord()));

      pointed = manager.getPointed(edge2.getRecord());
      assertEquals(2, pointed.size());
      assertTrue(pointed.contains(vertex1.getRecord()));
      assertTrue(pointed.contains(vertex2.getRecord()));

      pointed = manager.getPointed(edge3.getRecord());
      assertEquals(2, pointed.size());
      assertTrue(pointed.contains(vertex2.getRecord()));
      assertTrue(pointed.contains(vertex3.getRecord()));

      pointed = manager.getPointed(edge4.getRecord());
      assertEquals(2, pointed.size());
      assertTrue(pointed.contains(vertex3.getRecord()));
      assertTrue(pointed.contains(vertex.getRecord()));

    } finally {
      OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(prev);
      graph.drop();
    }
  }

}
