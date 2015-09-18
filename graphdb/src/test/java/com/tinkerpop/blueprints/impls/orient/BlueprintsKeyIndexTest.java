package com.tinkerpop.blueprints.impls.orient;

import com.tinkerpop.blueprints.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(JUnit4.class)
public class BlueprintsKeyIndexTest {

  private static final String ROOT_NODE_NAME = "rootNode";
  private static final String KEY_NAME       = "name";

  @Test
  public void test_with_createKeyIndex() throws Exception {
    final OrientGraph graph = new OrientGraph("memory:" + BlueprintsKeyIndexTest.class.getSimpleName());
    graph.setWarnOnForceClosingTx(false);
    try {
      /* create key index */
      graph.createKeyIndex(KEY_NAME, Vertex.class);

      /* create the root vertex */{
        final Vertex v = graph.addVertex(null);
        v.setProperty(KEY_NAME, ROOT_NODE_NAME); /* as key index */
        graph.commit();

        final Object rootVertexId = v.getId();
        assertNotNull(rootVertexId);
      }

      /* get rootNode */
      final List<Vertex> rootNodes = toArrayList(graph.getVertices(KEY_NAME, ROOT_NODE_NAME));
      assertEquals(1, rootNodes.size()); // ##########
      // java.lang.AssertionError:
      // expected:<1> but was:<0>
    } finally {
      graph.drop();
    }
  }

  @Test
  public void test_without_createKeyIndex() throws Exception {
    final OrientGraph graph = new OrientGraph("memory:" + BlueprintsKeyIndexTest.class.getSimpleName());
    graph.setWarnOnForceClosingTx(false);
    try {
      /* create key index */
      // graph.createKeyIndex("name", Vertex.class);

      /* create the root vertex */{
        final Vertex v = graph.addVertex(null);
        v.setProperty(KEY_NAME, ROOT_NODE_NAME); /* as key index */
        graph.commit();

        final Object rootVertexId = v.getId();
        assertNotNull(rootVertexId);
      }

      /* get rootNode */
      final List<Vertex> rootNodes = toArrayList(graph.getVertices(KEY_NAME, ROOT_NODE_NAME));
      assertEquals(1, rootNodes.size()); // ########## no problem
    } finally {
      graph.drop();
    }
  }

  @Test
  public void test_without_createKeyIndexVertexType() throws Exception {
    final OrientGraph graph = new OrientGraph("memory:" + BlueprintsKeyIndexTest.class.getSimpleName());
    graph.setWarnOnForceClosingTx(false);
    graph.createVertexType("Test");

    graph.createVertexType("Test1");
    try {
      /* create key index */
      // graph.createKeyIndex("name", Vertex.class);

      /* create the root vertex */{
        Vertex v = graph.addVertex("class:Test");
        v.setProperty(KEY_NAME, ROOT_NODE_NAME); /* as key index */

        v = graph.addVertex("class:Test1");
        v.setProperty(KEY_NAME, ROOT_NODE_NAME);

        v = graph.addVertex("class:Test1");
        v.setProperty(KEY_NAME, "Fail");

        graph.commit();

        final Object rootVertexId = v.getId();
        assertNotNull(rootVertexId);
      }

      /* get rootNode */
      final List<Vertex> rootNodes = toArrayList(graph.getVertices("Test." + KEY_NAME, ROOT_NODE_NAME));
      assertEquals(1, rootNodes.size()); // ########## no problem
    } finally {
      graph.drop();
    }
  }

  // ///

  public static <E> ArrayList<E> toArrayList(final Iterable<E> iterable) {
    if (iterable instanceof ArrayList) {
      return (ArrayList<E>) iterable;
    }
    final ArrayList<E> list = new ArrayList<E>();
    if (iterable != null) {
      for (E e : iterable) {
        list.add(e);
      }
    }
    return list;
  }
}
