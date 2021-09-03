package com.orientechnologies.orient.graph.blueprints;

import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GetPropertiesTest {

  private OrientGraph graph;

  @Before
  public void before() {
    graph = new OrientGraph("memory:" + GetPropertiesTest.class.getSimpleName());
  }

  @After
  public void after() {
    graph.drop();
  }

  @Test
  public void getPropertiesFromVertex() {
    OrientVertex v = graph.addVertex(null);
    v.setProperty("test", "test");
    v.setProperty("test1", "test1");

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("aa", 111);
    map.put("bb", "test");
    v.setProperty("testMap", map);

    v.save();

    Map<String, Object> props = v.getProperties();
    Assert.assertEquals(3, props.size());
    Assert.assertEquals("test", props.get("test"));
    Assert.assertEquals("test1", props.get("test1"));
    Assert.assertNotNull(props.get("testMap"));

    map = (Map<String, Object>) props.get("testMap");
    Assert.assertEquals(111, map.get("aa"));
    Assert.assertEquals("test", map.get("bb"));
  }

  @Test
  public void getPropertiesFromEdge() {
    OrientVertex v = graph.addVertex(null);
    OrientVertex v1 = graph.addVertex(null);
    OrientEdge e = (OrientEdge) v.addEdge("test", v1);
    e.setProperty("test", "test");
    e.setProperty("test1", "test1");
    Map<String, Object> props = e.getProperties();
    Assert.assertEquals(2, props.size());
    Assert.assertEquals("test", props.get("test"));
    Assert.assertEquals("test1", props.get("test1"));
  }
}
