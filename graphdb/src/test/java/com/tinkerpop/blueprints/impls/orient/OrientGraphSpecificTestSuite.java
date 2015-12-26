package com.tinkerpop.blueprints.impls.orient;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.TestSuite;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.GraphTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(JUnit4.class)
public class OrientGraphSpecificTestSuite extends TestSuite {
  public OrientGraphSpecificTestSuite() {
    graphTest = new OrientGraphDefault();
  }

  public OrientGraphSpecificTestSuite(final GraphTest graphTest) {
    super(graphTest);
  }

  @Test
  public void testGetEdgesWithTargetVertex() throws Exception {
    Graph graph = graphTest.generateGraph();

    OrientVertex v1 = (OrientVertex) graph.addVertex(null);
    OrientVertex v2 = (OrientVertex) graph.addVertex(null);
    OrientVertex v3 = (OrientVertex) graph.addVertex(null);
    v1.addEdge("targets", v2);
    v1.addEdge("targets", v3);

    Assert.assertEquals(1, count(v1.getEdges(v2, Direction.OUT, "targets")));
    Assert.assertEquals(0, count(v1.getEdges(v2, Direction.IN, "targets")));
    Assert.assertEquals(0, count(v1.getEdges(v2, Direction.OUT, "aaa")));
    Assert.assertEquals(1, count(v1.getEdges(v3, Direction.OUT, "targets")));
    Assert.assertEquals(0, count(v1.getEdges(v3, Direction.OUT, "bbb")));
    Assert.assertEquals(1, count(v1.getEdges(v2, Direction.OUT)));
    Assert.assertEquals(1, count(v1.getEdges(v3, Direction.OUT)));

    Assert.assertEquals(1, count(v2.getEdges(v1, Direction.IN, "targets")));
    Assert.assertEquals(0, count(v2.getEdges(v1, Direction.OUT, "targets")));
    Assert.assertEquals(0, count(v2.getEdges(v1, Direction.IN, "aaa")));
    Assert.assertEquals(1, count(v2.getEdges(v1, Direction.IN, "targets")));
    Assert.assertEquals(0, count(v2.getEdges(v1, Direction.IN, "bbb")));
    Assert.assertEquals(1, count(v2.getEdges(v1, Direction.IN)));

    graph.shutdown();
  }

  @Test
  public void testComplexMapProperty() throws Exception {
    // complex map properties have problems when unmarshalled from disk to
    // an OTrackedMap
    Graph graph = graphTest.generateGraph("complex-map");
    final HashMap<String, Object> consignee = new HashMap<String, Object>();
    consignee.put("name", "Company 4");
    final ArrayList consigneeAddress = new ArrayList();
    consigneeAddress.add("Lilla Bommen 6");
    consignee.put("address", consigneeAddress);
    consignee.put("zipCode", "41104");
    consignee.put("city", "G\u00f6teborg");
    final HashMap<String, Object> consigneeCountry = new HashMap<String, Object>();
    consigneeCountry.put("name", "Sverige");
    consigneeCountry.put("code", "SV");
    consignee.put("country", consigneeCountry);
    consignee.put("contactName", "Contact Person 4");
    consignee.put("telephone", "0731123456");
    consignee.put("telefax", null);
    consignee.put("mobileTelephone", "072345678");
    consignee.put("email", "test@company4.com");
    consignee.put("hiflexId", null);

    final HashMap<String, Object> delivery = new HashMap<String, Object>();
    delivery.put("name", "Company 5");
    final ArrayList deliveryAddress = new ArrayList();
    deliveryAddress.add("Stora Enens V\u00e4g 38");
    delivery.put("address", deliveryAddress);
    delivery.put("zipCode", "43931");
    delivery.put("city", "Onsala");
    final HashMap<String, Object> deliveryCountry = new HashMap<String, Object>();
    deliveryCountry.put("name", "Sverige");
    deliveryCountry.put("code", "SV");
    delivery.put("country", deliveryCountry);
    delivery.put("contactName", "Contact Person 5");
    delivery.put("telephone", "030060094");
    delivery.put("telefax", null);
    delivery.put("mobileTelephone", null);
    delivery.put("email", "test@company5.com");
    delivery.put("hiflexId", null);

    final HashMap<String, Object> pickup = new HashMap<String, Object>();
    pickup.put("name", "Pickup Company 2");
    final ArrayList pickupAddress = new ArrayList();
    pickupAddress.add("Drottninggatan 1");
    pickup.put("address", pickupAddress);
    pickup.put("zipCode", "41103");
    pickup.put("city", "G\u00f6teborg");
    final HashMap<String, Object> pickupCountry = new HashMap<String, Object>();
    pickupCountry.put("name", "Sverige");
    pickupCountry.put("code", "SV");
    pickup.put("country", pickupCountry);
    pickup.put("contactName", "Contact Person 6");
    pickup.put("telephone", "071234567");
    pickup.put("telefax", null);
    pickup.put("mobileTelephone", null);
    pickup.put("email", "test@pickupcompany2.com");
    pickup.put("hiflexId", null);

    final Map shipping = new HashMap();
    shipping.put("name", "Posten MyPack");
    shipping.put("code", "postenmypack");
    shipping.put("templateName", "POSTENMYPACK");
    shipping.put("rates", new ArrayList());

    final Vertex v = graph.addVertex(null);
    v.setProperty("weight", 20);
    v.setProperty("height", 20);
    v.setProperty("consigneeAddress", consignee);
    v.setProperty("width", 10);
    v.setProperty("sum", 400);
    v.setProperty("shippingMethod", shipping);
    v.setProperty("type", "shipment");
    v.setProperty("depth", 30);
    v.setProperty("estimatedCost", 200);
    v.setProperty("deliveryAddress", delivery);
    v.setProperty("pickupAddress", pickup);

    ((TransactionalGraph) graph).commit();

    // have to shutdown the graph so that the map will read back out as an
    // OTrackedMap. Maps that exist in memory
    // do not show the problem.
    graph.shutdown();

    graph = graphTest.generateGraph("complex-map");

    final Vertex v1 = graph.getVertex(v.getId());
    assertNotNull(v1);

    // check the delivery address. not sure if there should be other
    // assertions here, but the basic issues
    // is that the keys/values in the OTrackedMap appear like this:
    // mobileTelephone=null:null
    final Map d = v1.getProperty("deliveryAddress");
    assertNotNull(d);
    assertTrue(d.containsKey("telefax"));
    graph.shutdown();
    graphTest.dropGraph(((OrientGraphTest) graphTest).getWorkingDirectory() + File.separator + "complex-map");
  }
}
