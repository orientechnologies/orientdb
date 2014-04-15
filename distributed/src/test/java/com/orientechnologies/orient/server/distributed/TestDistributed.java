//package com.orientechnologies.orient.server.distributed;
//
//import com.hazelcast.core.Hazelcast;
//import com.hazelcast.core.HazelcastInstance;
//import com.orientechnologies.common.io.OFileUtils;
//import com.orientechnologies.orient.core.record.impl.ODocument;
//import com.orientechnologies.orient.server.OServer;
//import com.orientechnologies.orient.server.OServerMain;
//import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;
//import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
//import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.io.File;
//import java.io.FileNotFoundException;
//import java.util.ArrayList;
//import java.util.Collections;
//
//public class TestDistributed {
//
//  private OServer server;
//
//  public static class StandaloneHazelcastPlugin extends OHazelcastPlugin {
//
//    @Override
//    protected HazelcastInstance configureHazelcast() throws FileNotFoundException {
//      return Hazelcast.newHazelcastInstance();
//    }
//
//    @Override
//    protected ODocument loadDatabaseConfiguration(String iDatabaseName, File file) {
//      ODocument doc = new ODocument();
//      doc.field("replication", true)
//          .field("autoDeploy", true)
//          .field("hotAlignment", true)
//          .field("resyncEvery", 15)
//          .field(
//              "clusters",
//              new ODocument()
//                  .field("internal", new ODocument().field("replication", false))
//                  .field("index", new ODocument().field("replication", false))
//                  .field(
//                      "*",
//                      new ODocument()
//                          .field("replication", true)
//                          .field("readQuorum", 1)
//                          .field("writeQuorum", 1)
//                          .field("failureAvailableNodesLessQuorum", false)
//                          .field("readYourWrites", true)
//                          .field(
//                              "partitioning",
//                              new ODocument()
//                                  .field("strategy", "round-robin")
//                                  .field("default", 0)
//                                  .field("partitions", Collections.singletonList(new ArrayList<String>(Collections.singletonList("<NEW_NODE>")))))));
//
//      return doc;
//    }
//  }
//
//  @Before
//  public void setUp() throws Exception {
//    File target = new File("target/testdb");
//    OFileUtils.deleteRecursively(target);
//    target.mkdirs();
//
//    server = OServerMain.create();
//    server
//        .startup("<orient-server>"
//            + "<handlers>"
//            + "<handler class=\""
//            + StandaloneHazelcastPlugin.class.getName()
//            + "\">"
//            + "<parameters>"
//            + "<parameter name=\"enabled\" value=\"true\" />"
//            + "<parameter name=\"sharding.strategy.round-robin\" value=\"com.orientechnologies.orient.server.hazelcast.sharding.strategy.ORoundRobinPartitioninStrategy\" />"
//            + "</parameters>"
//            + "</handler>"
//            + "</handlers>"
//            + "<network><protocols></protocols><listeners></listeners><cluster></cluster></network><storages></storages><users></users>"
//            + "<properties><entry name=\"server.database.path\" value=\"target/\"/></properties>" + "</orient-server>");
//    server.activate();
//  }
//
//  @After
//  public void tearDown() {
//    server.shutdown();
//  }
//
//  @Test
//  public void testCreateClass() {
//    OrientGraphFactory factory = new OrientGraphFactory("plocal:target/testdb");
//    OrientGraphNoTx graph = factory.getNoTx();
//
//    graph.addVertex(null);
//  }
//}
