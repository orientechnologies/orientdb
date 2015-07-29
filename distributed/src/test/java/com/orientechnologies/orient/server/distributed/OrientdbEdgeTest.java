/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class OrientdbEdgeTest {
  private static OServer server;

  static {
    System.setProperty("ORIENTDB_ROOT_PASSWORD", "root");
  }

  public OrientdbEdgeTest() {
  }

  @AfterClass
  public static void tearDownClass() {
    if (server != null)
      server.shutdown();
  }

  protected static OrientGraphFactory getGraphFactory() throws Exception {
    Map<String, Object> conf = new HashMap<String, Object>();

    conf.put("storage.url", "remote:localhost/test");
    conf.put("storage.pool-min", 1);
    conf.put("storage.pool-max", 10);
    conf.put("storage.user", "admin");
    conf.put("storage.password", "admin");

    OGlobalConfiguration.CLIENT_CONNECT_POOL_WAIT_TIMEOUT.setValue(15000);

    verifyDatabaseExists(conf);

    return new OrientGraphFactory((String) conf.get("storage.url"), (String) conf.get("storage.user"),
        (String) conf.get("storage.password")).setupPool((Integer) conf.get("storage.pool-min"),
        (Integer) conf.get("storage.pool-max"));
  }

  @BeforeClass
  public static void setup() throws Exception {
    File file = new File("./target/databases/");
    if (file.exists())
      OFileUtils.deleteRecursively(file);
    file.mkdirs();

    server = OServerMain.create();
    server
        .startup("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
            + "<orient-server>\n"
            + "    <handlers>\n"
            + "        <!-- GRAPH PLUGIN -->\n"
            + "        <handler class=\"com.orientechnologies.orient.graph.handler.OGraphServerHandler\">\n"
            + "            <parameters>\n"
            + "                <parameter name=\"enabled\" value=\"true\"/>\n"
            + "                <parameter name=\"graph.pool.max\" value=\"50\"/>\n"
            + "            </parameters>\n"
            + "        </handler>\n"
            + "       \n"
            + "<handler class=\"com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin\">\n"
            + "            <parameters>\n"
            + "                <parameter name=\"nodeName\" value=\"unittest\" />\n"
            + "                <parameter name=\"enabled\" value=\"true\"/>\n"
            + "                <parameter name=\"configuration.db.default\"\n"
            + "                           value=\"src/test/resources/default-distributed-db-config.json\"/>\n"
            + "                <parameter name=\"configuration.hazelcast\" value=\"config/hazelcast.xml\"/>\n"
            + "\n"
            + "                <!-- PARTITIONING STRATEGIES -->\n"
            + "                <parameter name=\"sharding.strategy.round-robin\"\n"
            + "                           value=\"com.orientechnologies.orient.server.hazelcast.sharding.strategy.ORoundRobinPartitioninStrategy\"/>\n"
            + "            </parameters>\n"
            + "        </handler>"
            + "    </handlers>\n"
            + "    <network>\n"
            + "        <protocols>\n"
            + "            <protocol name=\"binary\"\n"
            + "                      implementation=\"com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary\"/>\n"
            + "        </protocols>\n"
            + "        <listeners>\n"
            + "            <listener protocol=\"binary\" ip-address=\"0.0.0.0\" port-range=\"2424-2430\"/>\n"
            + "        </listeners>\n"
            + "        <cluster>\n"
            + "        </cluster>\n"
            + "    </network>\n"
            + "    <storages>\n"
            + "    </storages>\n"
            + "    <users>\n"
            + "      <user name=\"admin\" password=\"admin\" resources=\"*\"/>\n"
            + "    </users>\n"
            + "    <properties>\n"
            + "\n"
            + "        <!-- Uses the Hazelcast's distributed cache as 2nd level cache -->\n"
            + "        <!-- <entry name=\"cache.level2.impl\" value=\"com.orientechnologies.orient.server.hazelcast.OHazelcastCache\" /> -->\n"
            + "\n"
            + "        <!-- DATABASE POOL: size min/max -->\n"
            + "        <entry name=\"db.pool.min\" value=\"1\"/>\n"
            + "        <entry name=\"db.pool.max\" value=\"20\"/>\n"
            + "\n"
            + "        <!-- LEVEL1 AND 2 CACHE: enable/disable and set the size as number of entries -->\n"
            + "        <entry name=\"cache.level1.enabled\" value=\"false\"/>\n"
            + "        <entry name=\"cache.level1.size\" value=\"1000\"/>\n"
            + "        <entry name=\"cache.level2.enabled\" value=\"false\"/>\n"
            + "        <entry name=\"cache.level2.size\" value=\"1000\"/>\n"
            + "\n"
            + "<entry name=\"server.database.path\" value=\"target/databases\" />"
            + "        <!-- PROFILER: configures the profiler as <seconds-for-snapshot>,<archive-snapshot-size>,<summary-size>  -->\n"
            + "        <entry name=\"profiler.enabled\" value=\"true\"/>\n"
            + "        <!-- <entry name=\"profiler.config\" value=\"30,10,10\" />  -->\n" + "\n"
            + "        <!-- LOG: enable/Disable logging. Levels are: finer, fine, finest, info, warning -->\n"
            + "        <entry name=\"log.console.level\" value=\"finest\"/>\n"
            + "        <entry name=\"log.file.level\" value=\"finest\"/>\n" + "    </properties>\n" + "</orient-server>");

    server.activate();
  }

  private static void verifyDatabaseExists(Map<String, Object> conf) {
    final String url = (String) conf.get("storage.url");

    if (!url.startsWith("remote:"))
      return;

    try {
      final OServerAdmin admin = new OServerAdmin(url);

      admin.connect((String) conf.get("storage.user"), (String) conf.get("storage.password"));

      if (!admin.existsDatabase()) {
        System.err.println("creating database " + url);
        admin.createDatabase("graph", "plocal");
      }

      try {
        OrientGraph t = new OrientGraph(url, (String) conf.get("storage.user"), (String) conf.get("storage.password"));
        t.command(new OCommandSQL("alter database custom useLightweightEdges=false")).execute();
        t.commit();
        t.shutdown();
      } catch (Throwable ignored) {
        // blank
      }

      try {
        OrientGraph t = new OrientGraph(url, (String) conf.get("storage.user"), (String) conf.get("storage.password"));
        t.command(new OCommandSQL("ALTER CLASS V CLUSTERSELECTION balanced")).execute();
        t.commit();
        t.shutdown();
      } catch (Throwable ignored) {
        // blank
      }

      try {
        OrientGraph t = new OrientGraph(url, (String) conf.get("storage.user"), (String) conf.get("storage.password"));
        t.command(new OCommandSQL("ALTER CLASS E CLUSTERSELECTION balanced")).execute();
        t.commit();
        t.shutdown();
      } catch (Throwable ignored) {
        // blank
      }

      admin.close();
    } catch (IOException ex1) {
      throw new RuntimeException(ex1);
    }
  }

  @Test
  public void testEdges() throws Exception {
    OrientGraphFactory factory = getGraphFactory();

    try {
      factory.getNoTx().createEdgeType("some-label");
    } catch (OSchemaException ex) {
      if (!ex.getMessage().contains("exists"))
        throw (ex);
      factory.getNoTx().command(new OCommandSQL("delete edge some-label")).execute();
    }

    try {
      factory.getNoTx().createVertexType("some-v-label");
    } catch (OSchemaException ex) {
      if (!ex.getMessage().contains("exists"))
        throw (ex);
      factory.getNoTx().command(new OCommandSQL("delete vertex some-v-label")).execute();
    }

    OrientGraph t = factory.getTx();

    Vertex v1 = t.addVertex("class:some-v-label");
    Vertex v2 = t.addVertex("class:some-v-label");
    v1.setProperty("_id", "v1");
    v2.setProperty("_id", "v2");

    OrientEdge edge = t.addEdge(null, v1, v2, "some-label");
    edge.setProperty("some", "thing");

    t.commit();
    t.shutdown();

    t = factory.getTx();

    assertEquals(2, t.countVertices("some-v-label"));
    assertEquals(1, t.countEdges());
    assertNotNull(t.getVertices("_id", "v1").iterator().next());
    assertNotNull(t.getVertices("_id", "v2").iterator().next());
    t.commit();
    t.shutdown();

    t = factory.getTx();

    // works
    assertEquals(1, t.getVertices("_id", "v1").iterator().next().query().labels("some-label").count());
    // NoSuchElementException
    assertNotNull(t.getVertices("_id", "v1").iterator().next().query().labels("some-label").edges().iterator().next());

    t.commit();
  }
}
