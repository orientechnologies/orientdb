/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    File file = new File("./target/databases/");
    if (file.exists())
      OFileUtils.deleteRecursively(file);
  }

  protected static ODatabasePool getGraphFactory() throws Exception {
    Map<String, Object> conf = new HashMap<String, Object>();

    conf.put("storage.url", "remote:localhost/test");
    conf.put("db.name", "test");
    conf.put("storage.pool-min", 1);
    conf.put("storage.pool-max", 10);
    conf.put("storage.user", "root");
    conf.put("storage.password", "root");

    OGlobalConfiguration.CLIENT_CONNECT_POOL_WAIT_TIMEOUT.setValue(15000);

    verifyDatabaseExists(conf);

    ODatabasePool pool = new ODatabasePool(conf.get("storage.url") + "/" + (String) conf.get("db.name"),
        (String) conf.get("storage.user"), (String) conf.get("storage.password"), OrientDBConfig.defaultConfig());
    return pool;
  }

  @BeforeClass
  public static void setup() throws Exception {
    File file = new File("./target/databases/");
    if (file.exists())
      OFileUtils.deleteRecursively(file);
    file.mkdirs();

    server = new OServer(false);
    server.startup("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" + "<orient-server>\n" + "    <handlers>\n"
//        + "        <!-- GRAPH PLUGIN -->\n"
//        + "        <handler class=\"com.orientechnologies.orient.graph.handler.OGraphServerHandler\">\n"
//        + "            <parameters>\n" + "                <parameter name=\"enabled\" value=\"true\"/>\n"
//        + "                <parameter name=\"graph.pool.max\" value=\"50\"/>\n" + "            </parameters>\n"
//        + "        </handler>\n" + "       \n"
        + "<handler class=\"com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin\">\n" + "            <parameters>\n"
        + "                <parameter name=\"nodeName\" value=\"unittest\" />\n"
        + "                <parameter name=\"enabled\" value=\"true\"/>\n"
        + "                <parameter name=\"configuration.db.default\"\n"
        + "                           value=\"src/test/resources/default-distributed-db-config.json\"/>\n"
        + "                <parameter name=\"configuration.hazelcast\" value=\"config/hazelcast.xml\"/>\n" + "\n"
        + "            </parameters>\n" + "        </handler>" + "    </handlers>\n" + "    <network>\n" + "        <protocols>\n"
        + "            <protocol name=\"binary\"\n"
        + "                      implementation=\"com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary\"/>\n"
        + "        </protocols>\n" + "        <listeners>\n"
        + "            <listener protocol=\"binary\" ip-address=\"0.0.0.0\" port-range=\"2424-2430\"/>\n" + "        </listeners>\n"
        + "        <cluster>\n" + "        </cluster>\n" + "    </network>\n" + "    <storages>\n" + "    </storages>\n"
        + "    <users>\n" + "      <user name=\"root\" password=\"root\" resources=\"*\"/>\n" + "    </users>\n"
        + "    <properties>\n" + "\n" + "        <!-- Uses the Hazelcast's distributed cache as 2nd level cache -->\n"
        + "        <!-- <entry name=\"cache.level2.impl\" value=\"com.orientechnologies.orient.server.hazelcast.OHazelcastCache\" /> -->\n"
        + "\n" + "        <!-- DATABASE POOL: size min/max -->\n" + "        <entry name=\"db.pool.min\" value=\"1\"/>\n"
        + "        <entry name=\"db.pool.max\" value=\"20\"/>\n" + "\n"
        + "        <!-- LEVEL1 AND 2 CACHE: enable/disable and set the size as number of entries -->\n"
        + "        <entry name=\"cache.level1.enabled\" value=\"false\"/>\n"
        + "        <entry name=\"cache.level1.size\" value=\"1000\"/>\n"
        + "        <entry name=\"cache.level2.enabled\" value=\"false\"/>\n"
        + "        <entry name=\"cache.level2.size\" value=\"1000\"/>\n" + "\n"
        + "<entry name=\"server.database.path\" value=\"target/databases\" />"
        + "        <!-- PROFILER: configures the profiler as <seconds-for-snapshot>,<archive-snapshot-size>,<summary-size>  -->\n"
        + "        <entry name=\"profiler.enabled\" value=\"true\"/>\n"
        + "        <!-- <entry name=\"profiler.config\" value=\"30,10,10\" />  -->\n" + "\n"
        + "        <!-- LOG: enable/Disable logging. Levels are: finer, fine, finest, info, warning -->\n"
        + "        <entry name=\"log.console.level\" value=\"info\"/>\n"
        + "        <entry name=\"log.file.level\" value=\"info\"/>\n" + "    </properties>\n"
        + " <isAfterFirstTime>true</isAfterFirstTime></orient-server>");

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

      ODatabaseDocumentTx t = new ODatabaseDocumentTx(url);
      t.open((String) conf.get("storage.user"), (String) conf.get("storage.password"));
      t.begin();
      try {
        t.command(new OCommandSQL("alter database custom useLightweightEdges=false")).execute();
        t.commit();
        t.begin();
        t.command(new OCommandSQL("ALTER CLASS V CLUSTERSELECTION balanced")).execute();
        t.commit();
        t.begin();
        t.command(new OCommandSQL("ALTER CLASS E CLUSTERSELECTION balanced")).execute();
        t.commit();
      } finally {
        t.close();
      }

      admin.close();
    } catch (IOException ex1) {
      throw new RuntimeException(ex1);
    }
  }

  @Test
  public void testEdges() throws Exception {
    ODatabasePool pool = getGraphFactory();

    ODatabaseDocument g = pool.acquire();
    try {
      try {
        g.createEdgeClass("some-label");
      } catch (OSchemaException ex) {
        if (!ex.getMessage().contains("exists"))
          throw (ex);
        g.command(new OCommandSQL("delete edge some-label")).execute();
      }

      try {
        g.createVertexClass("some-v-label");
      } catch (OSchemaException ex) {
        if (!ex.getMessage().contains("exists"))
          throw (ex);
        g.command(new OCommandSQL("delete vertex some-v-label")).execute();
      }
    } finally {
      g.close();
    }

    ODatabaseDocument t = pool.acquire();
    t.begin();
    try {
      OVertex v1 = t.newVertex("some-v-label");
      v1.save();
      OVertex v2 = t.newVertex("some-v-label");
      v1.setProperty("_id", "v1");
      v2.setProperty("_id", "v2");
      v2.save();

      OEdge edge = v1.addEdge(v2, "some-label");
      edge.setProperty("some", "thing");
      edge.save();
      t.commit();
      t.close();

      t = pool.acquire();
      t.begin();

      assertEquals(2, t.getClass("some-v-label").count());
      assertEquals(1, t.getClass("E").count());
      assertNotNull(getVertices(t, "_id", "v1").iterator().next());
      assertNotNull(getVertices(t, "_id", "v2").iterator().next());
      t.commit();
      t.close();

      t = pool.acquire();
      t.begin();

      // works
      assertEquals(1, count(getVertices(t, "_id", "v1").iterator().next().getEdges(ODirection.BOTH, "some-label")));
      // NoSuchElementException
//      assertNotNull(t.getVertices("_id", "v1").iterator().next()..labels("some-label").edges().iterator().next());//TODO what...?

      t.commit();
    } finally {
      t.close();
    }
    pool.close();
  }

  private int count(Iterable<OEdge> edges) {
    int result = 0;
    for (OEdge e : edges) {
      result++;
    }
    return result;
  }

  private Iterable<OVertex> getVertices(ODatabaseDocument db, String key, String value) {
    OResultSet rs = db.command("SELECT FROM V WHERE " + key + " = ?", value);
    List<OVertex> result = rs.vertexStream().collect(Collectors.toList());
    rs.close();
    return result;
  }
}
