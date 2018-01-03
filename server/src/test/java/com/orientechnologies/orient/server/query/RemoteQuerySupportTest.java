package com.orientechnologies.orient.server.query;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.*;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE;
import static org.junit.Assert.*;

/**
 * Created by tglman on 03/01/17.
 */
public class RemoteQuerySupportTest {

  private static final String SERVER_DIRECTORY = "./target/query";
  private OServer           server;
  private OrientDB          orientDB;
  private ODatabaseDocument session;
  private int               oldPageSize;

  @Before
  public void before() throws Exception {
    OGlobalConfiguration.CLASS_MINIMUM_CLUSTERS.setValue(1);
    server = new OServer(false);
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(getClass().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();

    orientDB = new OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig());
    orientDB.create(RemoteQuerySupportTest.class.getSimpleName(), ODatabaseType.MEMORY);
    session = orientDB.open(RemoteQuerySupportTest.class.getSimpleName(), "admin", "admin");
    session.createClass("Some");
    oldPageSize = QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger();
    QUERY_REMOTE_RESULTSET_PAGE_SIZE.setValue(10);
  }

  @Test
  public void testQuery() {
    for (int i = 0; i < 150; i++) {
      ODocument doc = new ODocument("Some");
      doc.setProperty("prop", "value");
      session.save(doc);
    }
    OResultSet res = session.query("select from Some");
    for (int i = 0; i < 150; i++) {
      assertTrue(res.hasNext());
      OResult item = res.next();
      assertEquals(item.getProperty("prop"), "value");
    }
  }

  @Test
  public void testCommandSelect() {
    for (int i = 0; i < 150; i++) {
      ODocument doc = new ODocument("Some");
      doc.setProperty("prop", "value");
      session.save(doc);
    }
    OResultSet res = session.command("select from Some");
    for (int i = 0; i < 150; i++) {
      assertTrue(res.hasNext());
      OResult item = res.next();
      assertEquals(item.getProperty("prop"), "value");
    }
  }

  @Test(expected = ODatabaseException.class)
  public void testQueryKilledSession() {
    for (int i = 0; i < 150; i++) {
      ODocument doc = new ODocument("Some");
      doc.setProperty("prop", "value");
      session.save(doc);
    }
    OResultSet res = session.query("select from Some");

    for (OClientConnection conn : server.getClientConnectionManager().getConnections()) {
      conn.close();
    }
    session.activateOnCurrentThread();

    for (int i = 0; i < 150; i++) {
      assertTrue(res.hasNext());
      OResult item = res.next();
      assertEquals(item.getProperty("prop"), "value");
    }
  }

  @Test
  public void testQueryEmbedded() {
    ODocument doc = new ODocument("Some");
    doc.setProperty("prop", "value");
    ODocument emb = new ODocument();
    emb.setProperty("one", "value");
    doc.setProperty("emb", emb, OType.EMBEDDED);
    session.save(doc);
    OResultSet res = session.query("select emb from Some");

    OResult item = res.next();
    assertNotNull(item.getProperty("emb"));
    assertEquals(((OResult) item.getProperty("emb")).getProperty("one"), "value");
  }

  @Test
  public void testQueryDoubleEmbedded() {
    ODocument doc = new ODocument("Some");
    doc.setProperty("prop", "value");
    ODocument emb1 = new ODocument();
    emb1.setProperty("two", "value");
    ODocument emb = new ODocument();
    emb.setProperty("one", "value");
    emb.setProperty("secEmb", emb1, OType.EMBEDDED);

    doc.setProperty("emb", emb, OType.EMBEDDED);
    session.save(doc);
    OResultSet res = session.query("select emb from Some");

    OResult item = res.next();
    assertNotNull(item.getProperty("emb"));
    OResult resEmb = item.getProperty("emb");
    assertEquals(resEmb.getProperty("one"), "value");
    assertEquals(((OResult) resEmb.getProperty("secEmb")).getProperty("two"), "value");
  }

  @Test
  public void testQueryEmbeddedList() {
    ODocument doc = new ODocument("Some");
    doc.setProperty("prop", "value");
    ODocument emb = new ODocument();
    emb.setProperty("one", "value");
    List<Object> list = new ArrayList<>();
    list.add(emb);
    doc.setProperty("list", list, OType.EMBEDDEDLIST);
    session.save(doc);
    OResultSet res = session.query("select list from Some");

    OResult item = res.next();
    assertNotNull(item.getProperty("list"));
    assertEquals(((List<OResult>) item.getProperty("list")).size(), 1);
    assertEquals(((List<OResult>) item.getProperty("list")).get(0).getProperty("one"), "value");
  }

  @Test
  public void testQueryEmbeddedSet() {
    ODocument doc = new ODocument("Some");
    doc.setProperty("prop", "value");
    ODocument emb = new ODocument();
    emb.setProperty("one", "value");
    Set<ODocument> set = new HashSet<>();
    set.add(emb);
    doc.setProperty("set", set, OType.EMBEDDEDSET);
    session.save(doc);
    OResultSet res = session.query("select set from Some");

    OResult item = res.next();
    assertNotNull(item.getProperty("set"));
    assertEquals(((Set<OResult>) item.getProperty("set")).size(), 1);
    assertEquals(((Set<OResult>) item.getProperty("set")).iterator().next().getProperty("one"), "value");
  }

  @Test
  public void testQueryEmbeddedMap() {
    ODocument doc = new ODocument("Some");
    doc.setProperty("prop", "value");
    ODocument emb = new ODocument();
    emb.setProperty("one", "value");
    Map<String, ODocument> map = new HashMap<>();
    map.put("key", emb);
    doc.setProperty("map", map, OType.EMBEDDEDMAP);
    session.save(doc);
    OResultSet res = session.query("select map from Some");

    OResult item = res.next();
    assertNotNull(item.getProperty("map"));
    assertEquals(((Map<String, OResult>) item.getProperty("map")).size(), 1);
    assertEquals(((Map<String, OResult>) item.getProperty("map")).get("key").getProperty("one"), "value");
  }

  @After
  public void after() {
    QUERY_REMOTE_RESULTSET_PAGE_SIZE.setValue(oldPageSize);
    session.close();
    orientDB.close();
    server.shutdown();

    Orient.instance().shutdown();
    OFileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
    Orient.instance().startup();
  }

}
