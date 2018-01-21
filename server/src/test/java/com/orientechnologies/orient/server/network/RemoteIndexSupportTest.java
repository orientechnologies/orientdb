package com.orientechnologies.orient.server.network;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by tglman on 26/04/16.
 */
public class RemoteIndexSupportTest {

  private OServer server;

  @Before
  public void before() throws Exception {
    server = new OServer(false);
    server.startup(getClass().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();

    OServerAdmin server = new OServerAdmin("remote:localhost");
    server.connect("root", "root");
    server.createDatabase("test", "graph", "memory");

  }

  @Test
  public void testOneValueIndexInTxLookup() throws IOException {

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("remote:localhost/test");
    db.open("admin", "admin");

    OClass clazz = db.getMetadata().getSchema().createClass("TestIndex");
    clazz.createProperty("test", OType.STRING).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

    db.begin();
    ODocument doc = new ODocument("TestIndex");
    doc.field("test", "testKey");
    db.save(doc);
    OIndex<Collection<OIdentifiable>> idx = (OIndex<Collection<OIdentifiable>>) db.getMetadata().getIndexManager()
        .getIndex("TestIndex.test");
    Collection<OIdentifiable> res = idx.get("testKey");
    assertEquals( 1,res.size());
    db.close();
  }

  @After
  public void after() {
    server.shutdown();

    Orient.instance().shutdown();
    OFileUtils.deleteRecursively(new File(server.getDatabaseDirectory()));
    Orient.instance().startup();
  }
}
