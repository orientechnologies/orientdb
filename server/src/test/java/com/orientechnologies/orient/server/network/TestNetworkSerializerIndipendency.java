package com.orientechnologies.orient.server.network;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentAbstract;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinary;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;
import com.orientechnologies.orient.server.OServer;
import java.io.File;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestNetworkSerializerIndipendency {
  private OServer server;

  @Before
  public void before() throws Exception {
    server = new OServer(false);
    server.startup(getClass().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();
  }

  @Test(expected = OStorageException.class)
  public void createCsvDatabaseConnectBinary() throws IOException {
    ORecordSerializer prev = ODatabaseDocumentAbstract.getDefaultSerializer();
    ODatabaseDocumentAbstract.setDefaultSerializer(ORecordSerializerSchemaAware2CSV.INSTANCE);
    OrientDB orientdb =
        new OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig());
    orientdb
        .execute("create database test plocal users(admin identified by 'adminpwd' role admin)")
        .close();

    ODatabaseDocument dbTx = null;
    try {
      ODatabaseDocumentAbstract.setDefaultSerializer(ORecordSerializerBinary.INSTANCE);

      dbTx = orientdb.open("test", "admin", "adminpwd");
      ODocument document = new ODocument();
      document.field("name", "something");
      document.field("surname", "something-else");
      document = dbTx.save(document, dbTx.getClusterNameById(dbTx.getDefaultClusterId()));
      dbTx.commit();
      ODocument doc = dbTx.load(document.getIdentity());
      assertEquals(doc.fields(), document.fields());
      assertEquals(doc.<Object>field("name"), document.field("name"));
      assertEquals(doc.<Object>field("surname"), document.field("surname"));

    } finally {
      if (dbTx != null && !dbTx.isClosed()) {
        dbTx.close();
        ((ODatabaseDocumentInternal) dbTx).getStorage().close();
      }

      orientdb.execute("drop database test ").close();
      ODatabaseDocumentAbstract.setDefaultSerializer(prev);
    }
    orientdb.close();
  }

  @Test
  public void createBinaryDatabaseConnectCsv() throws IOException {
    ORecordSerializer prev = ODatabaseDocumentAbstract.getDefaultSerializer();
    ODatabaseDocumentAbstract.setDefaultSerializer(ORecordSerializerBinary.INSTANCE);
    OrientDB orientdb =
        new OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig());
    orientdb
        .execute("create database test plocal users(admin identified by 'adminpwd' role admin)")
        .close();

    ODatabaseDocument dbTx = null;
    try {
      ODatabaseDocumentAbstract.setDefaultSerializer(ORecordSerializerSchemaAware2CSV.INSTANCE);
      dbTx = orientdb.open("test", "admin", "adminpwd");
      ODocument document = new ODocument();
      document.field("name", "something");
      document.field("surname", "something-else");
      document = dbTx.save(document, dbTx.getClusterNameById(dbTx.getDefaultClusterId()));
      dbTx.commit();
      ODocument doc = dbTx.load(document.getIdentity());
      assertEquals(doc.fields(), document.fields());
      assertEquals(doc.<Object>field("name"), document.field("name"));
      assertEquals(doc.<Object>field("surname"), document.field("surname"));
    } finally {
      if (dbTx != null) {
        dbTx.close();
        ((ODatabaseDocumentInternal) dbTx).getStorage().close();
      }

      orientdb.execute("drop database test ").close();
      ODatabaseDocumentAbstract.setDefaultSerializer(prev);
    }
    orientdb.close();
  }

  @After
  public void after() {
    server.shutdown();

    Orient.instance().shutdown();
    File directory = new File(server.getDatabaseDirectory());
    OFileUtils.deleteRecursively(directory);
    ODatabaseDocumentAbstract.setDefaultSerializer(
        ORecordSerializerFactory.instance().getFormat(ORecordSerializerBinary.NAME));
    Orient.instance().startup();
  }
}
