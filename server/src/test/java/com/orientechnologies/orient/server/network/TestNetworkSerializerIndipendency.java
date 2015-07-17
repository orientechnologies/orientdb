package com.orientechnologies.orient.server.network;

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinary;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;
import com.orientechnologies.orient.server.OServer;

public class TestNetworkSerializerIndipendency {
  private OServer             server;
  private static final String SERVER_DIRECTORY = "./target/db";

  @BeforeClass
  public void before() throws Exception {
    server = new OServer();
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(getClass().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();
  }

  @Test
  public void createCsvDatabaseConnectBinary() throws IOException {
    ORecordSerializer prev = ODatabaseDocumentTx.getDefaultSerializer();
    ODatabaseDocumentTx.setDefaultSerializer(ORecordSerializerSchemaAware2CSV.INSTANCE);
    createDatabase();

    ODatabaseDocumentTx dbTx = null;
    try {
      ODatabaseDocumentTx.setDefaultSerializer(ORecordSerializerBinary.INSTANCE);
      dbTx = new ODatabaseDocumentTx("remote:localhost/test");
      dbTx.open("admin", "admin");
      ODocument document = new ODocument();
      document.field("name", "something");
      document.field("surname", "something-else");
      document = dbTx.save(document);
      dbTx.commit();
      ODocument doc = dbTx.load(document.getIdentity());
      assertEquals(doc.fields(), document.fields());
      assertEquals(doc.field("name"), document.field("name"));
      assertEquals(doc.field("surname"), document.field("surname"));
    } finally {
      if (dbTx != null) {
				dbTx.close();
				dbTx.getStorage().close();
			}

      dropDatabase();
      ODatabaseDocumentTx.setDefaultSerializer(prev);
    }
  }

  private void dropDatabase() throws IOException {
    OServerAdmin admin = new OServerAdmin("remote:localhost/test");
    admin.connect("root", "D2AFD02F20640EC8B7A5140F34FCA49D2289DB1F0D0598BB9DE8AAA75A0792F3");
    admin.dropDatabase("plocal");
  }

  private void createDatabase() throws IOException {
    OServerAdmin admin = new OServerAdmin("remote:localhost/test");
    admin.connect("root", "D2AFD02F20640EC8B7A5140F34FCA49D2289DB1F0D0598BB9DE8AAA75A0792F3");
    admin.createDatabase("document", "plocal");
  }

  @Test
  public void createBinaryDatabaseConnectCsv() throws IOException {
    ORecordSerializer prev = ODatabaseDocumentTx.getDefaultSerializer();
    ODatabaseDocumentTx.setDefaultSerializer(ORecordSerializerBinary.INSTANCE);
    createDatabase();

    ODatabaseDocumentTx dbTx = null;
    try {
      ODatabaseDocumentTx.setDefaultSerializer(ORecordSerializerSchemaAware2CSV.INSTANCE);
      dbTx = new ODatabaseDocumentTx("remote:localhost/test");
      dbTx.open("admin", "admin");
      ODocument document = new ODocument();
      document.field("name", "something");
      document.field("surname", "something-else");
      document = dbTx.save(document);
      dbTx.commit();
      ODocument doc = dbTx.load(document.getIdentity());
      assertEquals(doc.fields(), document.fields());
      assertEquals(doc.field("name"), document.field("name"));
      assertEquals(doc.field("surname"), document.field("surname"));
    } finally {
      if (dbTx != null) {
				dbTx.close();
				dbTx.getStorage().close();
			}

      dropDatabase();
      ODatabaseDocumentTx.setDefaultSerializer(prev);
    }
  }

  @AfterClass
  public void after() {
    server.shutdown();
    File iDirectory = new File(SERVER_DIRECTORY);
    deleteDirectory(iDirectory);
    Orient.instance().startup();
  }

  private void deleteDirectory(File iDirectory) {
    if (iDirectory.isDirectory())
      for (File f : iDirectory.listFiles()) {
        if (f.isDirectory())
          deleteDirectory(f);
        else if (!f.delete())
          throw new OConfigurationException("Can't delete the file: " + f);
      }
  }
}
