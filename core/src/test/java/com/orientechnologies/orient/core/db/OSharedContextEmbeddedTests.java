package com.orientechnologies.orient.core.db;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentEmbedded;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.Test;

public class OSharedContextEmbeddedTests {

  @Test
  public void testSimpleConfigStore() {
    try (OrientDB orientDb = new OrientDB("embedded:", OrientDBConfig.defaultConfig())) {
      orientDb.execute("create database test memory users(admin identified by 'admin' role admin)");
      try (ODatabaseSession session = orientDb.open("test", "admin", "admin")) {
        OSharedContextEmbedded shared =
            (OSharedContextEmbedded) ((ODatabaseDocumentEmbedded) session).getSharedContext();
        ODocument config = new ODocument();
        config.setProperty("one", "two");
        shared.saveConfig(session, "simple", config);
        ODocument loadConfig = shared.loadConfig(session, "simple");
        assertEquals((String) config.getProperty("one"), loadConfig.getProperty("one"));
      }
    }
  }

  @Test
  public void testConfigStoreDouble() {
    try (OrientDB orientDb = new OrientDB("embedded:", OrientDBConfig.defaultConfig())) {
      orientDb.execute("create database test memory users(admin identified by 'admin' role admin)");
      try (ODatabaseSession session = orientDb.open("test", "admin", "admin")) {
        OSharedContextEmbedded shared =
            (OSharedContextEmbedded) ((ODatabaseDocumentEmbedded) session).getSharedContext();
        ODocument config = new ODocument();
        config.setProperty("one", "two");
        shared.saveConfig(session, "simple", config);
        ODocument loadConfig = shared.loadConfig(session, "simple");
        assertEquals((String) config.getProperty("one"), loadConfig.getProperty("one"));

        ODocument other = new ODocument();
        other.setProperty("one", "three");
        shared.saveConfig(session, "simple", other);
        ODocument reLoadConfig = shared.loadConfig(session, "simple");
        assertEquals((String) other.getProperty("one"), reLoadConfig.getProperty("one"));
      }
    }
  }
}
