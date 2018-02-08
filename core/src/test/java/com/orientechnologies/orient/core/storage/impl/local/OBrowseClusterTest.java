package com.orientechnologies.orient.core.storage.impl.local;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;

public class OBrowseClusterTest {

  private ODatabaseSession db;
  private OrientDB         orientDb;

  @Before
  public void before() {
    orientDb = new OrientDB("embedded:",
        OrientDBConfig.builder().addConfig(OGlobalConfiguration.CLASS_MINIMUM_CLUSTERS, 1).build());
    orientDb.create("test", ODatabaseType.MEMORY);
    db = orientDb.open("test", "admin", "admin");
    db.createVertexClass("One");
  }

  @Test
  public void testBrowse() {
    int numberOfEntries = 10000;
    for (int i = 0; i <= numberOfEntries; i++) {
      OVertex v = db.newVertex("One");
      v.setProperty("a", i);
      db.save(v);
    }
    int cluster = db.getClass("One").getDefaultClusterId();
    Iterator<OBrowsePage> browser = ((OAbstractPaginatedStorage) ((ODatabaseDocumentInternal) db).getStorage())
        .browseCluster(cluster);
    int count = 0;
    while (browser.hasNext()) {
      OBrowsePage page = browser.next();
      for (OBrowsePage.OBrowseEntry entry : page) {
        count++;
        assertNotNull(entry.getBuffer());
        assertNotNull(entry.getClusterPosition());
      }
    }
    assertEquals(numberOfEntries, count);
  }

  @After
  public void after() {
    db.close();
    orientDb.close();
  }

}
