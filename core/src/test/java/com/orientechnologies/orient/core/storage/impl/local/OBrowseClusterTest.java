package com.orientechnologies.orient.core.storage.impl.local;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.record.OVertex;
import java.util.Iterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OBrowseClusterTest {

  private ODatabaseSession db;
  private OrientDB orientDb;

  @Before
  public void before() {
    orientDb =
        new OrientDB(
            "embedded:",
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.CLASS_MINIMUM_CLUSTERS, 1)
                .build());
    orientDb.create("test", ODatabaseType.MEMORY);
    db = orientDb.open("test", "admin", "admin");
    db.createVertexClass("One");
  }

  @Test
  public void testBrowse() {
    int numberOfEntries = 4962;
    for (int i = 0; i < numberOfEntries; i++) {
      OVertex v = db.newVertex("One");
      v.setProperty("a", i);
      db.save(v);
    }
    int cluster = db.getClass("One").getDefaultClusterId();
    Iterator<OClusterBrowsePage> browser =
        ((OAbstractPaginatedStorage) ((ODatabaseDocumentInternal) db).getStorage())
            .browseCluster(cluster);
    int count = 0;

    while (browser.hasNext()) {
      OClusterBrowsePage page = browser.next();
      for (OClusterBrowseEntry entry : page) {
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
