package com.orientechnologies.orient.core.storage.impl.local;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MetadataOnlyTest {

  private OrientDB orientDb;

  @Before
  public void before() {
    orientDb =
        new OrientDB(
            "embedded:./target/",
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.CLASS_MINIMUM_CLUSTERS, 1)
                .build());
    orientDb.execute(
        "create database testMetadataOnly plocal users (admin identified by 'admin' role admin)");
  }

  @Test
  public void test() {
    ODatabaseSession db = orientDb.open("testMetadataOnly", "admin", "admin");
    byte[] blob =
        new byte[] {
          1, 2, 3, 4, 5, 6,
        };
    ((OAbstractPaginatedStorage) ((ODatabaseDocumentInternal) db).getStorage()).metadataOnly(blob);
    db.close();
    OrientDBInternal.extract(orientDb).forceDatabaseClose("testMetadataOnly");
    db = orientDb.open("testMetadataOnly", "admin", "admin");
    Optional<byte[]> loaded =
        ((OAbstractPaginatedStorage) ((ODatabaseDocumentInternal) db).getStorage())
            .getLastMetadata();
    assertTrue(loaded.isPresent());
    assertArrayEquals(loaded.get(), blob);
    db.close();
  }

  @After
  public void after() {

    orientDb.drop("testMetadataOnly");
    orientDb.close();
  }
}
