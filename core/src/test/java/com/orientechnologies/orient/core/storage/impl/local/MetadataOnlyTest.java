package com.orientechnologies.orient.core.storage.impl.local;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.core.tx.OTxMetadataHolderImpl;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
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

    var holder =
        new OTxMetadataHolderImpl(
            new CountDownLatch(1),
            new OTransactionId(1, 10),
            new OTransactionSequenceStatus(new long[] {0, 10, 10}));
    byte[] blob = holder.metadata();
    ((ODatabaseDocumentInternal) db).getStorage().metadataOnly(blob);
    db.close();
    OrientDBInternal.extract(orientDb).forceDatabaseClose("testMetadataOnly");
    db = orientDb.open("testMetadataOnly", "admin", "admin");
    Optional<byte[]> loaded = ((ODatabaseDocumentInternal) db).getStorage().getLastMetadata();
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
