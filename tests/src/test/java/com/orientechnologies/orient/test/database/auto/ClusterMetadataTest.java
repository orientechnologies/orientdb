package com.orientechnologies.orient.test.database.auto;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;

/**
 * @author Andrey Lomakin
 * @since 10.07.13
 */
@Test
public class ClusterMetadataTest {
  private String              url;
  private ODatabaseDocumentTx databaseDocumentTx;

  @Parameters(value = "url")
  public ClusterMetadataTest(String url) {
    this.url = url;
  }

  @BeforeMethod
  public void beforeMethod() {
    databaseDocumentTx = new ODatabaseDocumentTx(url);
    if (databaseDocumentTx.exists())
      databaseDocumentTx.open("admin", "admin");
    else
      databaseDocumentTx.create();
  }

  @AfterMethod
  public void afterMethod() {
    databaseDocumentTx.close();
  }

  public void testMetadataStore() throws Exception {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:clusterMetadataTest");
    db.create();

    final int clusterId = db.addCluster("clusterTest", OStorage.CLUSTER_TYPE.PHYSICAL);
    OCluster cluster = db.getStorage().getClusterById(clusterId);

    Assert.assertTrue(cluster.useWal());
    Assert.assertEquals(cluster.recordGrowFactor(), 1.2f);
    Assert.assertEquals(cluster.recordOverflowGrowFactor(), 1.2f);
    Assert.assertEquals(cluster.compression(), OGlobalConfiguration.STORAGE_COMPRESSION_METHOD.getValueAsString());

    db.command(new OCommandSQL("alter cluster clusterTest use_wal false")).execute();
    db.command(new OCommandSQL("alter cluster clusterTest record_grow_factor 2")).execute();
    db.command(new OCommandSQL("alter cluster clusterTest record_overflow_grow_factor 2")).execute();
    db.command(new OCommandSQL("alter cluster clusterTest compression nothing")).execute();

    Assert.assertFalse(cluster.useWal());
    Assert.assertEquals(cluster.recordGrowFactor(), 2f);
    Assert.assertEquals(cluster.recordOverflowGrowFactor(), 2f);
    Assert.assertEquals(cluster.compression(), "nothing");

    OStorage storage = db.getStorage();
    db.close();
    storage.close(true);

    db = new ODatabaseDocumentTx("plocal:clusterMetadataTest");
    db.open("admin", "admin");

    cluster = db.getStorage().getClusterById(clusterId);
    Assert.assertFalse(cluster.useWal());
    Assert.assertEquals(cluster.recordGrowFactor(), 2f);
    Assert.assertEquals(cluster.recordOverflowGrowFactor(), 2f);
    Assert.assertEquals(cluster.compression(), "nothing");

    try {
      db.command(new OCommandSQL("alter cluster clusterTest record_grow_factor 0.5")).execute();
      Assert.fail();
    } catch (OException e) {
    }

    try {
      db.command(new OCommandSQL("alter cluster clusterTest record_grow_factor fff")).execute();
      Assert.fail();
    } catch (OException e) {
    }

    try {
      db.command(new OCommandSQL("alter cluster clusterTest record_overflow_grow_factor 0.5")).execute();
      Assert.fail();
    } catch (OException e) {
    }

    try {
      db.command(new OCommandSQL("alter cluster clusterTest record_overflow_grow_factor fff")).execute();
      Assert.fail();
    } catch (OException e) {
    }

    try {
      db.command(new OCommandSQL("alter cluster clusterTest compression dsgfgd")).execute();
      Assert.fail();
    } catch (OException e) {
    }

    Assert.assertFalse(cluster.useWal());
    Assert.assertEquals(cluster.recordGrowFactor(), 2f);
    Assert.assertEquals(cluster.recordOverflowGrowFactor(), 2f);
    Assert.assertEquals(cluster.compression(), "nothing");

    db.drop();
  }
}
