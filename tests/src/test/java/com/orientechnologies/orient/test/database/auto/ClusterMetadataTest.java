package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OStorage;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 10.07.13
 */
@Test
public class ClusterMetadataTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public ClusterMetadataTest(@Optional String url) {
    super(url);
  }

  public void testMetadataStore() throws Exception {
    final int clusterId = database.addCluster("clusterTest");
    OCluster cluster = database.getStorage().getClusterById(clusterId);

    Assert.assertEquals(cluster.recordGrowFactor(), 1.2f);
    Assert.assertEquals(cluster.recordOverflowGrowFactor(), 1.2f);

    database.command(new OCommandSQL("alter cluster clusterTest record_grow_factor 2")).execute();
    database.command(new OCommandSQL("alter cluster clusterTest record_overflow_grow_factor 2")).execute();

    Assert.assertEquals(cluster.recordGrowFactor(), 2f);
    Assert.assertEquals(cluster.recordOverflowGrowFactor(), 2f);

    OStorage storage = database.getStorage();
    database.close();
    storage.close(true, false);

    database.activateOnCurrentThread();
    database.resetInitialization();
    database.open("admin", "admin");

    cluster = database.getStorage().getClusterById(clusterId);
    Assert.assertEquals(cluster.recordGrowFactor(), 2f);
    Assert.assertEquals(cluster.recordOverflowGrowFactor(), 2f);

    try {
      database.command(new OCommandSQL("alter cluster clusterTest record_grow_factor 0.5")).execute();
      Assert.fail();
    } catch (OException e) {
    }

    try {
      database.command(new OCommandSQL("alter cluster clusterTest record_grow_factor fff")).execute();
      Assert.fail();
    } catch (OException e) {
    }

    try {
      database.command(new OCommandSQL("alter cluster clusterTest record_overflow_grow_factor 0.5")).execute();
      Assert.fail();
    } catch (OException e) {
    }

    try {
      database.command(new OCommandSQL("alter cluster clusterTest record_overflow_grow_factor fff")).execute();
      Assert.fail();
    } catch (OException e) {
    }

    Assert.assertEquals(cluster.recordGrowFactor(), 2f);
    Assert.assertEquals(cluster.recordOverflowGrowFactor(), 2f);
  }
}
