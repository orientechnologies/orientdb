package com.orientechnologies.orient.test.internal.index;

import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import java.util.Random;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 14.08.13
 */
public class SBTreeInsertionSpeedTest extends SpeedTestMonoThread {
  private ODatabaseDocumentTx databaseDocumentTx;
  private OIndex index;
  private Random random = new Random();

  public SBTreeInsertionSpeedTest() {
    super(5000000);
  }

  @Override
  @Test(enabled = false)
  public void init() throws Exception {
    OGlobalConfiguration.NON_TX_CLUSTERS_SYNC_IMMEDIATELY.setValue("");
    OGlobalConfiguration.INDEX_MANUAL_LAZY_UPDATES.setValue(10000);

    String buildDirectory = System.getProperty("buildDirectory", ".");
    if (buildDirectory == null) buildDirectory = ".";

    databaseDocumentTx =
        new ODatabaseDocumentTx("plocal:" + buildDirectory + "/SBTreeInsertionSpeedTTest");
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
    }

    databaseDocumentTx.create();
    databaseDocumentTx
        .command(new OCommandSQL("create index  sbtree_index unique String"))
        .execute();

    index =
        databaseDocumentTx
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(databaseDocumentTx, "sbtree_index");
  }

  @Override
  @Test(enabled = false)
  public void cycle() throws Exception {
    databaseDocumentTx.begin();
    String key = "bsadfasfas" + random.nextInt();
    index.put(key, new ORecordId(0, 0));
    databaseDocumentTx.commit();
  }

  @Override
  @Test(enabled = false)
  public void deinit() throws Exception {
    databaseDocumentTx.close();
  }
}
