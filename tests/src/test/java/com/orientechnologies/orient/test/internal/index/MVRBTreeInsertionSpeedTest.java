package com.orientechnologies.orient.test.internal.index;

import org.testng.annotations.Test;

import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.common.util.MersenneTwisterFast;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndexUnique;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * @author Andrey Lomakin
 * @author Luca Garulli
 * @since 30.01.13
 */
public class MVRBTreeInsertionSpeedTest extends SpeedTestMonoThread {
  private ODatabaseDocumentTx databaseDocumentTx;
  private OIndexUnique        index;
  private MersenneTwisterFast random = new MersenneTwisterFast();

  public MVRBTreeInsertionSpeedTest() {
    super(5000000);
  }

  @Override
  @Test(enabled = false)
  public void init() throws Exception {
    OGlobalConfiguration.NON_TX_CLUSTERS_SYNC_IMMEDIATELY.setValue("");
    OGlobalConfiguration.INDEX_MANUAL_LAZY_UPDATES.setValue(10000);

    String buildDirectory = System.getProperty("buildDirectory", ".");
    if (buildDirectory == null)
      buildDirectory = ".";

    databaseDocumentTx = new ODatabaseDocumentTx("plocal:" + buildDirectory + "/uniqueHashIndexTest");
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
    }

    databaseDocumentTx.create();

    index = (OIndexUnique) databaseDocumentTx.getMetadata().getIndexManager()
        .createIndex("mvrbtreeIndexTest", "UNIQUE", new OSimpleKeyIndexDefinition(-1, OType.STRING), new int[0], null, null);
  }

  @Override
  @Test(enabled = false)
  public void cycle() throws Exception {
    String key = "bsadfasfas" + random.nextInt();
    index.put(key, new ORecordId(0, 0));
  }

  @Override
  public void deinit() throws Exception {
    databaseDocumentTx.close();
  }
}
