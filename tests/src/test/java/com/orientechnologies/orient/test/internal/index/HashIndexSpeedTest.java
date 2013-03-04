package com.orientechnologies.orient.test.internal.index;

import java.util.Random;

import org.testng.annotations.Test;

import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.OClusterPositionLong;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.index.hashindex.local.OUniqueHashIndex;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * @author Andrey Lomakin
 * @since 30.01.13
 */
public class HashIndexSpeedTest extends SpeedTestMonoThread {
  private ODatabaseDocumentTx databaseDocumentTx;
  private OUniqueHashIndex    hashIndex = new OUniqueHashIndex();
  private Random              random    = new Random();

  public HashIndexSpeedTest() {
    super(5000000);
  }

  @Override
  @Test(enabled = false)
  public void init() throws Exception {
    String buildDirectory = System.getProperty("buildDirectory", "/temp");
    if (buildDirectory == null)
      buildDirectory = ".";

    databaseDocumentTx = new ODatabaseDocumentTx("local:" + buildDirectory + "/uniqueHashIndexTest");
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
    }

    databaseDocumentTx.create();

    hashIndex.create("uhashIndexTest", new OSimpleKeyIndexDefinition(OType.STRING), databaseDocumentTx,
        OMetadata.CLUSTER_INDEX_NAME, new int[0], null);
  }

  @Override
  @Test(enabled = false)
  public void cycle() throws Exception {
    String key = "bsadfasfas" + random.nextInt();
    hashIndex.put(key, new ORecordId(0, new OClusterPositionLong(0)));
  }

  @Override
  public void deinit() throws Exception {
    hashIndex.close();
  }
}
