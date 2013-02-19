package com.orientechnologies.orient.test.internal.index;

import java.util.Random;

import org.testng.annotations.Test;

import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.OClusterPositionLong;
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
  private Random              random = new Random();

  public MVRBTreeInsertionSpeedTest() {
    super(1000000);
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

    index = (OIndexUnique) databaseDocumentTx.getMetadata().getIndexManager()
        .createIndex("mvrbtreeIndexTest", "UNIQUE", new OSimpleKeyIndexDefinition(OType.STRING), new int[0], null);
  }

  @Override
  @Test(enabled = false)
  public void cycle() throws Exception {
    String key = "bsadfasfas" + random.nextInt();
    index.put(key, new ORecordId(0, new OClusterPositionLong(0)));
  }

  @Override
  public void deinit() throws Exception {
    databaseDocumentTx.close();
  }
}
