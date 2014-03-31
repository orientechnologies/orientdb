package com.orientechnologies.test;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.common.util.MersenneTwisterFast;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OIndex;

/**
 * @author Andrey Lomakin
 * @since 30.01.13
 */
public class LuceneUniqueSpeedReadTest extends SpeedTestMonoThread {
  private ODatabaseDocumentTx databaseDocumentTx;
  private OIndex              hashIndex;
  private MersenneTwisterFast random = new MersenneTwisterFast();

  private int                 i      = 0;

  public LuceneUniqueSpeedReadTest() {
    super(5000000);
  }

  @Override
  @Test(enabled = false)
  public void init() throws Exception {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    if (buildDirectory == null)
      buildDirectory = ".";

    databaseDocumentTx = new ODatabaseDocumentTx("local:" + buildDirectory + "/uniqueHashIndexTest");
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
    }


    hashIndex = databaseDocumentTx.getMetadata().getIndexManager().getIndex("hashIndex");
  }

  @Override
  @Test(enabled = false)
  public void cycle() throws Exception {
    String key = "bsadfasfas" + i;
    Assert.assertNotNull(hashIndex.get(key));
    i++;
  }

  @Override
  @Test(enabled = false)
  public void deinit() throws Exception {
    databaseDocumentTx.close();
  }
}
