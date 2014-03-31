package com.orientechnologies.test;

import java.util.Collection;

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
public class LuceneNotUniqueSpeedReadTest extends SpeedTestMonoThread {
  private ODatabaseDocumentTx databaseDocumentTx;
  private OIndex              hashIndex;
  private MersenneTwisterFast random = new MersenneTwisterFast();

  private int                 i      = 0;

  public LuceneNotUniqueSpeedReadTest() {
    super(5000000);
  }

  @Override
  @Test(enabled = false)
  public void init() throws Exception {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    if (buildDirectory == null)
      buildDirectory = ".";

    databaseDocumentTx = new ODatabaseDocumentTx("local:" + buildDirectory + "/notUniqueLucene");
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
    }

    hashIndex = databaseDocumentTx.getMetadata().getIndexManager().getIndex("hashIndex");
  }

  @Override
  @Test(enabled = false)
  public void cycle() throws Exception {
    String key = "bsadfasfas" + i;
    Collection res = (Collection) hashIndex.get(key);
    Assert.assertTrue(res.size() > 0);
    i++;
  }

  @Override
  @Test(enabled = false)
  public void deinit() throws Exception {
    databaseDocumentTx.drop();
  }
}
