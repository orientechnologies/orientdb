package com.orientechnologies.test;

import org.testng.annotations.Test;

import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.common.util.MersenneTwisterFast;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.OClusterPositionLong;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * @author Andrey Lomakin
 * @since 30.01.13
 */
public class LuceneNotUniqueSpeedTest extends SpeedTestMonoThread {
  private ODatabaseDocumentTx databaseDocumentTx;
  private OIndex              hashIndex;
  private MersenneTwisterFast random = new MersenneTwisterFast();

  private int                 i      = 0;

  public LuceneNotUniqueSpeedTest() {
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
      databaseDocumentTx.drop();
    }

    databaseDocumentTx.create();

    hashIndex = databaseDocumentTx.getMetadata().getIndexManager()
        .createIndex("hashIndex", "NOTUNIQUE", new OSimpleKeyIndexDefinition(OType.STRING), new int[0], null, null, "LUCENE");
  }

  @Override
  @Test(enabled = false)
  public void cycle() throws Exception {
    String key = "bsadfasfas" + i;
    hashIndex.put(key, new ORecordId(0, new OClusterPositionLong(0)));
    i++;
  }

  @Override
  @Test(enabled = false)
  public void deinit() throws Exception {
    // databaseDocumentTx.drop();
  }
}
