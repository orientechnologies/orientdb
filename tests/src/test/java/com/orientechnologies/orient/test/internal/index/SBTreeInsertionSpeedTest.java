package com.orientechnologies.orient.test.internal.index;

import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import java.util.Random;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 14.08.13
 */
public class SBTreeInsertionSpeedTest extends SpeedTestMonoThread {
  private ODatabaseDocumentInternal databaseDocumentTx;
  private OIndex index;
  private Random random = new Random();

  public SBTreeInsertionSpeedTest() {
    super(5000000);
  }

  @Override
  @Test(enabled = false)
  public void init() throws Exception {

    String buildDirectory = System.getProperty("buildDirectory", ".");
    if (buildDirectory == null) buildDirectory = ".";

    databaseDocumentTx =
        new ODatabaseDocumentTx("plocal:" + buildDirectory + "/SBTreeInsertionSpeedTTest");
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
    }

    databaseDocumentTx.create();
    databaseDocumentTx.command("create index  sbtree_index unique String").close();

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
