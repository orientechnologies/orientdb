package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.testng.annotations.*;

@Test
public abstract class BaseTest {
  protected ODatabaseDocumentTx db;
  private boolean               dropDb = false;

  @Parameters(value = "url")
  public BaseTest(@Optional String url) {
    if (url == null) {
      final String buildDirectory = System.getProperty("buildDirectory", ".");
      url = "plocal:" + buildDirectory + "/test-db/" + this.getClass().getSimpleName();
      dropDb = true;
    }

    db = new ODatabaseDocumentTx(url);
  }

  @BeforeClass
  public void beforeClass() {
    if (dropDb) {
      if (db.exists()) {
        db.open("admin", "admin");
        db.drop();
      }

      db.create();
    } else
      db.open("admin", "admin");
  }

  @AfterClass
  public void afterClass() {
    if (dropDb)
      db.drop();
    else
      db.close();
  }

}
