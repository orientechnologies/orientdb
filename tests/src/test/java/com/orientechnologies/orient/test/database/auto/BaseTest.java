package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.testng.annotations.*;

@Test
public abstract class BaseTest {
  protected ODatabaseDocumentTx database;
  protected String              url;
  private boolean               dropDb = false;

  @Parameters(value = "url")
  public BaseTest(@Optional String url) {
    if (url == null) {
      final String buildDirectory = System.getProperty("buildDirectory", ".");
      url = "plocal:" + buildDirectory + "/test-db/" + this.getClass().getSimpleName();
      dropDb = true;
    }

    database = new ODatabaseDocumentTx(url);
    this.url = database.getURL();
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    if (dropDb) {
      if (database.exists()) {
        database.open("admin", "admin");
        database.drop();
      }

      database.create();
    } else
      database.open("admin", "admin");
  }

  @AfterClass
  public void afterClass() throws Exception {
    if (dropDb) {
      if (database.isClosed())
        database.open("admin", "admin");

      database.drop();
    } else {
      if (!database.isClosed())
        database.close();
    }
  }

  @BeforeMethod
  public void beforeMethod() throws Exception {
    if (database.isClosed())
      database.open("admin", "admin");
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    if (!database.isClosed())
      database.close();
  }

}
