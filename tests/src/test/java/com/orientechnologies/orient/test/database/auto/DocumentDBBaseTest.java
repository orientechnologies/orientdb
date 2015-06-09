package com.orientechnologies.orient.test.database.auto;

import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 7/3/14
 */
@Test
public abstract class DocumentDBBaseTest extends BaseTest<ODatabaseDocumentTx> {
  protected DocumentDBBaseTest() {
  }

  @Parameters(value = "url")
  protected DocumentDBBaseTest(@Optional String url) {
    super(url);
  }

  @Parameters(value = "url")
  protected DocumentDBBaseTest(@Optional String url, String prefix) {
    super(url, prefix);
  }

  protected ODatabaseDocumentTx createDatabaseInstance(String url) {
    return Orient.instance().getDatabaseFactory().createDatabase("graph", url);
  }
}
