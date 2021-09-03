package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 7/3/14
 */
@Test
public abstract class DocumentDBBaseTest extends BaseTest<ODatabaseDocumentInternal> {
  protected DocumentDBBaseTest() {}

  @Parameters(value = "url")
  protected DocumentDBBaseTest(@Optional String url) {
    super(url);
  }

  @Parameters(value = "url")
  protected DocumentDBBaseTest(@Optional String url, String prefix) {
    super(url, prefix);
  }

  protected ODatabaseDocumentInternal createDatabaseInstance(String url) {
    return new ODatabaseDocumentTx(url);
  }
}
