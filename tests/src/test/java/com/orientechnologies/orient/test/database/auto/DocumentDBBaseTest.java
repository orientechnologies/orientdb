package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 7/3/14
 */
@Test
public abstract class DocumentDBBaseTest extends BaseTest<ODatabaseDocumentTx> {
  @Parameters(value = "url")
  protected DocumentDBBaseTest(@Optional String url) {
    super(url);
  }

  protected ODatabaseDocumentTx createDatabaseInstance(String url) {
    return Orient.instance().getDatabaseFactory().createDatabase("graph", url);
  }
}