package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseFlat;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 7/10/14
 */
public abstract class FlatDBBaseTest extends BaseTest<ODatabaseFlat> {
	@Parameters(value = "url")
  protected FlatDBBaseTest(@Optional String url) {
    super(url);
  }

  @Override
  protected ODatabaseFlat createDatabaseInstance(String url) {
    return new ODatabaseFlat(url);
  }

	@Override
	protected void createDatabase() {
		ODatabaseDocumentTx db = new ODatabaseDocumentTx(database.getURL());
		db.create();
		db.close();

		database.open("admin", "admin");
	}
}
