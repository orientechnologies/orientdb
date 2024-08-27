package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.object.db.OObjectDatabasePool;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 7/3/14
 */
@Test
public class ObjectDBBaseTest extends BaseTest<ODatabaseObject> {
  public ObjectDBBaseTest() {}

  @Parameters(value = "url")
  public ObjectDBBaseTest(@Optional String url) {
    super(url);
  }

  @Parameters(value = "url")
  public ObjectDBBaseTest(@Optional String url, String prefix) {
    super(url, prefix);
  }

  @Override
  protected OObjectDatabaseTx createDatabaseInstance(String url) {
    return new OObjectDatabaseTx(url);
  }

  protected ODatabaseSession rawSession(String user, String password) {
    ODatabaseDocumentTx session = new ODatabaseDocumentTx(this.url);
    session.open(user, password);
    return session;
  }

  protected void reopendb(String user, String password) {
    if (!database.isClosed() && !database.isActiveOnCurrentThread()) {
      database = new OObjectDatabaseTx(this.url);
    }
    database.open(user, password);
  }

  protected void reopenpool(String user, String password) {
    database = OObjectDatabasePool.global().acquire(url, user, password);
  }

  protected ODatabaseObject openpool(String user, String password) {
    return OObjectDatabasePool.global().acquire(url, user, password);
  }
}
