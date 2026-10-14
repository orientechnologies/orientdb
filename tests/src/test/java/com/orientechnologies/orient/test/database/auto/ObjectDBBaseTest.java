package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.object.db.OrientDBObject;
import java.io.IOException;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 7/3/14
 */
@Test
public class ObjectDBBaseTest extends BaseTest<ODatabaseObject> {
  private static final OLogger logger = OLogManager.instance().logger(ObjectDBBaseTest.class);
  protected OrientDBObject objectContext;

  public ObjectDBBaseTest() {
    this.objectContext = new OrientDBObject(this.baseContext);
  }

  @Parameters(value = "url")
  public ObjectDBBaseTest(@Optional String url) {
    super(url);
    this.objectContext = new OrientDBObject(this.baseContext);
  }

  protected ObjectDBBaseTest(@Optional String url, String prefix) {
    super(url, prefix);
    this.objectContext = new OrientDBObject(this.baseContext);
  }

  protected ODatabaseSession rawSession(String user, String password) {
    return baseContext.open(data.getDbName(), user, password);
  }

  protected ODatabaseSession rawSession(String database, String user, String password) {
    return baseContext.open(database, user, password);
  }

  protected ODatabaseObject session(String database, String user, String password) {
    return objectContext.open(database, user, password);
  }

  protected void dropAndCreateDatabase(String database) throws IOException {
    if (baseContext.exists(database)) {
      baseContext.drop(database);
    }
    createBaseDb(database);
  }

  protected void reopendb(String user, String password) {
    database = objectContext.open(data.getDbName(), user, password);
  }

  protected void reopenpool(String user, String password) {
    if (!database.isClosed()) {
      database.close();
    }
    database = objectContext.open(data.getDbName(), user, password);
  }

  protected ODatabaseObject openpool(String user, String password) {
    return session(data.getDbName(), user, password);
  }

  protected void dropdb() throws IOException {
    logger.info("deleting database %s", url);
    baseContext.drop(data.getDbName());
  }
}
