package com.orientechnologies.orient.core.metadata.index;

import static org.junit.Assert.fail;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import org.junit.Test;

public class TestImmutableIndexLoad {

  @Test
  public void testLoadAndUseIndexOnOpen() {
    OrientDB orientDB =
        OCreateDatabaseUtil.createDatabase(
            TestImmutableIndexLoad.class.getSimpleName(),
            "embedded:./target/",
            OCreateDatabaseUtil.TYPE_PLOCAL);
    ODatabaseSession db =
        orientDB.open(
            TestImmutableIndexLoad.class.getSimpleName(),
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    OClass one = db.createClass("One");
    OProperty property = one.createProperty("one", OType.STRING);
    property.createIndex(OClass.INDEX_TYPE.UNIQUE);
    db.close();
    orientDB.close();

    orientDB =
        new OrientDB(
            "embedded:./target/",
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    db =
        orientDB.open(
            TestImmutableIndexLoad.class.getSimpleName(),
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    ODocument doc = new ODocument("One");
    doc.setProperty("one", "a");
    db.save(doc);
    try {
      ODocument doc1 = new ODocument("One");
      doc1.setProperty("one", "a");
      db.save(doc1);
      fail("It should fail the unique index");
    } catch (ORecordDuplicatedException e) {
      // EXPEXTED
    }
    db.close();
    orientDB.drop(TestImmutableIndexLoad.class.getSimpleName());
    orientDB.close();
  }
}
