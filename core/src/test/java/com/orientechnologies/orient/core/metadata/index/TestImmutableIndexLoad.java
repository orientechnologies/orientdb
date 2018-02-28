package com.orientechnologies.orient.core.metadata.index;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import org.junit.Test;

import static org.junit.Assert.fail;

public class TestImmutableIndexLoad {

  @Test
  public void testLoadAndUseIndexOnOpen() {
    OrientDB orientDB = new OrientDB("embedded:./target/", OrientDBConfig.defaultConfig());
    orientDB.create(TestImmutableIndexLoad.class.getSimpleName(), ODatabaseType.PLOCAL);
    ODatabaseSession db = orientDB.open(TestImmutableIndexLoad.class.getSimpleName(), "admin", "admin");
    OClass one = db.createClass("One");
    OProperty property = one.createProperty("one", OType.STRING);
    property.createIndex(OClass.INDEX_TYPE.UNIQUE);
    db.close();
    orientDB.close();

    orientDB = new OrientDB("embedded:./target/", OrientDBConfig.defaultConfig());
    db = orientDB.open(TestImmutableIndexLoad.class.getSimpleName(), "admin", "admin");
    ODocument doc = new ODocument("One");
    doc.setProperty("one", "a");
    db.save(doc);
    try {
      ODocument doc1 = new ODocument("One");
      doc1.setProperty("one", "a");
      db.save(doc1);
      fail("It should fail the unique index");
    } catch (ORecordDuplicatedException e) {
      //EXPEXTED
    }
    db.close();
    orientDB.drop(TestImmutableIndexLoad.class.getSimpleName());
    orientDB.close();
  }

}
