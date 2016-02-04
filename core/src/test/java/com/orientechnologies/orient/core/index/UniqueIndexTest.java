package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Created by tglman on 01/02/16.
 */
public class UniqueIndexTest {

  private ODatabaseDocument db;

  @BeforeMethod
  public void before() {
    db = new ODatabaseDocumentTx("memory:" + UniqueIndexTest.class.getSimpleName());
    db.create();
  }

  @Test()
  public void testUniqueOnUpdate() {
    final OSchema schema = db.getMetadata().getSchema();
    OClass userClass = schema.createClass("User");
    userClass.createProperty("MailAddress", OType.STRING).createIndex(OClass.INDEX_TYPE.UNIQUE);

    ODocument john = new ODocument("User");
    john.field("MailAddress", "john@doe.com");
    db.save(john);

    ODocument jane = new ODocument("User");
    jane.field("MailAddress", "jane@doe.com");
    ODocument id = jane.save();
    db.save(jane);

    try {
      ODocument toUp = db.load(id.getIdentity());
      toUp.field("MailAddress", "john@doe.com");
      db.save(toUp);
      Assert.fail("Expected record duplicate exception");
    } catch (ORecordDuplicatedException ex) {
    }
    ODocument fromDb = db.load(id.getIdentity());
    Assert.assertEquals(fromDb.field("MailAddress"), "jane@doe.com");
  }

  @AfterMethod
  public void after() {
    db.drop();
  }

}
