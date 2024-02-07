package com.orientechnologies.orient.core.index;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import org.junit.Assert;
import org.junit.Test;

/** Created by tglman on 01/02/16. */
public class UniqueIndexTest extends BaseMemoryDatabase {

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

  @Test
  public void testUniqueOnUpdateNegativeVersion() {
    final OSchema schema = db.getMetadata().getSchema();
    OClass userClass = schema.createClass("User");
    userClass.createProperty("MailAddress", OType.STRING).createIndex(OClass.INDEX_TYPE.UNIQUE);

    ODocument jane = new ODocument("User");
    jane.field("MailAddress", "jane@doe.com");
    jane.save();

    final ORID rid = jane.getIdentity();

    reOpen("admin", "adminpwd");

    ODocument joneJane = db.load(rid);

    joneJane.field("MailAddress", "john@doe.com");
    joneJane.field("@version", -1);

    joneJane.save();

    reOpen("admin", "adminpwd");

    try {
      ODocument toUp = new ODocument("User");
      toUp.field("MailAddress", "john@doe.com");

      db.save(toUp);
      Assert.fail("Expected record duplicate exception");
    } catch (ORecordDuplicatedException ex) {
    }

    final OResultSet result = db.query("select from User where MailAddress = 'john@doe.com'");
    Assert.assertEquals(result.stream().count(), 1);
  }
}
