package com.orientechnologies.orient.core.metadata.security;

import java.util.HashSet;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class ORestricetedUserCleanUpTest {

  @Test
  public void testAutoCleanUserAfterDelate() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + ORestricetedUserCleanUpTest.class.getSimpleName());
    db.create();
    try {
      OSchema schema = db.getMetadata().getSchema();
      schema.createClass("TestRecord", schema.getClass(OSecurityShared.RESTRICTED_CLASSNAME));

      OSecurity security = db.getMetadata().getSecurity();
      OUser auser = security.createUser("auser", "whereever", new String[] {});
      OUser reader = security.getUser("admin");
      ODocument doc = new ODocument("TestRecord");
      Set<OIdentifiable> users = new HashSet<OIdentifiable>();
      users.add(auser.getIdentity());
      users.add(reader.getIdentity());
      doc.field(OSecurityShared.ALLOW_READ_FIELD, users);
      doc.field(OSecurityShared.ALLOW_UPDATE_FIELD, users);
      doc.field(OSecurityShared.ALLOW_DELETE_FIELD, users);
      doc.field(OSecurityShared.ALLOW_ALL_FIELD, users);
      ODocument rid = db.save(doc);

      security.dropUser("auser");
      db.getLocalCache().clear();
      doc = db.load(rid.getIdentity());
      Assert.assertEquals(((Set<?>) doc.field(OSecurityShared.ALLOW_ALL_FIELD)).size(), 2);
      Assert.assertEquals(((Set<?>) doc.field(OSecurityShared.ALLOW_UPDATE_FIELD)).size(), 2);
      Assert.assertEquals(((Set<?>) doc.field(OSecurityShared.ALLOW_DELETE_FIELD)).size(), 2);
      Assert.assertEquals(((Set<?>) doc.field(OSecurityShared.ALLOW_ALL_FIELD)).size(), 2);
      doc.field("abc", "abc");
      doc.save();

      db.getLocalCache().clear();
      doc = db.load(rid.getIdentity());
      ((Set<?>) doc.field(OSecurityShared.ALLOW_ALL_FIELD)).remove(null);
      ((Set<?>) doc.field(OSecurityShared.ALLOW_UPDATE_FIELD)).remove(null);
      ((Set<?>) doc.field(OSecurityShared.ALLOW_DELETE_FIELD)).remove(null);
      ((Set<?>) doc.field(OSecurityShared.ALLOW_ALL_FIELD)).remove(null);

      Assert.assertEquals(((Set<?>) doc.field(OSecurityShared.ALLOW_ALL_FIELD)).size(), 1);
      Assert.assertEquals(((Set<?>) doc.field(OSecurityShared.ALLOW_UPDATE_FIELD)).size(), 1);
      Assert.assertEquals(((Set<?>) doc.field(OSecurityShared.ALLOW_DELETE_FIELD)).size(), 1);
      Assert.assertEquals(((Set<?>) doc.field(OSecurityShared.ALLOW_ALL_FIELD)).size(), 1);
      doc.field("abc", "abc");
      doc.save();
    } finally {
      db.drop();
    }

  }

}
