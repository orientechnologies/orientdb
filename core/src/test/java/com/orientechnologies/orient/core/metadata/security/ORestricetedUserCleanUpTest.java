package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.common.directmemory.ODirectMemoryAllocator;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.HashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

public class ORestricetedUserCleanUpTest {

  @Test
  public void testAutoCleanUserAfterDelate() {
    ODatabaseDocument db =
        new ODatabaseDocumentTx("memory:" + ORestricetedUserCleanUpTest.class.getSimpleName());
    db.create();
    try {
      OSchema schema = db.getMetadata().getSchema();
      schema.createClass("TestRecord", schema.getClass(OSecurityShared.RESTRICTED_CLASSNAME));

      System.gc();
      ODirectMemoryAllocator.instance().checkTrackedPointerLeaks();

      OSecurity security = db.getMetadata().getSecurity();
      OUser auser = security.createUser("auser", "wherever", new String[] {});
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

      System.gc();
      ODirectMemoryAllocator.instance().checkTrackedPointerLeaks();
      security.dropUser("auser");
      db.getLocalCache().clear();
      doc = db.load(rid.getIdentity());
      Assert.assertEquals(((Set<?>) doc.field(OSecurityShared.ALLOW_ALL_FIELD)).size(), 2);
      Assert.assertEquals(((Set<?>) doc.field(OSecurityShared.ALLOW_UPDATE_FIELD)).size(), 2);
      Assert.assertEquals(((Set<?>) doc.field(OSecurityShared.ALLOW_DELETE_FIELD)).size(), 2);
      Assert.assertEquals(((Set<?>) doc.field(OSecurityShared.ALLOW_ALL_FIELD)).size(), 2);
      doc.field("abc", "abc");
      doc.save();

      System.gc();
      ODirectMemoryAllocator.instance().checkTrackedPointerLeaks();
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
      System.gc();
      ODirectMemoryAllocator.instance().checkTrackedPointerLeaks();
    } finally {
      db.drop();
    }

    System.gc();
    ODirectMemoryAllocator.instance().checkTrackedPointerLeaks();
  }
}
