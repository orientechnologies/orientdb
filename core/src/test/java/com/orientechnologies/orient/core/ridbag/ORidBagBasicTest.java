package com.orientechnologies.orient.core.ridbag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.db.record.ridbag.embedded.OEmbeddedRidBag;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.Test;

public class ORidBagBasicTest {

  @Test
  public void embeddedRidBagSerializationTest() {
    ODatabaseDocument db =
        new ODatabaseDocumentTx("memory:" + OEmbeddedRidBag.class.getSimpleName());
    db.create();
    try {
      OEmbeddedRidBag bag = new OEmbeddedRidBag();

      bag.add(new ORecordId(3, 1000));
      bag.convertLinks2Records();
      bag.convertRecords2Links();
      byte[] bytes = new byte[1024];
      UUID id = UUID.randomUUID();
      bag.serialize(bytes, 0, id);

      OEmbeddedRidBag bag1 = new OEmbeddedRidBag();
      bag1.deserialize(bytes, 0);

      assertEquals(bag.size(), 1);

      assertEquals(null, bag1.iterator().next());
    } finally {
      db.drop();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testExceptionInCaseOfNull() {
    OEmbeddedRidBag bag = new OEmbeddedRidBag();
    bag.add(null);
  }

  @Test
  public void allowOnlyAtRoot() {
    final OrientDB orientDB =
        OCreateDatabaseUtil.createDatabase(
            ORidBagBasicTest.class.getSimpleName(), "memory:", OCreateDatabaseUtil.TYPE_MEMORY);
    final ODatabaseSession session =
        orientDB.open(
            ORidBagBasicTest.class.getSimpleName(),
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    try {
      OVertex record = session.newVertex();
      List<Object> valueList = new ArrayList<>();
      valueList.add(new ORidBag());
      record.setProperty("emb", valueList);
      session.save(record);
      fail("Should not be possible to save a ridbag in a list");
    } catch (ODatabaseException ex) {
      // this is expected
    }

    try {
      OVertex record = session.newVertex();
      Set<Object> valueSet = new HashSet<>();
      valueSet.add(new ORidBag());
      record.setProperty("emb", valueSet);
      session.save(record);
      fail("Should not be possible to save a ridbag in a set");
    } catch (ODatabaseException ex) {
      // this is expected
    }

    try {
      OVertex record = session.newVertex();
      Map<String, Object> valueSet = new HashMap<>();
      valueSet.put("key", new ORidBag());
      record.setProperty("emb", valueSet);
      session.save(record);
      fail("Should not be possible to save a ridbag in a set");
    } catch (ODatabaseException ex) {
      // this is expected
    }

    try {
      OVertex record = session.newVertex();
      Map<String, Object> valueSet = new HashMap<>();
      OElement nested = session.newEmbeddedElement();
      nested.setProperty("bag", new ORidBag());
      valueSet.put("key", nested);
      record.setProperty("emb", valueSet);
      session.save(record);
      fail("Should not be possible to save a ridbag in a set");
    } catch (ODatabaseException ex) {
      // this is expected
    }

    try {
      OVertex record = session.newVertex();
      List<Object> valueList = new ArrayList<>();
      OElement nested = session.newEmbeddedElement();
      nested.setProperty("bag", new ORidBag());
      valueList.add(nested);
      record.setProperty("emb", valueList);
      session.save(record);
      fail("Should not be possible to save a ridbag in a list");
    } catch (ODatabaseException ex) {
      // this is expected
    }

    try {
      OVertex record = session.newVertex();
      Set<Object> valueSet = new HashSet<>();
      OElement nested = session.newEmbeddedElement();
      nested.setProperty("bag", new ORidBag());
      valueSet.add(nested);
      record.setProperty("emb", valueSet);
      session.save(record);
      fail("Should not be possible to save a ridbag in a set");
    } catch (ODatabaseException ex) {
      // this is expected
    }

    try {
      OVertex record = session.newVertex();
      OElement nested = session.newEmbeddedElement();
      nested.setProperty("bag", new ORidBag());
      record.setProperty("emb", nested);
      session.save(record);
      fail("Should not be possible to save a ridbag in a set");
    } catch (ODatabaseException ex) {
      // this is expected
    }
    session.close();
    orientDB.close();
  }
}
