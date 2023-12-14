package com.orientechnologies.orient.core.ridbag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.orientechnologies.BaseMemoryDatabase;
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

public class ORidBagBasicTest extends BaseMemoryDatabase {

  @Test
  public void embeddedRidBagSerializationTest() {
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
  }

  @Test(expected = IllegalArgumentException.class)
  public void testExceptionInCaseOfNull() {
    OEmbeddedRidBag bag = new OEmbeddedRidBag();
    bag.add(null);
  }

  @Test
  public void allowOnlyAtRoot() {
    try {
      OVertex record = db.newVertex();
      List<Object> valueList = new ArrayList<>();
      valueList.add(new ORidBag());
      record.setProperty("emb", valueList);
      db.save(record);
      fail("Should not be possible to save a ridbag in a list");
    } catch (ODatabaseException ex) {
      // this is expected
    }

    try {
      OVertex record = db.newVertex();
      Set<Object> valueSet = new HashSet<>();
      valueSet.add(new ORidBag());
      record.setProperty("emb", valueSet);
      db.save(record);
      fail("Should not be possible to save a ridbag in a set");
    } catch (ODatabaseException ex) {
      // this is expected
    }

    try {
      OVertex record = db.newVertex();
      Map<String, Object> valueSet = new HashMap<>();
      valueSet.put("key", new ORidBag());
      record.setProperty("emb", valueSet);
      db.save(record);
      fail("Should not be possible to save a ridbag in a set");
    } catch (ODatabaseException ex) {
      // this is expected
    }

    try {
      OVertex record = db.newVertex();
      Map<String, Object> valueSet = new HashMap<>();
      OElement nested = db.newEmbeddedElement();
      nested.setProperty("bag", new ORidBag());
      valueSet.put("key", nested);
      record.setProperty("emb", valueSet);
      db.save(record);
      fail("Should not be possible to save a ridbag in a set");
    } catch (ODatabaseException ex) {
      // this is expected
    }

    try {
      OVertex record = db.newVertex();
      List<Object> valueList = new ArrayList<>();
      OElement nested = db.newEmbeddedElement();
      nested.setProperty("bag", new ORidBag());
      valueList.add(nested);
      record.setProperty("emb", valueList);
      db.save(record);
      fail("Should not be possible to save a ridbag in a list");
    } catch (ODatabaseException ex) {
      // this is expected
    }

    try {
      OVertex record = db.newVertex();
      Set<Object> valueSet = new HashSet<>();
      OElement nested = db.newEmbeddedElement();
      nested.setProperty("bag", new ORidBag());
      valueSet.add(nested);
      record.setProperty("emb", valueSet);
      db.save(record);
      fail("Should not be possible to save a ridbag in a set");
    } catch (ODatabaseException ex) {
      // this is expected
    }

    try {
      OVertex record = db.newVertex();
      OElement nested = db.newEmbeddedElement();
      nested.setProperty("bag", new ORidBag());
      record.setProperty("emb", nested);
      db.save(record);
      fail("Should not be possible to save a ridbag in a set");
    } catch (ODatabaseException ex) {
      // this is expected
    }
  }
}
