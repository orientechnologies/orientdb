package com.orientechnologies.orient.core.ridbag;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.db.record.ridbag.embedded.OEmbeddedRidBag;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.annotations.Test;

import java.util.UUID;

import static org.testng.AssertJUnit.assertEquals;

public class OEmbeddedRidBagBasicTest {

  @Test
  public void embeddedRidBagSerializationTest() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + OEmbeddedRidBag.class.getSimpleName());
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

  @Test(expectedExceptions = ODatabaseException.class)
  public void embeddedRidBagInvalidClusterTest() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + OEmbeddedRidBag.class.getSimpleName());
    db.create();
    db.getMetadata().getSchema().createClass("test");
    try {
      ODocument document = new ODocument("test");
      ORidBag ridBag = new ORidBag();
      ridBag.add(new ORecordId(-2, -30));
      document.field("ridbag", ridBag);
      db.save(document);
    } finally {
      db.drop();
    }

  }

  @Test(expectedExceptions = ODatabaseException.class)
  public void embeddedRidBagInvalidClusterPositionTest() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + OEmbeddedRidBag.class.getSimpleName());
    db.create();
    OClass cls = db.getMetadata().getSchema().createClass("test");
    try {
      ODocument document = new ODocument("test");
      ORidBag ridBag = new ORidBag();
      ridBag.add(new ORecordId(cls.getDefaultClusterId(), -30));
      document.field("ridbag", ridBag);
      db.save(document);
    } finally {
      db.drop();
    }

  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testExceptionInCaseOfNull() {
    OEmbeddedRidBag bag = new OEmbeddedRidBag();
    bag.add(null);

  }

}
