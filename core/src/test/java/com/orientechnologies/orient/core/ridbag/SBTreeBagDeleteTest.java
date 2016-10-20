package com.orientechnologies.orient.core.ridbag;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManagerShared;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by tglman on 01/07/16.
 */
public class SBTreeBagDeleteTest {

  private ODatabaseDocumentInternal db;

  @Before
  public void before() {
    db = new ODatabaseDocumentTx("memory:" + SBTreeBagDeleteTest.class.getSimpleName());
    db.create();
  }

  @After
  public void after() {
    db.drop();
  }

  @Test
  public void testDeleteRidbagTx() {
    ODocument doc = new ODocument();
    ORidBag bag = new ORidBag();
    int size = OGlobalConfiguration.INDEX_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger() * 2;
    for (int i = 0; i < size; i++)
      bag.add(new ORecordId(10, i));
    doc.field("bag", bag);

    ORID id = db.save(doc).getIdentity();

    bag = doc.field("bag");
    OBonsaiCollectionPointer pointer = bag.getPointer();

    db.begin();
    db.delete(doc);
    db.commit();

    doc = db.load(id);
    assertNull(doc);

    ((OSBTreeCollectionManagerShared) db.getSbTreeCollectionManager()).clear();

    OSBTreeBonsai<OIdentifiable, Integer> tree = db.getSbTreeCollectionManager().loadSBTree(pointer);
    assertNull(tree);
  }

  @Test
  public void testDeleteRidbagNoTx() {
    ODocument doc = new ODocument();
    ORidBag bag = new ORidBag();
    int size = OGlobalConfiguration.INDEX_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger() * 2;
    for (int i = 0; i < size; i++)
      bag.add(new ORecordId(10, i));
    doc.field("bag", bag);

    ORID id = db.save(doc).getIdentity();

    bag = doc.field("bag");
    OBonsaiCollectionPointer pointer = bag.getPointer();

    db.delete(doc);

    doc = db.load(id);
    assertNull(doc);

    ((OSBTreeCollectionManagerShared) db.getSbTreeCollectionManager()).clear();

    OSBTreeBonsai<OIdentifiable, Integer> tree = db.getSbTreeCollectionManager().loadSBTree(pointer);
    assertNull(tree);
  }

}
