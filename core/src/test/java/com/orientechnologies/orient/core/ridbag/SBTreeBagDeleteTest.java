package com.orientechnologies.orient.core.ridbag;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.RID_BAG_SBTREEBONSAI_DELETE_DELAY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import java.util.Collections;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Created by tglman on 01/07/16. */
public class SBTreeBagDeleteTest {

  private ODatabaseDocumentInternal db;

  @Before
  public void before() {
    db = new ODatabaseDocumentTx("memory:" + SBTreeBagDeleteTest.class.getSimpleName());
    db.create();
    db.getConfiguration().setValue(RID_BAG_SBTREEBONSAI_DELETE_DELAY, 50);
  }

  @After
  public void after() {
    db.drop();
  }

  @Test
  public void testDeleteRidbagTx() throws InterruptedException {

    ODocument doc = new ODocument();
    ORidBag bag = new ORidBag();
    int size =
        OGlobalConfiguration.INDEX_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger() * 2;
    for (int i = 0; i < size; i++) bag.add(new ORecordId(10, i));
    doc.field("bag", bag);

    ORID id = db.save(doc, db.getClusterNameById(db.getDefaultClusterId())).getIdentity();

    bag = doc.field("bag");
    OBonsaiCollectionPointer pointer = bag.getPointer();

    db.begin();
    db.delete(doc);
    db.commit();

    doc = db.load(id);
    assertNull(doc);

    Thread.sleep(100);
    OSBTreeBonsai<OIdentifiable, Integer> tree =
        db.getSbTreeCollectionManager().loadSBTree(pointer);
    assertEquals(0, tree.getRealBagSize(Collections.emptyMap()));
  }

  @Test
  public void testDeleteRidbagNoTx() throws InterruptedException {
    ODocument doc = new ODocument();
    ORidBag bag = new ORidBag();
    int size =
        OGlobalConfiguration.INDEX_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger() * 2;
    for (int i = 0; i < size; i++) bag.add(new ORecordId(10, i));
    doc.field("bag", bag);

    ORID id = db.save(doc, db.getClusterNameById(db.getDefaultClusterId())).getIdentity();

    bag = doc.field("bag");
    OBonsaiCollectionPointer pointer = bag.getPointer();

    db.delete(doc);

    doc = db.load(id);
    assertNull(doc);

    Thread.sleep(100);

    OSBTreeBonsai<OIdentifiable, Integer> tree =
        db.getSbTreeCollectionManager().loadSBTree(pointer);
    assertEquals(0, tree.getRealBagSize(Collections.emptyMap()));
  }
}
