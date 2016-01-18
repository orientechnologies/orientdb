package com.orientechnologies.orient.core.db.record.impl;

import static org.testng.AssertJUnit.assertEquals;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODirtyManager;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;

public class ODirtyManagerRidbagTest {

  private ODatabaseDocument db;

  @BeforeMethod
  public void before() {
    db = new ODatabaseDocumentTx("memory:" + ODirtyManagerRidbagTest.class.getSimpleName());
    db.create();
  }

  @Test
  public void testRidBagTree() {
    Object value = OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValue();
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(-1);
    try {
      ODocument doc = new ODocument();
      doc.field("test", "ddd");
      ORidBag bag = new ORidBag();
      ODocument doc1 = new ODocument();
      bag.add(doc1);
      doc.field("bag", bag);
      ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);
      ODirtyManager manager = ORecordInternal.getDirtyManager(doc1);
      assertEquals(2, manager.getNewRecord().size());
    } finally {
      OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(value);
    }
  }

  @AfterMethod
  public void after() {
    db.drop();
  }

}
