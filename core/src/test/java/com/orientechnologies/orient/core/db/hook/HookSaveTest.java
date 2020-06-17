package com.orientechnologies.orient.core.db.hook;

import static org.junit.Assert.assertNotNull;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Created by tglman on 01/06/16. */
public class HookSaveTest {

  private ODatabaseDocument database;

  @Before
  public void before() {
    database = new ODatabaseDocumentTx("memory:" + HookSaveTest.this.getClass().getSimpleName());
    database.create();
  }

  @After
  public void after() {
    database.drop();
  }

  @Test
  public void testCreatedLinkedInHook() {
    database.registerHook(
        new ORecordHook() {
          @Override
          public void onUnregister() {}

          @Override
          public RESULT onTrigger(TYPE iType, ORecord iRecord) {
            if (iType != TYPE.BEFORE_CREATE) return RESULT.RECORD_NOT_CHANGED;
            ODocument doc = (ODocument) iRecord;
            if (doc.containsField("test")) return RESULT.RECORD_NOT_CHANGED;
            ODocument doc1 = new ODocument("test");
            doc1.field("test", "value");
            doc.field("testNewLinkedRecord", doc1);
            return RESULT.RECORD_CHANGED;
          }

          @Override
          public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
            return null;
          }
        });

    ODocument doc = database.save(new ODocument("test"));
    ODocument newRef = doc.field("testNewLinkedRecord");
    assertNotNull(newRef);
    assertNotNull(newRef.getIdentity().isPersistent());
  }

  @Test
  public void testCreatedBackLinkedInHook() {
    database.registerHook(
        new ORecordHook() {
          @Override
          public void onUnregister() {}

          @Override
          public RESULT onTrigger(TYPE iType, ORecord iRecord) {
            if (iType != TYPE.BEFORE_CREATE) return RESULT.RECORD_NOT_CHANGED;
            ODocument doc = (ODocument) iRecord;
            if (doc.containsField("test")) return RESULT.RECORD_NOT_CHANGED;
            ODocument doc1 = new ODocument("test");
            doc1.field("test", "value");
            doc.field("testNewLinkedRecord", doc1);
            doc1.field("backLink", doc);
            return RESULT.RECORD_CHANGED;
          }

          @Override
          public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
            return null;
          }
        });

    ODocument doc = database.save(new ODocument("test"));
    ODocument newRef = doc.field("testNewLinkedRecord");
    assertNotNull(newRef);
    assertNotNull(newRef.getIdentity().isPersistent());
  }
}
