package com.orientechnologies.orient.core.record.impl;

import static org.testng.AssertJUnit.assertEquals;
import java.security.Identity;

import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.hook.ORecordHook.DISTRIBUTED_EXECUTION_MODE;
import com.orientechnologies.orient.core.hook.ORecordHook.RESULT;
import com.orientechnologies.orient.core.hook.ORecordHook.TYPE;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;

public class TestAvoidLazyLoadOnSerialization {
  private int readCount = 0;

  @Test
  public void testLinkListNotLazyLoad() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + TestAvoidLazyLoadOnSerialization.class.getSimpleName());
    db.create();
    db.registerHook(new ORecordHook() {

      @Override
      public void onUnregister() {

      }

      @Override
      public RESULT onTrigger(TYPE iType, ORecord iRecord) {
        if (iType == TYPE.AFTER_READ) {
          TestAvoidLazyLoadOnSerialization.this.readCount++;
        }
        return RESULT.RECORD_NOT_CHANGED;
      }

      @Override
      public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
        return DISTRIBUTED_EXECUTION_MODE.SOURCE_NODE;
      }
    });
    try {

      ODocument doc = new ODocument();
      ODocument oldOwner = new ODocument();
      ORID id = db.save(doc).getIdentity();
      ORecordLazyList list = new ORecordLazyList(oldOwner);
      list.add(id);
      ODocument toSerialize = new ODocument();
      toSerialize.field("list", list);
      int curCount = readCount;
      toSerialize.toStream();
      assertEquals(curCount, readCount);

    } finally {
      db.drop();
    }

  }

}
