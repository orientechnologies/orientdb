package com.orientechnologies.orient.core.db.hook;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class HookRegisterRemoveTest {

  @Test
  public void addAndRemoveHookTest() {

    ODatabaseDocumentTx db =
        new ODatabaseDocumentTx("memory:" + HookRegisterRemoveTest.class.getSimpleName());
    db.create();
    final AtomicInteger integer = new AtomicInteger(0);
    ORecordHook iHookImpl =
        new ORecordHook() {

          @Override
          public void onUnregister() {}

          @Override
          public RESULT onTrigger(TYPE iType, ORecord iRecord) {
            integer.incrementAndGet();
            return null;
          }

          @Override
          public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
            return null;
          }
        };
    db.registerHook(iHookImpl);

    db.save(new ODocument().field("test", "test"), db.getClusterNameById(db.getDefaultClusterId()));
    assertEquals(3, integer.get());
    db.unregisterHook(iHookImpl);
    db.save(new ODocument(), db.getClusterNameById(db.getDefaultClusterId()));
    assertEquals(3, integer.get());
    db.drop();
  }
}
