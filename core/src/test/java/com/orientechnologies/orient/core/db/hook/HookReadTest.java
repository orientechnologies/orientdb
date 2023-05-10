package com.orientechnologies.orient.core.db.hook;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.metadata.security.OSecurityPolicy;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Test;

/** Created by tglman on 01/06/16. */
public class HookReadTest extends BaseMemoryDatabase {

  public void beforeTests() {
    super.beforeTest();
    db.getMetadata().getSchema().createClass("TestClass");
  }

  @Test
  public void testSelectChangedInHook() {
    db.registerHook(
        new ORecordHook() {
          @Override
          public void onUnregister() {}

          @Override
          public RESULT onTrigger(TYPE iType, ORecord iRecord) {
            if (iType == TYPE.AFTER_READ
                && !((ODocument) iRecord)
                    .getClassName()
                    .equalsIgnoreCase(OSecurityPolicy.class.getSimpleName()))
              ((ODocument) iRecord).field("read", "test");
            return RESULT.RECORD_CHANGED;
          }

          @Override
          public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
            return null;
          }
        });

    db.save(new ODocument("TestClass"));

    OResultSet res = db.query("select from TestClass");
    assertEquals(res.next().getProperty("read"), "test");
  }
}
