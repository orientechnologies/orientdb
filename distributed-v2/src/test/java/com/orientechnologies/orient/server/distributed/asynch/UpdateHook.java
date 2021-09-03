package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class UpdateHook implements ORecordHook {

  @Override
  public void onUnregister() {}

  @Override
  public RESULT onTrigger(TYPE iType, ORecord iRecord) {
    if (iType.equals(TYPE.AFTER_DELETE)) {
      if (iRecord instanceof ODocument) {
        ODocument doc = (ODocument) iRecord;
        if (doc.getSchemaClass() != null && doc.getSchemaClass().isSubClassOf("E")) {
          if (doc.field("out") == null) {
            throw new RuntimeException("out vertex can't be null");
          }
          if (doc.field("in") == null) {
            throw new RuntimeException("in vertex can't be null");
          }
        }
      }
    }

    if (iType.equals(TYPE.AFTER_CREATE)
        || iType.equals(TYPE.AFTER_UPDATE)
        || iType.equals(TYPE.AFTER_DELETE)) {
      // OLogManager.instance().info(this, iType + ": " + iRecord + " at: " +
      // System.currentTimeMillis());
    }
    return null;
  }

  @Override
  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.SOURCE_NODE;
  }
}
