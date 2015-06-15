package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.record.ORecord;

public class UpdateHook implements ORecordHook {

  @Override
  public void onUnregister() {
  }

  @Override
  public RESULT onTrigger(TYPE iType, ORecord iRecord) {
    if (iType.equals(TYPE.AFTER_CREATE) || iType.equals(TYPE.AFTER_UPDATE) || iType.equals(TYPE.AFTER_DELETE)) {
      // OLogManager.instance().info(this, iType + ": " + iRecord + " at: " + System.currentTimeMillis());
    }
    return null;
  }

  @Override
  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.SOURCE_NODE;
  }

}
