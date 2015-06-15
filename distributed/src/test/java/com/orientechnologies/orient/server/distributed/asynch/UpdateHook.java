package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;

public class UpdateHook implements ORecordHook {

  @Override
  public void onUnregister() {
  }

  @Override
  public RESULT onTrigger(TYPE iType, ORecord iRecord) {
    if (iType.equals(TYPE.AFTER_CREATE) || iType.equals(TYPE.AFTER_UPDATE) || iType.equals(TYPE.AFTER_DELETE)) {
    }
    switch (iType) {
    case AFTER_CREATE: {
      if (((ODocument) iRecord).getSchemaClass().isSubClassOf(OrientEdgeType.CLASS_NAME)) {
        HookFireCounter.incrementEdgeCreatedCnt();
      } else {
        HookFireCounter.incrementVertexCreatedCnt();
      }
      break;
    }
    case AFTER_UPDATE: {
      if (((ODocument) iRecord).getSchemaClass().isSubClassOf(OrientEdgeType.CLASS_NAME)) {
        HookFireCounter.incrementEdgeUpdatedCnt();
      } else {
        HookFireCounter.incrementVertexUpdatedCnt();
      }
      break;
    }
    default: {
    }
    }
    return null;
  }

  @Override
  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.SOURCE_NODE;
  }

}
