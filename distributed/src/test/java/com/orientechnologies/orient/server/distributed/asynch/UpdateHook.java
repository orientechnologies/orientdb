package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;

public class UpdateHook implements ORecordHook {

  @Override
  public void onUnregister() {
  }

  @Override
  public RESULT onTrigger(TYPE iType, ORecord iRecord) {
    if (iType.equals(TYPE.AFTER_DELETE)) {
      if (iRecord instanceof ODocument) {
        ODocument doc = (ODocument) iRecord;
        if (doc.getSchemaClass() != null && doc.getSchemaClass().isSubClassOf(OrientEdgeType.CLASS_NAME)) {
          if (doc.field(OrientBaseGraph.CONNECTION_OUT) == null) {
            throw new RuntimeException(OrientBaseGraph.CONNECTION_OUT + " vertex can't be null");
          }
          if (doc.field(OrientBaseGraph.CONNECTION_IN) == null) {
            throw new RuntimeException(OrientBaseGraph.CONNECTION_IN + " vertex can't be null");
          }
        }
      }
    }

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
