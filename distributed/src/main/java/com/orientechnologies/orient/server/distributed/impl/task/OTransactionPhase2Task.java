package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.server.distributed.task.OAbstractReplicatedTask;

/**
 * @author Luigi Dell'Aquila (l.dellaquila - at - orientdb.com)
 */
public class OTransactionPhase2Task extends OAbstractReplicatedTask {
  public static final int FACTORYID = 44;

  private ODistributedRequestId transactionId;
  private boolean               success;

  public OTransactionPhase2Task(ODistributedRequestId transactionId, boolean success) {
    this.transactionId = transactionId;
    this.success = success;
  }

  @Override
  public String getName() {
    return "TxPhase2";
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.WRITE;
  }

  @Override
  public Object execute(ODistributedRequestId requestId, OServer iServer, ODistributedServerManager iManager,
      ODatabaseDocumentInternal database) throws Exception {
    if (success) {
      ((ODatabaseDocumentDistributed) database).commit2pc(transactionId);
    } else {
      ((ODatabaseDocumentDistributed) database).rollback2pc(transactionId);
    }
    return null; //TODO
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }
}
