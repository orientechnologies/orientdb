package com.orientechnologies.orient.distributed.context.coordination.message.operation;

import com.orientechnologies.orient.core.db.config.OAddNodeInfo;
import com.orientechnologies.orient.core.id.ODatabaseId;
import com.orientechnologies.orient.core.id.OGroupId;
import com.orientechnologies.orient.core.id.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.OCoordinatedDistributedOps;
import com.orientechnologies.orient.distributed.context.coordination.OVersion;
import com.orientechnologies.orient.distributed.context.coordination.message.ODistributedMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import java.util.Optional;
import java.util.Set;

public interface OOperationContext {

  OCoordinatedDistributedOps getOps();

  void mergeNode(
      ONodeId node,
      ONodeStateNetwork state,
      ONodeStateNetwork original,
      OTransactionIdPromise promise);

  void establish(OGroupId groupId, Set<ONodeId> candidates, OTransactionIdPromise promise);

  void distributedDrop(ODatabaseId dbId, OVersion version, OTransactionIdPromise promise);

  void declareDatabase(
      OTransactionIdPromise promise,
      ODatabaseId id,
      String name,
      Set<OAddNodeInfo> partecipants,
      int minimumQuorum);

  void registerNode(ONodeId node, OVersion version, OTransactionIdPromise promise);

  void recoordinateOperation(OTransactionIdPromise promise, OOperationMessage op);

  void apllied(OTransactionIdPromise promise);

  Optional<OAcceptResult> propose(ODistributedMessage message);

  void confirm(OTransactionIdPromise promise);

  void cancel(OTransactionIdPromise promise);
}
