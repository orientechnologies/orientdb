package com.orientechnologies.orient.distributed.context.coordination.message.operation;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.ONodeState;
import com.orientechnologies.orient.distributed.context.coordination.OVersion;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import java.util.Optional;
import java.util.Set;

public interface OOperationContext {

  ONodeState getNodeState();

  void mergeNode(
      ONodeId node,
      ONodeStateNetwork state,
      ONodeStateNetwork original,
      OTransactionIdPromise promise);

  void establish(OGroupId groupId, Set<ONodeId> candidates, OTransactionIdPromise promise);

  void distributedDrop(ODatabaseId dbId, OVersion version, OTransactionIdPromise promise);

  Optional<OAcceptResult> validateDropDatabase(
      ODatabaseId dbId, OVersion version, OTransactionIdPromise promise);

  void cancelDropDatabase(ODatabaseId dbId, OVersion version, OTransactionIdPromise promise);

  Optional<OAcceptResult> promiseDeclare(
      OTransactionIdPromise promise,
      ODatabaseId id,
      String name,
      Set<OAddNodeInfo> partecipants,
      int minimumQuorum);

  void declareDatabase(
      OTransactionIdPromise promise,
      ODatabaseId id,
      String name,
      Set<OAddNodeInfo> partecipants,
      int minimumQuorum);

  void cancelDeclare(OTransactionIdPromise promise, ODatabaseId id, String name);

  void registerNode(ONodeId node, OVersion version, OTransactionIdPromise promise);

  void cancelRegisterPromise(OTransactionIdPromise promise);
}
