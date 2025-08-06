package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.distributed.context.coordination.message.ODistributedMessage;
import java.util.List;

public interface ODistributedMessageLog {

  /** Log received message for eventually re-send to other nodes
   *
   * @param message to log
   */
  void log(ODistributedMessage message);

  /**  Recover the messages with specified ids from the log.
   *
   *  The current implementation just miss from the result the messages that
   *  cannot be found.
   *
   * @param ids of messages to search
   * @return list of found messages
   */
  List<ODistributedMessage> recover(List<OTransactionId> ids);

  /** Remove from the log all the messages up to the provided status
   *
   * @param status clean state to use as base
   */
  void cleanFrom(long[] status);
}
