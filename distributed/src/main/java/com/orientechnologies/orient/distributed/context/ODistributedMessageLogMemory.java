package com.orientechnologies.orient.distributed.context;

import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.distributed.context.coordination.message.ODistributedMessage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ODistributedMessageLogMemory implements ODistributedMessageLog {

  private final List<ODistributedMessage> log;

  public ODistributedMessageLogMemory() {
    log = Collections.synchronizedList(new ArrayList<>());
  }

  @Override
  public void log(ODistributedMessage message) {
    this.log.add(message);
  }

  @Override
  public List<ODistributedMessage> recover(List<OTransactionId> ids) {
    Set<OTransactionId> toSearch = new HashSet<>(ids);
    var iterator = this.log.listIterator();
    List<ODistributedMessage> found = new ArrayList<>();
    while (iterator.hasPrevious()) {
      ODistributedMessage message = iterator.previous();
      if (toSearch.contains(message.getPromiseId().getId())) {
        found.add(message);
      }
    }

    return found;
  }

  public void cleanFrom(long[] status) {
    var iterator = this.log.listIterator();
    while (iterator.hasNext()) {
      ODistributedMessage message = iterator.next();
      OTransactionId id = message.getPromiseId().getId();
      if (status[id.getPosition()] >= id.getSequence()) {
        iterator.remove();
      }
    }
  }
}
