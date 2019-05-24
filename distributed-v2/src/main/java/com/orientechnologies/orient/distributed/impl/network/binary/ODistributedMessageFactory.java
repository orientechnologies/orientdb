package com.orientechnologies.orient.distributed.impl.network.binary;

import com.orientechnologies.orient.distributed.impl.coordinator.network.ODistributedMessage;

public interface ODistributedMessageFactory {
  ODistributedMessage createMessage(int message);
}
