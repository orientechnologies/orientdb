package com.orientechnologies.orient.server;

import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import java.io.IOException;

/** Created by tglman on 14/08/17. */
public interface OServerAware {

  void init(OServer server);

  void coordinatedRequest(
      OClientConnection connection, int requestType, int clientTxId, OChannelBinary channel)
      throws IOException;

  ODistributedServerManager getDistributedManager();
}
