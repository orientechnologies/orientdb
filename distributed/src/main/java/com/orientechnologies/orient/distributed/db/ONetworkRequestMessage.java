package com.orientechnologies.orient.distributed.db;

import com.orientechnologies.orient.core.db.ONetworkMessage;
import com.orientechnologies.orient.server.distributed.ODistributedDatabase;
import com.orientechnologies.orient.server.distributed.ODistributedRequest;
import com.orientechnologies.orient.server.distributed.OLoggerDistributed;
import com.orientechnologies.orient.server.distributed.impl.ODistributedDatabaseImpl;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ONetworkRequestMessage implements ONetworkMessage {

  private static final OLoggerDistributed logger =
      OLoggerDistributed.logger(ONetworkRequestMessage.class);
  private final OrientDBDistributed ctx;
  private final ODistributedRequest req;

  public ONetworkRequestMessage(OrientDBDistributed ctx) {
    this.ctx = ctx;
    req = new ODistributedRequest(ctx.getTaskFactoryManager());
  }

  @Override
  public void execute() {
    final String dbName = req.getDatabaseName();
    ODistributedDatabase ddb = null;
    boolean online = false;
    if (dbName != null) {
      try {
        online = ctx.waitOnline(dbName);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      if (req.getTask().isNodeOnlineRequired()) {
        ddb = ctx.getDatabase(dbName);
        if (online && ddb == null) {
          ctx.openNoAuthorization(dbName);
        }
        ddb = ctx.getDatabase(dbName);
        if (ddb == null) {
          logger.warnNode(
              ctx.getNodeId(),
              "Message %s require online database, but offline, dropping execution",
              req.toString());
          return;
        }
      }
    }
    if (ddb != null) {
      ddb.processRequest(req, true);
    } else {
      ODistributedDatabaseImpl.executeNoDb(req, ctx);
    }
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    req.fromStream(input);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    throw new UnsupportedOperationException("not needed so far");
  }
}
