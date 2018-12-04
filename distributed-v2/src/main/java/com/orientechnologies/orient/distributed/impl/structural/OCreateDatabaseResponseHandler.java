package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OSessionOperationId;

public class OCreateDatabaseResponseHandler implements OStructuralResponseHandler {

  private OStructuralDistributedMember sender;
  private OSessionOperationId          sessionOperationId;
  private String                       database;
  private OrientDBDistributed          orientDB;

  public OCreateDatabaseResponseHandler(OStructuralDistributedMember sender, OSessionOperationId sessionOperationId,
      String database, OrientDBDistributed orientDB) {
    this.sender = sender;
    this.sessionOperationId = sessionOperationId;
    this.database = database;
    this.orientDB = orientDB;
  }

  @Override
  public boolean receive(OStructuralCoordinator coordinator, OStructuralRequestContext context, OStructuralDistributedMember member,
      OStructuralNodeResponse response) {
    OCreateDatabaseOperationResponse res = (OCreateDatabaseOperationResponse) response;
    //TODO:Check the response for eventual errors.

    if (context.getInvolvedMembers().size() == context.getResponses().size()) {
      for (OStructuralDistributedMember node : context.getInvolvedMembers()) {
        orientDB.getPlugin().setDatabaseStatus(node.getName(), database, ODistributedServerManager.DB_STATUS.ONLINE);
      }
      coordinator.reply(sender, sessionOperationId, new OCreateDatabaseSubmitResponse());
    }

    return false;
  }

  @Override
  public boolean timeout(OStructuralCoordinator coordinator, OStructuralRequestContext context) {
    return false;
  }
}
