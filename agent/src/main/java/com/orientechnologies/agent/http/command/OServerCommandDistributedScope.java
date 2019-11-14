package com.orientechnologies.agent.http.command;

import com.orientechnologies.agent.cloud.processor.tasks.EnterpriseStatsResponse;
import com.orientechnologies.agent.cloud.processor.tasks.NewEnterpriseStatsTask;
import com.orientechnologies.agent.operation.NodeResponse;
import com.orientechnologies.agent.operation.OperationResponseFromNode;
import com.orientechnologies.agent.operation.ResponseOk;
import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.operation.NodeOperation;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by Enrico Risa on 16/11/15.
 */
public abstract class OServerCommandDistributedScope extends OServerCommandDistributedAuthenticated {

  private final OEnterpriseServer enterpriseServer;

  protected OServerCommandDistributedScope(String iRequiredResource, OEnterpriseServer enterpriseServer) {
    super(iRequiredResource);
    this.enterpriseServer = enterpriseServer;

  }

  public List<OperationResponseFromNode> sendTask(OHttpRequest iRequest, NodeOperation op) {

    String node = iRequest.getParameter("node");
    if ("_all".equalsIgnoreCase(node)) {
      return enterpriseServer.getNodesManager().sendAll(op);
    } else {
      return Collections.singletonList(enterpriseServer.getNodesManager().send(node, op));
    }

  }

  abstract void proxyRequest(OHttpRequest iRequest, OHttpResponse iResponse);

  protected ODatabaseDocumentInternal getProfiledDatabaseInstance(final OHttpRequest iRequest) throws InterruptedException {
    // after authentication, if current login user is different compare with current DB user, reset DB user to login user
    ODatabaseDocumentInternal localDatabase = ODatabaseRecordThreadLocal.instance().getIfDefined();

    if (localDatabase == null) {
      final List<String> parts = OStringSerializerHelper.split(iRequest.authorization, ':');
      localDatabase = server.openDatabase(iRequest.databaseName, parts.get(0), parts.get(1));
    } else {

      String currentUserId = iRequest.data.currentUserId;
      if (currentUserId != null && currentUserId.length() > 0 && localDatabase != null && localDatabase.getUser() != null) {
        if (!currentUserId.equals(localDatabase.getUser().getIdentity().toString())) {
          ODocument userDoc = localDatabase.load(new ORecordId(currentUserId));
          localDatabase.setUser(new OUser(userDoc));
        }
      }
    }

    iRequest.data.lastDatabase = localDatabase.getName();
    iRequest.data.lastUser = localDatabase.getUser() != null ? localDatabase.getUser().getName() : null;
    return (ODatabaseDocumentInternal) localDatabase.getDatabaseOwner();
  }

  @Override
  public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {

    try {
      if (isLocalNode(iRequest)) {
        if ("GET".equalsIgnoreCase(iRequest.httpMethod)) {
          doGet(iRequest, iResponse);
        } else if ("POST".equalsIgnoreCase(iRequest.httpMethod)) {
          doPost(iRequest, iResponse);
        } else if ("PUT".equalsIgnoreCase(iRequest.httpMethod)) {
          doPut(iRequest, iResponse);
        } else if ("DELETE".equalsIgnoreCase(iRequest.httpMethod)) {
          doDelete(iRequest, iResponse);
        }
      } else {
        proxyRequest(iRequest, iResponse);
      }
    } catch (Exception e) {
      iResponse.send(OHttpUtils.STATUS_BADREQ_CODE, OHttpUtils.STATUS_BADREQ_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN, e, null);
    }
    return false;
  }

  protected void doGet(OHttpRequest iRequest, OHttpResponse iResponse) throws IOException {

  }

  protected void doDelete(OHttpRequest iRequest, OHttpResponse iResponse) throws IOException {

  }

  protected void doPost(OHttpRequest iRequest, OHttpResponse iResponse) throws IOException {

  }

  protected void doPut(OHttpRequest iRequest, OHttpResponse iResponse) throws IOException {

  }
}
