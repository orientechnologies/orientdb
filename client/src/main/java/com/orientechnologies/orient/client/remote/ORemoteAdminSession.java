package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.client.remote.message.ORemoteResultSet;
import com.orientechnologies.orient.client.remote.message.OServerQueryRequest;
import com.orientechnologies.orient.client.remote.message.OServerQueryResponse;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.OAdminSession;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37Client;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Map;

public class ORemoteAdminSession implements OAdminSession {

  private OrientDBRemote context;
  private ORemoteClientSession session;

  public ORemoteAdminSession(OrientDBRemote context, ORemoteClientSession session) {
    this.context = context;
    this.session = session;
  }

  @Override
  public OResultSet execute(String statement, Map<String, Object> params) {

    int recordsPerPage =
        context
            .getContextConfiguration()
            .getValueAsInteger(OGlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE);
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    OServerQueryRequest request =
        new OServerQueryRequest(
            "sql",
            statement,
            params,
            OServerQueryRequest.COMMAND,
            ORecordSerializerNetworkV37Client.INSTANCE,
            recordsPerPage);

    OServerQueryResponse response =
        context.networkAdminOperation(
            request, session, "Error sending request:" + request.getDescription());
    return new ORemoteResultSet(
        null,
        response.getQueryId(),
        response.getResult(),
        response.getExecutionPlan(),
        response.getQueryStats(),
        response.isHasNextPage());
  }

  @Override
  public OResultSet execute(String statement, Object... params) {

    int recordsPerPage =
        context
            .getContextConfiguration()
            .getValueAsInteger(OGlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE);
    if (recordsPerPage <= 0) {
      recordsPerPage = 100;
    }
    OServerQueryRequest request =
        new OServerQueryRequest(
            "sql",
            statement,
            params,
            OServerQueryRequest.COMMAND,
            ORecordSerializerNetworkV37Client.INSTANCE,
            recordsPerPage);

    OServerQueryResponse response =
        context.networkAdminOperation(
            request, session, "Error sending request:" + request.getDescription());
    return new ORemoteResultSet(
        null,
        response.getQueryId(),
        response.getResult(),
        response.getExecutionPlan(),
        response.getQueryStats(),
        response.isHasNextPage());
  }

  @Override
  public void close() {
    session.closeAllSessions(context.connectionManager, context.getContextConfiguration());
  }
}
