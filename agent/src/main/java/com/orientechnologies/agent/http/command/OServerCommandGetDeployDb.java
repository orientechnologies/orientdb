package com.orientechnologies.agent.http.command;

import com.orientechnologies.agent.EnterprisePermissions;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;

/**
 * Created by enricorisa on 18/06/14.
 */
public class OServerCommandGetDeployDb extends OServerCommandAuthenticatedServerAbstract {
  private static final String[] NAMES = { "GET|deployDb/*" };

  public OServerCommandGetDeployDb() {
    super(EnterprisePermissions.SERVER_DISTRIBUTED.toString());
  }

  @Override
  public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] parts = checkSyntax(iRequest.getUrl(), 2, "Syntax error: distributed/<cluster>/<db>");
    String db = parts[1];

    final ODistributedServerManager manager = OServerMain.server().getDistributedManager();
    manager.installDatabase(true, db, false, OGlobalConfiguration.DISTRIBUTED_BACKUP_TRY_INCREMENTAL_FIRST.getValueAsBoolean());

    iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, null, null);
    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
