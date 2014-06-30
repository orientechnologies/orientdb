package com.orientechnologies.agent.http.command;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.task.ODeployDatabaseTask;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;

import java.util.Collection;
import java.util.Map;

/**
 * Created by enricorisa on 18/06/14.
 */
public class OServerCommandGetDeployDb extends OServerCommandAuthenticatedServerAbstract {
  private static final String[] NAMES = { "GET|deployDb/*" };

  public OServerCommandGetDeployDb() {
    super("server.profiler");
  }

  @Override
  public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] parts = checkSyntax(iRequest.url, 2, "Syntax error: distributed/<cluster>/<db>");
    String db = parts[1];

    ODistributedServerManager manager = OServerMain.server().getDistributedManager();
    Map<String, Object> config = manager.getConfigurationMap();
    ODocument dbConf = (ODocument) config.get("database." + db);
    if (manager instanceof OHazelcastPlugin) {
      ((OHazelcastPlugin) manager).installDatabase(true, db, dbConf);
    }
    // ODistributedConfiguration cfg = manager.getDatabaseConfiguration(db);
    // final Collection<String> nodes = cfg.getServers();
    // nodes.remove(manager.getLocalNodeName());
    // ODistributedServerLog.warn(this, manager.getLocalNodeName(), nodes.toString(), ODistributedServerLog.DIRECTION.OUT,
    // "requesting deploy of database '%s' on local server...", db);
    //
    //
    // ODistributedMessageService messageService = manager.getMessageService();
    // messageService.registerDatabase(db);
    // final Map<String, Object> results = (Map<String, Object>) manager.sendRequest(db, null, nodes, new ODeployDatabaseTask(),
    // ODistributedRequest.EXECUTION_MODE.RESPONSE);
    // ODistributedServerLog.warn(this, manager.getLocalNodeName(), nodes.toString(), ODistributedServerLog.DIRECTION.OUT,
    // "deploy returned: %s", results);
    iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, null, null);
    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
