package com.orientechnologies.agent.http.command;

import com.orientechnologies.agent.proxy.HttpProxy;
import com.orientechnologies.agent.proxy.HttpProxyListener;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;

import java.io.IOException;

/**
 * Created by Enrico Risa on 16/11/15.
 */
public abstract class OServerCommandDistributedScope extends OServerCommandAuthenticatedServerAbstract {

  HttpProxy proxy = new HttpProxy();

  protected OServerCommandDistributedScope(String iRequiredResource) {
    super(iRequiredResource);

  }

  boolean isLocalNode(OHttpRequest iRequest) {

    ODistributedServerManager distributedManager = server.getDistributedManager();
    String node = iRequest.getParameter("node");

    if (node == null || distributedManager == null) {
      return true;
    }
    return distributedManager.getLocalNodeName().equalsIgnoreCase(node);
  }

  public void proxyRequest(OHttpRequest iRequest, OHttpResponse iResponse) throws IOException {
    proxyRequest(iRequest, iResponse, null);
  }

  public void proxyRequest(OHttpRequest iRequest, OHttpResponse iResponse, HttpProxyListener listener) throws IOException {

    ODistributedServerManager manager = server.getDistributedManager();
    String node = iRequest.getParameter("node");
    if ("_all".equalsIgnoreCase(node)) {
      proxy.broadcastRequest(manager, iRequest, iResponse);
    } else {
      proxy.proxyRequest(manager, node, iRequest, iResponse, listener);
    }
  }

}
