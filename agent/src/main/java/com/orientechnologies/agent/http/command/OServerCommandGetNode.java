/*
 * Copyright 2015 OrientDB LTD (info(at)orientdb.com)
 *   
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *   
 *        http://www.apache.org/licenses/LICENSE-2.0
 *   
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *   
 *   For more information: http://www.orientdb.com
 */

package com.orientechnologies.agent.http.command;

import com.orientechnologies.orient.server.OServerInfo;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;

import java.io.IOException;

/**
 * Created by Enrico Risa on 20/11/15.
 */
public class OServerCommandGetNode extends OServerCommandDistributedScope {

  private static final String[] NAMES = { "GET|node/*", "POST|node/*" };

  public OServerCommandGetNode() {
    super("server.profiler");
  }

  @Override
  public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] parts = checkSyntax(iRequest.getUrl(), 2, "Syntax error: node/<command>/[<id>]");

    String command = parts[1];

    if (isLocalNode(iRequest)) {
      if ("GET".equalsIgnoreCase(iRequest.httpMethod)) {
        doGet(iRequest, iResponse, command);
      } else {
        doPost(iResponse, parts, command);
      }

    } else {
      proxyRequest(iRequest, iResponse);
    }
    return false;
  }

  private void doPost(OHttpResponse iResponse, String[] parts, String command) throws IOException {
    if (command.equalsIgnoreCase("connection") && parts.length == 4) {
      if ("KILL".equalsIgnoreCase(parts[2])) {
        server.getClientConnectionManager().kill(server, Integer.parseInt(parts[3]));
      } else if ("INTERRUPT".equalsIgnoreCase(parts[2])) {
        server.getClientConnectionManager().interrupt(Integer.parseInt(parts[3]));
      } else {
        throw new IllegalArgumentException("Connection command '" + parts[2] + "' is unknown. Supported are: kill, interrupt");
      }
      iResponse.send(OHttpUtils.STATUS_OK_NOCONTENT_CODE, OHttpUtils.STATUS_OK_NOCONTENT_DESCRIPTION,
          OHttpUtils.CONTENT_TEXT_PLAIN, null, null);
    } else {
      throw new IllegalArgumentException(" Command '" + command + "' is unknown.");
    }
  }

  private void doGet(OHttpRequest iRequest, OHttpResponse iResponse, String command) throws IOException {
    if (command.equalsIgnoreCase("info")) {
      iRequest.data.commandInfo = "Server status";
      final String result = OServerInfo.getServerInfo(server);
      iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, result, null);
    } else {
      throw new IllegalArgumentException(" Command '" + command + "' is unknown.");
    }
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
