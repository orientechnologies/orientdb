/*
 * Copyright 2016 OrientDB LTD (info(at)orientdb.com)
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

import com.orientechnologies.agent.backup.OBackupManager;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;

import java.io.IOException;

/**
 * Created by Enrico Risa on 22/03/16.
 */
public class OServerCommandBackupManager extends OServerCommandDistributedScope {

  OBackupManager                backupManager;
  private static final String[] NAMES = { "GET|backupManager", "GET|backupManager/*", "POST|backupManager","POST|backupManager/*", "PUT|backupManager/*" };

  public OServerCommandBackupManager() {
    super("server.backup");
    backupManager = new OBackupManager();
  }

  @Override
  public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    return super.execute(iRequest, iResponse);
  }

  @Override
  protected void doPost(OHttpRequest iRequest, OHttpResponse iResponse) throws IOException {

    final String[] parts = checkSyntax(iRequest.getUrl(), 1, "Syntax error: backupManager");
    if (parts.length == 1) {
      ODocument body = new ODocument().fromJSON(iRequest.content, "noMap");
      ODocument doc = backupManager.addBackup(body);
      iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, doc.toJSON(""), null);
    } else if (parts.length == 3) {

      String uuid = parts[1];
      String command = parts[2];
      if (command.equals("restore")) {
        ODocument body = new ODocument().fromJSON(iRequest.content, "noMap");
        backupManager.restoreBackup(uuid, body);
      } else {
        throw new IllegalArgumentException("cannot execute post request ");
      }
    } else {
      throw new IllegalArgumentException("cannot execute post request ");
    }

  }

  @Override
  protected void doPut(OHttpRequest iRequest, OHttpResponse iResponse) throws IOException {
    final String[] parts = checkSyntax(iRequest.getUrl(), 2, "Syntax error: backupManager");
    ODocument body = new ODocument().fromJSON(iRequest.content, "noMap");
    backupManager.changeBackup(parts[1], body);
    iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, body.toJSON(""), null);
  }

  @Override
  protected void doGet(OHttpRequest iRequest, OHttpResponse iResponse) throws IOException {
    final String[] parts = checkSyntax(iRequest.getUrl(), 1, "Syntax error: backupManager");

    if (parts.length == 1) {
      ODocument doc = backupManager.getConfiguration();
      iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, doc.toJSON(""), null);
    } else if (parts.length == 2) {

    } else if (parts.length == 3) {
      String uuid = parts[1];
      String command = parts[2];

      if (command.equalsIgnoreCase("status")) {
        ODocument status = backupManager.logs(uuid, 0, 1);
        iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, status.toJSON(""), null);
      } else if (command.equalsIgnoreCase("log")) {
        String pagePize = iRequest.getParameter("pageSize");
        String page = iRequest.getParameter("page");
        int pSize = pagePize != null ? Integer.valueOf(pagePize) : 5;
        int p = page != null ? Integer.valueOf(page) : 0;
        ODocument history = backupManager.logs(uuid, p, pSize);
        iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, history.toJSON(""), null);
      } else {
        throw new IllegalArgumentException("cannot find executor for command:" + command);
      }

    }

  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
