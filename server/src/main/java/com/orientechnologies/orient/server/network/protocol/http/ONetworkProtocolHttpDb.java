/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.server.network.protocol.http;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.server.OClientConnection;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.command.post.OServerCommandPostImportDatabase;
import com.orientechnologies.orient.server.network.protocol.http.command.post.OServerCommandPostUploadSingleFile;
import java.io.IOException;
import java.net.Socket;

public class ONetworkProtocolHttpDb extends ONetworkProtocolHttpAbstract {
  private static final int CURRENT_PROTOCOL_VERSION = 10;

  public ONetworkProtocolHttpDb(OServer server) {
    super(server);
  }

  @Override
  public void config(
      final OServerNetworkListener iListener,
      final OServer iServer,
      final Socket iSocket,
      final OContextConfiguration iConfiguration)
      throws IOException {
    server = iServer;
    setName(
        "OrientDB HTTP Connection "
            + iSocket.getLocalAddress()
            + ":"
            + iSocket.getLocalPort()
            + "<-"
            + iSocket.getRemoteSocketAddress());

    super.config(iListener, server, iSocket, iConfiguration);
    cmdManager.registerCommand(new OServerCommandPostImportDatabase());
    cmdManager.registerCommand(new OServerCommandPostUploadSingleFile());

    connection.getData().serverInfo =
        iConfiguration.getValueAsString(OGlobalConfiguration.NETWORK_HTTP_SERVER_INFO);
  }

  @Override
  public int getVersion() {
    return CURRENT_PROTOCOL_VERSION;
  }

  public String getType() {
    return "http";
  }

  @Override
  protected void afterExecution() throws InterruptedException {
    ODatabaseRecordThreadLocal.instance().remove();
  }

  @Override
  public OBinaryRequestExecutor executor(OClientConnection connection) {
    return null;
  }
}
