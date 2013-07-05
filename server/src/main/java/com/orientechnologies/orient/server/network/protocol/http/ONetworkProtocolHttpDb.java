/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.network.protocol.http;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommand;
import com.orientechnologies.orient.server.network.protocol.http.command.all.OServerCommandAction;
import com.orientechnologies.orient.server.network.protocol.http.command.all.OServerCommandFunction;
import com.orientechnologies.orient.server.network.protocol.http.command.delete.OServerCommandDeleteClass;
import com.orientechnologies.orient.server.network.protocol.http.command.delete.OServerCommandDeleteDatabase;
import com.orientechnologies.orient.server.network.protocol.http.command.delete.OServerCommandDeleteDocument;
import com.orientechnologies.orient.server.network.protocol.http.command.delete.OServerCommandDeleteIndex;
import com.orientechnologies.orient.server.network.protocol.http.command.delete.OServerCommandDeleteProperty;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetClass;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetCluster;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetConnect;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetConnections;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetDatabase;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetDictionary;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetDisconnect;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetDocument;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetDocumentByClass;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetExportDatabase;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetFileDownload;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetGephi;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetIndex;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetListDatabases;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetProfiler;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetQuery;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetServer;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetStorageAllocation;
import com.orientechnologies.orient.server.network.protocol.http.command.options.OServerCommandOptions;
import com.orientechnologies.orient.server.network.protocol.http.command.post.OServerCommandPostBatch;
import com.orientechnologies.orient.server.network.protocol.http.command.post.OServerCommandPostClass;
import com.orientechnologies.orient.server.network.protocol.http.command.post.OServerCommandPostCommand;
import com.orientechnologies.orient.server.network.protocol.http.command.post.OServerCommandPostDatabase;
import com.orientechnologies.orient.server.network.protocol.http.command.post.OServerCommandPostDocument;
import com.orientechnologies.orient.server.network.protocol.http.command.post.OServerCommandPostImportDatabase;
import com.orientechnologies.orient.server.network.protocol.http.command.post.OServerCommandPostImportRecords;
import com.orientechnologies.orient.server.network.protocol.http.command.post.OServerCommandPostProperty;
import com.orientechnologies.orient.server.network.protocol.http.command.post.OServerCommandPostStudio;
import com.orientechnologies.orient.server.network.protocol.http.command.post.OServerCommandPostUploadSingleFile;
import com.orientechnologies.orient.server.network.protocol.http.command.put.OServerCommandPostConnection;
import com.orientechnologies.orient.server.network.protocol.http.command.put.OServerCommandPutDocument;
import com.orientechnologies.orient.server.network.protocol.http.command.put.OServerCommandPutIndex;

public class ONetworkProtocolHttpDb extends ONetworkProtocolHttpAbstract {
  private static final String ORIENT_SERVER_DB = "OrientDB Server v." + OConstants.getVersion();

  @Override
  public void config(final OServer iServer, final Socket iSocket, final OContextConfiguration iConfiguration,
      final List<?> iStatelessCommands, List<?> iStatefulCommands) throws IOException {
    server = iServer;
    setName("HTTP-DB");

    if (sharedCmdManager == null)
      // FIRST TIME REGISTERS THE STATELESS COMMANDS
      registerStatelessCommands(iStatelessCommands);

    cmdManager = new OHttpNetworkCommandManager(server, sharedCmdManager);
    for (Object cmdConfig : iStatefulCommands)
      cmdManager.registerCommand(OServerNetworkListener.createCommand(server, (OServerCommandConfiguration) cmdConfig));

    cmdManager.registerCommand(new OServerCommandPostImportDatabase());
    cmdManager.registerCommand(new OServerCommandPostUploadSingleFile());

    super.config(server, iSocket, iConfiguration, iStatelessCommands, iStatefulCommands);
    connection.data.serverInfo = ORIENT_SERVER_DB;
  }

  @Override
  protected void afterExecution() throws InterruptedException {
    ODatabaseRecordThreadLocal.INSTANCE.remove();
  }

  public String getType() {
    return "http";
  }

  protected void registerStatelessCommands(final List<?> iStatelessCommands) {
    sharedCmdManager = new OHttpNetworkCommandManager(server, null);

    sharedCmdManager.registerCommand(new OServerCommandGetConnect());
    sharedCmdManager.registerCommand(new OServerCommandGetDisconnect());
    sharedCmdManager.registerCommand(new OServerCommandGetClass());
    sharedCmdManager.registerCommand(new OServerCommandGetCluster());
    sharedCmdManager.registerCommand(new OServerCommandGetDatabase());
    sharedCmdManager.registerCommand(new OServerCommandGetDictionary());
    sharedCmdManager.registerCommand(new OServerCommandGetDocument());
    sharedCmdManager.registerCommand(new OServerCommandGetDocumentByClass());
    sharedCmdManager.registerCommand(new OServerCommandGetQuery());
    sharedCmdManager.registerCommand(new OServerCommandGetServer());
    sharedCmdManager.registerCommand(new OServerCommandGetConnections());
    sharedCmdManager.registerCommand(new OServerCommandGetStorageAllocation());
    sharedCmdManager.registerCommand(new OServerCommandGetFileDownload());
    sharedCmdManager.registerCommand(new OServerCommandGetIndex());
    sharedCmdManager.registerCommand(new OServerCommandGetListDatabases());
    sharedCmdManager.registerCommand(new OServerCommandGetExportDatabase());
    sharedCmdManager.registerCommand(new OServerCommandGetProfiler());
    sharedCmdManager.registerCommand(new OServerCommandGetGephi());
    sharedCmdManager.registerCommand(new OServerCommandPostBatch());
    sharedCmdManager.registerCommand(new OServerCommandPostClass());
    sharedCmdManager.registerCommand(new OServerCommandPostCommand());
    sharedCmdManager.registerCommand(new OServerCommandPostDatabase());
    sharedCmdManager.registerCommand(new OServerCommandPostDocument());
    sharedCmdManager.registerCommand(new OServerCommandPostImportRecords());
    sharedCmdManager.registerCommand(new OServerCommandPostProperty());
    sharedCmdManager.registerCommand(new OServerCommandPostConnection());
    sharedCmdManager.registerCommand(new OServerCommandPostStudio());
    sharedCmdManager.registerCommand(new OServerCommandPutDocument());
    sharedCmdManager.registerCommand(new OServerCommandPutIndex());
    sharedCmdManager.registerCommand(new OServerCommandDeleteClass());
    sharedCmdManager.registerCommand(new OServerCommandDeleteDatabase());
    sharedCmdManager.registerCommand(new OServerCommandDeleteDocument());
    sharedCmdManager.registerCommand(new OServerCommandDeleteProperty());
    sharedCmdManager.registerCommand(new OServerCommandDeleteIndex());
    sharedCmdManager.registerCommand(new OServerCommandOptions());
    sharedCmdManager.registerCommand(new OServerCommandFunction());
    sharedCmdManager.registerCommand(new OServerCommandAction());

    for (Object cmd : iStatelessCommands)
      sharedCmdManager.registerCommand((OServerCommand) cmd);
  }

}
