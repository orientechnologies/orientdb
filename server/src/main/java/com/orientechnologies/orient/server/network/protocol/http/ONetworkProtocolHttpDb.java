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

import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.network.protocol.http.command.all.OServerCommandFunction;
import com.orientechnologies.orient.server.network.protocol.http.command.delete.OServerCommandDeleteClass;
import com.orientechnologies.orient.server.network.protocol.http.command.delete.OServerCommandDeleteDatabase;
import com.orientechnologies.orient.server.network.protocol.http.command.delete.OServerCommandDeleteDocument;
import com.orientechnologies.orient.server.network.protocol.http.command.delete.OServerCommandDeleteIndex;
import com.orientechnologies.orient.server.network.protocol.http.command.delete.OServerCommandDeleteProperty;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetClass;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetCluster;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetConnect;
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
import com.orientechnologies.orient.server.network.protocol.http.command.post.OServerCommandPostClass;
import com.orientechnologies.orient.server.network.protocol.http.command.post.OServerCommandPostCommand;
import com.orientechnologies.orient.server.network.protocol.http.command.post.OServerCommandPostDatabase;
import com.orientechnologies.orient.server.network.protocol.http.command.post.OServerCommandPostDocument;
import com.orientechnologies.orient.server.network.protocol.http.command.post.OServerCommandPostImportDatabase;
import com.orientechnologies.orient.server.network.protocol.http.command.post.OServerCommandPostImportRecords;
import com.orientechnologies.orient.server.network.protocol.http.command.post.OServerCommandPostProperty;
import com.orientechnologies.orient.server.network.protocol.http.command.post.OServerCommandPostStudio;
import com.orientechnologies.orient.server.network.protocol.http.command.post.OServerCommandPostUploadSingleFile;
import com.orientechnologies.orient.server.network.protocol.http.command.put.OServerCommandPutDocument;
import com.orientechnologies.orient.server.network.protocol.http.command.put.OServerCommandPutIndex;

public class ONetworkProtocolHttpDb extends ONetworkProtocolHttpAbstract {
  private static final String ORIENT_SERVER_DB = "OrientDB Server v." + OConstants.getVersion();

  @Override
  public void config(final OServer iServer, final Socket iSocket, final OContextConfiguration iConfiguration,
      final Object[] iCommands) throws IOException {
    server = iServer;
    setName("HTTP-DB");

    init(iCommands);

    cmdManager.registerCommand(new OServerCommandGetConnect());
    cmdManager.registerCommand(new OServerCommandGetDisconnect());
    cmdManager.registerCommand(new OServerCommandGetClass());
    cmdManager.registerCommand(new OServerCommandGetCluster());
    cmdManager.registerCommand(new OServerCommandGetDatabase());
    cmdManager.registerCommand(new OServerCommandGetDictionary());
    cmdManager.registerCommand(new OServerCommandGetDocument());
    cmdManager.registerCommand(new OServerCommandGetDocumentByClass());
    cmdManager.registerCommand(new OServerCommandGetQuery());
    cmdManager.registerCommand(new OServerCommandGetServer());
    cmdManager.registerCommand(new OServerCommandGetStorageAllocation());
    cmdManager.registerCommand(new OServerCommandGetFileDownload());
    cmdManager.registerCommand(new OServerCommandGetIndex());
    cmdManager.registerCommand(new OServerCommandGetListDatabases());
    cmdManager.registerCommand(new OServerCommandGetExportDatabase());
    cmdManager.registerCommand(new OServerCommandGetProfiler());
    cmdManager.registerCommand(new OServerCommandGetGephi());

    cmdManager.registerCommand(new OServerCommandPostClass());
    cmdManager.registerCommand(new OServerCommandPostCommand());
    cmdManager.registerCommand(new OServerCommandPostDatabase());
    cmdManager.registerCommand(new OServerCommandPostDocument());
    cmdManager.registerCommand(new OServerCommandPostProperty());
    cmdManager.registerCommand(new OServerCommandPostStudio());
    cmdManager.registerCommand(new OServerCommandPostUploadSingleFile());
    cmdManager.registerCommand(new OServerCommandPostDatabase());
    cmdManager.registerCommand(new OServerCommandPostImportRecords());
    cmdManager.registerCommand(new OServerCommandPostImportDatabase());

    cmdManager.registerCommand(new OServerCommandPutDocument());
    cmdManager.registerCommand(new OServerCommandPutIndex());

    cmdManager.registerCommand(new OServerCommandDeleteClass());
    cmdManager.registerCommand(new OServerCommandDeleteDatabase());
    cmdManager.registerCommand(new OServerCommandDeleteDocument());
    cmdManager.registerCommand(new OServerCommandDeleteProperty());
    cmdManager.registerCommand(new OServerCommandDeleteIndex());

    cmdManager.registerCommand(new OServerCommandOptions());

    cmdManager.registerCommand(new OServerCommandFunction());

    super.config(server, iSocket, iConfiguration, iCommands);
    connection.data.serverInfo = ORIENT_SERVER_DB;
  }

  public String getType() {
    return "http";
  }
}
