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
package com.orientechnologies.agent.http.command;

import java.net.SocketException;
import java.util.Date;
import java.util.List;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedServerAbstract;

public class OServerCommandPostBackupDatabase extends OServerCommandAuthenticatedServerAbstract implements OCommandOutputListener {
  public OServerCommandPostBackupDatabase() {
    super("database.backup");
  }

  private static final String[] NAMES = { "GET|backup/*", "POST|backup/*" };

  @Override
  public boolean execute(final OHttpRequest iRequest, final OHttpResponse iResponse) throws Exception {
    final String[] urlParts = checkSyntax(iRequest.url, 2, "Syntax error: backup/<database>");

    iRequest.data.commandInfo = "Database backup";
    try {
      iRequest.databaseName = urlParts[1];

      final ODatabaseDocumentTx database = getProfiledDatabaseInstance(iRequest);

      try {
        iResponse.writeStatus(OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION);
        iResponse.writeHeaders(OHttpUtils.CONTENT_GZIP);
        iResponse.writeLine("Content-Disposition: attachment; filename=" + database.getName() + ".gz");
        iResponse.writeLine("Date: " + new Date());
        iResponse.writeLine(null);

        // TODO
        database.backup(iResponse.getOutputStream(), null, null, null, 0, 0);

        try {
          iResponse.flush();
        } catch (SocketException e) {
        }
      } finally {
        if (database != null)
          database.close();
      }
    } catch (Exception e) {
      iResponse.sendStream(404, "File not found", null, null, 0);
    }
    return false;
  }

  protected ODatabaseDocumentTx getProfiledDatabaseInstance(final OHttpRequest iRequest) throws InterruptedException {
    // after authentication, if current login user is different compare with current DB user, reset DB user to login user
    ODatabaseDocumentInternal localDatabase = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();

    if (localDatabase == null) {
      final List<String> parts = OStringSerializerHelper.split(iRequest.authorization, ':');
      localDatabase = (ODatabaseDocumentTx) server.openDatabase(iRequest.databaseName, parts.get(0), parts.get(1));
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
    return (ODatabaseDocumentTx) ((ODatabaseDocumentInternal) localDatabase).getDatabaseOwner();
  }

  @Override
  public void onMessage(String iText) {
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
