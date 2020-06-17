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
package com.orientechnologies.orient.server.network.protocol.http.command.get;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;
import java.io.IOException;
import java.net.SocketException;
import java.util.Date;
import java.util.zip.GZIPOutputStream;

public class OServerCommandGetExportDatabase extends OServerCommandAuthenticatedDbAbstract
    implements OCommandOutputListener {
  private static final String[] NAMES = {"GET|export/*"};

  @Override
  public boolean execute(final OHttpRequest iRequest, final OHttpResponse iResponse)
      throws Exception {
    String[] urlParts =
        checkSyntax(iRequest.getUrl(), 2, "Syntax error: export/<database>/[<name>][?params*]");

    if (urlParts.length <= 2) {
      exportStandard(iRequest, iResponse);
    }
    return false;
  }

  protected void exportStandard(final OHttpRequest iRequest, final OHttpResponse iResponse)
      throws InterruptedException, IOException {
    iRequest.getData().commandInfo = "Database export";
    final ODatabaseDocumentInternal database = getProfiledDatabaseInstance(iRequest);
    try {
      iResponse.writeStatus(OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION);
      iResponse.writeHeaders(OHttpUtils.CONTENT_GZIP);
      iResponse.writeLine(
          "Content-Disposition: attachment; filename=" + database.getName() + ".gz");
      iResponse.writeLine("Date: " + new Date());
      iResponse.writeLine(null);
      final ODatabaseExport export =
          new ODatabaseExport(
              database, new GZIPOutputStream(iResponse.getOutputStream(), 16384), this);
      export.exportDatabase();

      try {
        iResponse.flush();
      } catch (SocketException e) {
      }
    } finally {
      if (database != null) database.close();
    }
  }

  @Override
  public void onMessage(String iText) {}

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
