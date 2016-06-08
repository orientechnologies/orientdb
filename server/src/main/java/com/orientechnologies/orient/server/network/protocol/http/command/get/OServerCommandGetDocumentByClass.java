/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.server.network.protocol.http.command.get;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

public class OServerCommandGetDocumentByClass extends OServerCommandAuthenticatedDbAbstract {
  private static final String[] NAMES = { "GET|documentbyclass/*", "HEAD|documentbyclass/*" };

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    ODatabaseDocument db = null;

    final String[] urlParts = checkSyntax(iRequest.url, 4,
        "Syntax error: documentbyclass/<database>/<class-name>/<record-position>[/fetchPlan]");

    final String fetchPlan = urlParts.length > 4 ? urlParts[4] : null;

    iRequest.data.commandInfo = "Load document";

    final ORecord rec;
    try {

      db = getProfiledDatabaseInstance(iRequest);
      if (((OMetadataInternal) db.getMetadata()).getImmutableSchemaSnapshot().getClass(urlParts[2]) == null) {
        throw new IllegalArgumentException("Invalid class '" + urlParts[2] + "'");
      }
      final String rid = db.getClusterIdByName(urlParts[2]) + ":" + urlParts[3];
      rec = db.load(new ORecordId(rid), fetchPlan);

      if (rec == null)
        iResponse.send(OHttpUtils.STATUS_NOTFOUND_CODE, "Not Found", OHttpUtils.CONTENT_JSON, "Record with id '" + rid
            + "' was not found.", null);
      else if (iRequest.httpMethod.equals("HEAD"))
        // JUST SEND HTTP CODE 200
        iResponse.send(OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, null, null, null);
      else
        // SEND THE DOCUMENT BACK
        iResponse.writeRecord(rec, fetchPlan, null);

    } finally {
      if (db != null)
        db.close();
    }

    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }

}
