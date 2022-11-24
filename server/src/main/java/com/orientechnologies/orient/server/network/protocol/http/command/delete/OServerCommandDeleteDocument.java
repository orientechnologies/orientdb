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
package com.orientechnologies.orient.server.network.protocol.http.command.delete;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandDocumentAbstract;

public class OServerCommandDeleteDocument extends OServerCommandDocumentAbstract {
  private static final String[] NAMES = {"DELETE|document/*"};

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    ODatabaseDocument db = null;

    try {
      final String[] urlParts =
          checkSyntax(iRequest.getUrl(), 3, "Syntax error: document/<database>/<record-id>");

      iRequest.getData().commandInfo = "Delete document";

      db = getProfiledDatabaseInstance(iRequest);

      // PARSE PARAMETERS
      final int parametersPos = urlParts[2].indexOf('?');
      final String rid = parametersPos > -1 ? urlParts[2].substring(0, parametersPos) : urlParts[2];
      final ORecordId recordId = new ORecordId(rid);

      if (!recordId.isValid())
        throw new IllegalArgumentException("Invalid Record ID in request: " + urlParts[2]);

      final ODocument doc = new ODocument(recordId);

      // UNMARSHALL DOCUMENT WITH REQUEST CONTENT
      if (iRequest.getContent() != null)
        // GET THE VERSION FROM THE DOCUMENT
        doc.fromJSON(iRequest.getContent());
      else {
        if (iRequest.getIfMatch() != null)
          // USE THE IF-MATCH HTTP HEADER AS VERSION
          ORecordInternal.setVersion(doc, Integer.parseInt(iRequest.getIfMatch()));
        else
          // IGNORE THE VERSION
          ORecordInternal.setVersion(doc, -1);
      }

      final OClass cls = ODocumentInternal.getImmutableSchemaClass(doc);
      if (cls != null) {
        if (cls.isSubClassOf("V"))
          // DELETE IT AS VERTEX
          db.command("DELETE VERTEX ?", recordId).close();
        else if (cls.isSubClassOf("E"))
          // DELETE IT AS EDGE
          db.command("DELETE EDGE ?", recordId).close();
        else {
          doc.reload(null, true);
          doc.delete();
        }
      } else {
        doc.reload(null, true);
        doc.delete();
      }

      iResponse.send(
          OHttpUtils.STATUS_OK_NOCONTENT_CODE,
          OHttpUtils.STATUS_OK_NOCONTENT_DESCRIPTION,
          OHttpUtils.CONTENT_TEXT_PLAIN,
          null,
          null);

    } finally {
      if (db != null) db.close();
    }
    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
