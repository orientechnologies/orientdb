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
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandDocumentAbstract;

import java.util.Collection;

public class OServerCommandGetIndex extends OServerCommandDocumentAbstract {
  private static final String[] NAMES = { "GET|index/*" };

  @SuppressWarnings("unchecked")
  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] urlParts = checkSyntax(iRequest.url, 3, "Syntax error: index/<database>/<index-name>/<key>");

    iRequest.data.commandInfo = "Index get";

    ODatabaseDocument db = null;

    try {
      db = getProfiledDatabaseInstance(iRequest);

      final OIndex<?> index = db.getMetadata().getIndexManager().getIndex(urlParts[2]);
      if (index == null)
        throw new IllegalArgumentException("Index name '" + urlParts[2] + "' not found");

      final Object content = index.get(urlParts[3]);

      if (content == null)
        iResponse.send(OHttpUtils.STATUS_NOTFOUND_CODE, OHttpUtils.STATUS_NOTFOUND_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN, null,
            null);
      else {
        final StringBuilder buffer = new StringBuilder(128);
        buffer.append('[');

        if (content instanceof Collection<?>) {
          Collection<OIdentifiable> collection = (Collection<OIdentifiable>) content;
          int count = 0;
          for (OIdentifiable item : collection) {
            if (count > 0) {
              buffer.append(", ");
            }
            buffer.append(item.getRecord().toJSON());
            count++;
          }
        } else
          buffer.append(((OIdentifiable) content).getRecord().toJSON());

        buffer.append(']');

        if (isJsonResponse(iResponse)) {
          iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, buffer.toString(), null);
        } else {
          iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_TEXT_PLAIN, buffer.toString(), null);
        }

      }
    } finally {
      if (db != null)
        db.close();
    } return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
