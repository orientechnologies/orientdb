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
package com.orientechnologies.orient.server.network.protocol.http.command.get;

import java.io.IOException;
import java.io.StringWriter;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.server.db.OSharedDocumentDatabase;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

public class OServerCommandGetClass extends OServerCommandAuthenticatedDbAbstract {
  private static final String[] NAMES = { "GET|class/*" };

  @Override
  public boolean execute(final OHttpRequest iRequest) throws Exception {
    String[] urlParts = checkSyntax(iRequest.url, 3, "Syntax error: class/<database>/<class-name>");

    iRequest.data.commandInfo = "Returns the information of a class in the schema";
    iRequest.data.commandDetail = urlParts[2];

    ODatabaseDocumentTx db = null;

    try {
      db = getProfiledDatabaseInstance(iRequest);

      if (db.getMetadata().getSchema().getClass(urlParts[2]) == null)
        throw new IllegalArgumentException("Invalid class '" + urlParts[2] + "'");

      final StringWriter buffer = new StringWriter();
      final OJSONWriter json = new OJSONWriter(buffer, JSON_FORMAT);
      json.beginObject();
      exportClassSchema(db, json, db.getMetadata().getSchema().getClass(urlParts[2]));
      json.endObject();
      sendTextContent(iRequest, OHttpUtils.STATUS_OK_CODE, "OK", null, OHttpUtils.CONTENT_JSON, buffer.toString());
    } finally {
      if (db != null)
        OSharedDocumentDatabase.release(db);
    }
    return false;
  }

  public void exportClassSchema(final ODatabaseRecord db, final OJSONWriter json, final OClass cls) throws IOException {
    if (cls == null)
      return;

    json.write(" \"class\": ");
    json.beginObject(1, false, null);
    json.writeAttribute(2, true, "name", cls.getName());

    if (cls.properties() != null && cls.properties().size() > 0) {
      json.beginObject(2, true, "properties");
      for (OProperty prop : cls.properties()) {
        json.beginObject(3, true, prop.getName());
        json.writeAttribute(4, true, "name", prop.getName());
        if (prop.getLinkedClass() != null)
          json.writeAttribute(4, true, "linkedClass", prop.getLinkedClass().getName());
        if (prop.getLinkedType() != null)
          json.writeAttribute(4, true, "linkedType", prop.getLinkedType().toString());
        json.writeAttribute(4, true, "type", prop.getType().toString());
        json.writeAttribute(4, true, "mandatory", prop.isMandatory());
        json.writeAttribute(4, true, "notNull", prop.isNotNull());
        json.writeAttribute(4, true, "min", prop.getMin());
        json.writeAttribute(4, true, "max", prop.getMax());
        json.endObject(3, true);
      }
      json.endObject(2, true);
    }
    json.endObject(1, true);
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
