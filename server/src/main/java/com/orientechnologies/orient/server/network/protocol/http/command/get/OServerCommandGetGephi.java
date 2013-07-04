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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;

public class OServerCommandGetGephi extends OServerCommandAuthenticatedDbAbstract {
  private static final String[] NAMES = { "GET|gephi/*" };

  @Override
  @SuppressWarnings("unchecked")
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    String[] urlParts = checkSyntax(
        iRequest.url,
        4,
        "Syntax error: gephi/<database>/sql/<query-text>[/<limit>][/<fetchPlan>].<br/>Limit is optional and is setted to 20 by default. Set expressely to 0 to have no limits.");

    final int limit = urlParts.length > 4 ? Integer.parseInt(urlParts[4]) : 20;

    final String fetchPlan = urlParts.length > 5 ? urlParts[5] : null;

    final String text = urlParts[3];

    iRequest.data.commandInfo = "Gephi";
    iRequest.data.commandDetail = text;

    ODatabaseDocumentTx db = null;

    try {
      db = getProfiledDatabaseInstance(iRequest);

      final List<OIdentifiable> response = (List<OIdentifiable>) db.command(
          new OSQLSynchQuery<ORecordSchemaAware<?>>(text, limit).setFetchPlan(fetchPlan)).execute();

      sendRecordsContent(iRequest, iResponse, response, fetchPlan);

    } finally {
      if (db != null)
        db.close();
    }

    return false;
  }

  protected void sendRecordsContent(final OHttpRequest iRequest, final OHttpResponse iResponse, List<OIdentifiable> iRecords,
      String iFetchPlan) throws IOException {
    final StringWriter buffer = new StringWriter();
    final OJSONWriter json = new OJSONWriter(buffer, OHttpResponse.JSON_FORMAT);
    json.setPrettyPrint(true);

    if (iRecords.size() > 0) {
      final ORecord<?> firstRecord = iRecords.get(0).getRecord();
      final OClass vertexBaseClass = firstRecord.getDatabase().getMetadata().getSchema().getClass(OGraphDatabase.VERTEX_ALIAS);
      if (firstRecord instanceof ODocument && ((ODocument) firstRecord).getSchemaClass().isSubClassOf(vertexBaseClass))
        // GRAPHDB MODEL
        generateGraphDbOutput(iRecords, json);
      else
        // DOCUMENT MODEL
        generateDefaultOutput(iRecords, json);
    }

    iResponse.send(OHttpUtils.STATUS_OK_CODE, "OK", OHttpUtils.CONTENT_JSON, buffer.toString(), null);
  }

  protected void generateDefaultOutput(List<OIdentifiable> iRecords, final OJSONWriter json) throws IOException {
    // LINKS IN THE FORMAT: [ source rid, field, target rid ]
    final List<String[]> links = new ArrayList<String[]>();

    for (OIdentifiable rec : iRecords) {
      if (rec.getRecord() instanceof ODocument) {
        final ODocument doc = rec.getRecord();

        json.resetAttributes();
        json.beginObject();
        json.beginObject(1, false, "an");
        json.beginObject(2, false, rec.getIdentity());

        for (String field : doc.fieldNames()) {
          final Object v = doc.field(field);
          if (v != null) {
            if (v instanceof OIdentifiable)
              links.add(new String[] { rec.getIdentity().toString(), field, ((OIdentifiable) v).getIdentity().toString() });
            else if (v instanceof Collection<?>) {
              for (Object o : ((Collection<?>) v))
                if (o instanceof OIdentifiable)
                  links.add(new String[] { rec.getIdentity().toString(), field, ((OIdentifiable) o).getIdentity().toString() });
            } else
              json.writeAttribute(3, false, field, v);
          }
        }

        json.endObject(2, false);
        json.endObject(1, false);
        json.endObject(0, false);
        json.newline();
      }
    }

    for (String[] rec : links) {
      json.resetAttributes();
      json.beginObject();
      json.beginObject(1, false, "ae");
      json.beginObject(2, false, rec[0] + "." + rec[1] + "." + rec[2]);

      json.writeAttribute(3, false, "source", rec[0]);
      json.writeAttribute(3, false, "label", rec[1]);
      json.writeAttribute(3, false, "target", rec[2]);
      json.writeAttribute(3, false, "directed", true);

      json.endObject(2, false);
      json.endObject(1, false);
      json.endObject(0, false);
      json.newline();
    }
  }

  @SuppressWarnings("unchecked")
  protected void generateGraphDbOutput(final List<OIdentifiable> iRecords, final OJSONWriter json) throws IOException {
    // CREATE A SET TO SPEED UP SEARCHES ON VERTICES
    final Set<ORID> vertexes = new HashSet<ORID>();
    for (OIdentifiable id : iRecords)
      vertexes.add(id.getIdentity());

    final Set<OIdentifiable> edges = new LinkedHashSet<OIdentifiable>();
    for (OIdentifiable rec : iRecords) {
      if (rec.getRecord() instanceof ODocument) {
        final ODocument doc = rec.getRecord();

        json.resetAttributes();
        json.beginObject(0, false, null);
        json.beginObject(1, false, "an");
        json.beginObject(2, false, doc.getIdentity());
        for (String field : doc.fieldNames()) {
          final Object v = doc.field(field);
          if (v != null) {
            if (OGraphDatabase.VERTEX_FIELD_OUT.equals(field)) {
              final Collection<ODocument> edgeSet = (Collection<ODocument>) v;
              for (ODocument e : edgeSet) {
                if (vertexes.contains(e.field("in")))
                  // VERTEX IS PART OF RESULT SET: ADD THE EDGE
                  edges.add(e);
              }
            } else if (OGraphDatabase.VERTEX_FIELD_IN.equals(field)) {
              final Collection<ODocument> edgeSet = (Collection<ODocument>) v;
              for (ODocument e : edgeSet) {
                if (vertexes.contains(e.field("out")))
                  // VERTEX IS PART OF RESULT SET: ADD THE EDGE
                  edges.add(e);
              }
            } else
              json.writeAttribute(3, false, field, v);
          }
        }
        json.endObject(2, false);
        json.endObject(1, false);
        json.endObject(0, false);
        json.newline();
      }
    }

    for (OIdentifiable rec : edges) {
      final ODocument doc = rec.getRecord();

      json.resetAttributes();
      json.beginObject();
      json.beginObject(1, false, "ae");
      json.beginObject(2, false, doc.getIdentity());
      json.writeAttribute(3, false, "directed", false);

      for (String field : doc.fieldNames()) {
        final Object v = doc.field(field);
        if (v != null) {
          if (OGraphDatabase.EDGE_FIELD_IN.equals(field))
            json.writeAttribute(3, false, "source", v);
          else if (OGraphDatabase.EDGE_FIELD_OUT.equals(field))
            json.writeAttribute(3, false, "target", v);
          else
            json.writeAttribute(3, false, field, v);
        }
      }
      json.endObject(2, false);
      json.endObject(1, false);
      json.endObject(0, false);
      json.newline();
    }
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
