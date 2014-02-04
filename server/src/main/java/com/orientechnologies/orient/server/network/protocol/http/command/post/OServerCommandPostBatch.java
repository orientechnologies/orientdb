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
package com.orientechnologies.orient.server.network.protocol.http.command.post;

import java.util.Collection;
import java.util.Map;

import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandDocumentAbstract;

/**
 * Executes a batch of operations in a single call. This is useful to reduce network latency issuing multiple commands as multiple
 * requests. Batch command supports transactions as well.<br>
 * <br>
 * Format: { "transaction" : &lt;true|false&gt;, "operations" : [ { "type" : "&lt;type&gt;" }* ] }<br>
 * Where:
 * <ul>
 * <li><b>type</b> can be:
 * <ul>
 * <li>'c' for create</li>
 * <li>'u' for update</li>
 * <li>'d' for delete. The '@rid' field only is needed.</li>
 * </ul>
 * </li>
 * </ul>
 * Example:<br>
 * 
 * <pre>
 * { "transaction" : true, 
 *   "operations" : [ 
 *        { "type" : "u",
 *          "record" : {
 *            "@rid" : "#14:122",
 *            "name" : "Luca",
 *            "vehicle" : "Car"
 *          } 
 *        }, { "type" : "d",
 *          "record" : {
 *            "@rid" : "#14:100"
 *          }
 *        }, { "type" : "c",
 *          "record" : {
 *            "@class" : "City",
 *            "name" : "Venice"
 *          }
 *        }
 *     ] 
 * }
 * </pre>
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OServerCommandPostBatch extends OServerCommandDocumentAbstract {
  private static final String[] NAMES = { "POST|batch/*" };

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    checkSyntax(iRequest.url, 2, "Syntax error: batch/<database>");

    iRequest.data.commandInfo = "Execute multiple requests in one shot";

    ODatabaseDocumentTx db = null;

    ODocument batch = null;
    int executed = 0;

    try {
      db = getProfiledDatabaseInstance(iRequest);

      batch = new ODocument().fromJSON(iRequest.content);

      Boolean tx = batch.field("transaction");
      if (tx == null)
        tx = false;

      final Collection<Map<Object, Object>> operations;
      try {
        operations = batch.field("operations");
      } catch (Exception e) {
        throw new IllegalArgumentException("Expected 'operations' field as a collection of objects");
      }

      if (operations == null || operations.isEmpty())
        throw new IllegalArgumentException("Input JSON has no operations to execute");

      if (tx)
        db.begin();

      // BROWSE ALL THE OPERATIONS
      for (Map<Object, Object> operation : operations) {
        final String type = (String) operation.get("type");

        if (type.equals("c")) {
          // CREATE
          final ODocument doc = getRecord(operation);
          doc.save();
          executed++;
        } else if (type.equals("u")) {
          // UPDATE
          final ODocument doc = getRecord(operation);
          doc.save();
          executed++;
        } else if (type.equals("d")) {
          // DELETE
          final ODocument doc = getRecord(operation);
          db.delete(doc.getIdentity());
          executed++;
        } else if (type.equals("cmd")) {
          // COMMAND
          final String language = (String) operation.get("language");
          final String command = (String) operation.get("command");

          final OCommandRequestText cmd = (OCommandRequestText) OCommandManager.instance().getRequester(language);
          cmd.setText(command);
          db.command(cmd).execute();
          executed++;
        } else if (type.equals("script")) {
          // COMMAND
          final String language = (String) operation.get("language");
          final String script = (String) operation.get("script");

          db.command(new OCommandScript(language, script)).execute();
          executed++;
        }
      }

      if (tx)
        db.commit();

    } finally {
      if (db != null)
        db.close();
    }

    iResponse
        .send(OHttpUtils.STATUS_OK_CODE, OHttpUtils.STATUS_OK_DESCRIPTION, OHttpUtils.CONTENT_TEXT_PLAIN, executed, null, true);
    return false;
  }

  public ODocument getRecord(Map<Object, Object> operation) {
    Object record = operation.get("record");

    ODocument doc;
    if (record instanceof Map<?, ?>)
      // CONVERT MAP IN DOCUMENT
      doc = new ODocument((Map<String, Object>) record);
    else
      doc = (ODocument) record;
    return doc;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
