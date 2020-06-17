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
package com.orientechnologies.orient.server.network.protocol.http.command.post;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandDocumentAbstract;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Executes a batch of operations in a single call. This is useful to reduce network latency issuing
 * multiple commands as multiple requests. Batch command supports transactions as well.<br>
 * <br>
 * Format: { "transaction" : &lt;true|false&gt;, "operations" : [ { "type" : "&lt;type&gt;" }* ] }
 * <br>
 * Where:
 *
 * <ul>
 *   <li><b>type</b> can be:
 *       <ul>
 *         <li>'c' for create
 *         <li>'u' for update
 *         <li>'d' for delete. The '@rid' field only is needed.
 *       </ul>
 * </ul>
 *
 * Example:<br>
 *
 * <p>
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
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OServerCommandPostBatch extends OServerCommandDocumentAbstract {
  private static final String[] NAMES = {"POST|batch/*"};

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    checkSyntax(iRequest.getUrl(), 2, "Syntax error: batch/<database>");

    iRequest.getData().commandInfo = "Execute multiple requests in one shot";

    ODatabaseDocument db = null;

    ODocument batch = null;

    Object lastResult = null;

    try {
      db = getProfiledDatabaseInstance(iRequest);

      if (db.getTransaction().isActive()) {
        // TEMPORARY PATCH TO UNDERSTAND WHY UNDER HIGH LOAD TX IS NOT COMMITTED AFTER BATCH. MAYBE
        // A PENDING TRANSACTION?
        OLogManager.instance()
            .warn(
                this,
                "Found database instance from the pool with a pending transaction. Forcing rollback before using it");
        db.rollback(true);
      }

      batch = new ODocument().fromJSON(iRequest.getContent());

      Boolean tx = batch.field("transaction");
      if (tx == null) tx = false;

      final Collection<Map<Object, Object>> operations;
      try {
        operations = batch.field("operations");
      } catch (Exception e) {
        throw new IllegalArgumentException(
            "Expected 'operations' field as a collection of objects", e);
      }

      if (operations == null || operations.isEmpty())
        throw new IllegalArgumentException("Input JSON has no operations to execute");

      boolean txBegun = false;
      if (tx && !db.getTransaction().isActive()) {
        db.begin();
        txBegun = true;
      }

      // BROWSE ALL THE OPERATIONS
      for (Map<Object, Object> operation : operations) {
        final String type = (String) operation.get("type");

        if (type.equals("c")) {
          // CREATE
          final ODocument doc = getRecord(operation);
          doc.save();
          lastResult = doc;
        } else if (type.equals("u")) {
          // UPDATE
          final ODocument doc = getRecord(operation);
          doc.save();
          lastResult = doc;
        } else if (type.equals("d")) {
          // DELETE
          final ODocument doc = getRecord(operation);
          db.delete(doc.getIdentity());
          lastResult = doc.getIdentity();
        } else if (type.equals("cmd")) {
          // COMMAND
          final String language = (String) operation.get("language");
          if (language == null) throw new IllegalArgumentException("language parameter is null");

          final Object command = operation.get("command");
          if (command == null) throw new IllegalArgumentException("command parameter is null");

          Object params = operation.get("parameters");
          if (params instanceof Collection) {
            params = ((Collection) params).toArray();
          }

          String commandAsString = null;
          if (command != null)
            if (OMultiValue.isMultiValue(command)) {
              for (Object c : OMultiValue.getMultiValueIterable(command)) {
                if (commandAsString == null) commandAsString = c.toString();
                else commandAsString += ";" + c.toString();
              }
            } else commandAsString = command.toString();

          OResultSet result;
          if (params == null) {
            result = db.execute(language, commandAsString);
          } else {
            result = db.execute(language, commandAsString, (Object[]) params);
          }
          lastResult = result.stream().map(x -> x.toElement()).collect(Collectors.toList());
          result.close();
        } else if (type.equals("script")) {
          // COMMAND
          final String language = (String) operation.get("language");
          if (language == null) throw new IllegalArgumentException("language parameter is null");

          final Object script = operation.get("script");
          if (script == null) throw new IllegalArgumentException("script parameter is null");

          StringBuilder text = new StringBuilder(1024);
          if (OMultiValue.isMultiValue(script)) {
            // ENSEMBLE ALL THE SCRIPT LINES IN JUST ONE SEPARATED BY LINEFEED
            int i = 0;
            for (Object o : OMultiValue.getMultiValueIterable(script)) {
              if (o != null) {
                if (i++ > 0) {
                  if (!text.toString().trim().endsWith(";")) {
                    text.append(";");
                  }
                  text.append("\n");
                }
                text.append(o.toString());
              }
            }
          } else text.append(script);

          Object params = operation.get("parameters");
          if (params instanceof Collection) {
            params = ((Collection) params).toArray();
          }

          OResultSet result;
          if (params == null) {
            result = db.execute(language, text.toString());
          } else {
            result = db.execute(language, text.toString(), (Object[]) params);
          }

          lastResult = result.stream().map(x -> x.toElement()).collect(Collectors.toList());
          result.close();
        }
      }

      if (txBegun) db.commit();

      try {
        iResponse.writeResult(lastResult);
      } catch (RuntimeException e) {
        OLogManager.instance()
            .error(
                this,
                "Error (%s) on serializing result of batch command:\n%s",
                e,
                batch.toJSON("prettyPrint"));
        throw e;
      }

    } finally {
      if (db != null) db.close();
    }
    return false;
  }

  public ODocument getRecord(Map<Object, Object> operation) {
    Object record = operation.get("record");

    ODocument doc;
    if (record instanceof Map<?, ?>)
      // CONVERT MAP IN DOCUMENT
      doc = new ODocument((Map<String, Object>) record);
    else doc = (ODocument) record;
    return doc;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
