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
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * SQL MOVE VERTEX command.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OCommandExecutorSQLMoveVertex extends OCommandExecutorSQLSetAware
    implements OCommandDistributedReplicateRequest {
  public static final String NAME = "MOVE VERTEX";
  private static final String KEYWORD_MERGE = "MERGE";
  private static final String KEYWORD_BATCH = "BATCH";
  private String source = null;
  private String clusterName;
  private String className;
  private OClass clazz;
  private List<OPair<String, Object>> fields;
  private ODocument merge;
  private int batch = 100;

  @SuppressWarnings("unchecked")
  public OCommandExecutorSQLMoveVertex parse(final OCommandRequest iRequest) {
    final ODatabaseDocument database = getDatabase();

    init((OCommandRequestText) iRequest);

    parserRequiredKeyword("MOVE");
    parserRequiredKeyword("VERTEX");

    source = parserRequiredWord(false, "Syntax error", " =><,\r\n");
    if (source == null) throw new OCommandSQLParsingException("Cannot find source");

    parserRequiredKeyword("TO");

    String temp = parseOptionalWord(true);

    while (temp != null) {
      if (temp.startsWith("CLUSTER:")) {
        if (className != null)
          throw new OCommandSQLParsingException(
              "Cannot define multiple sources. Found both cluster and class.");

        clusterName = temp.substring("CLUSTER:".length());
        if (database.getClusterIdByName(clusterName) == -1)
          throw new OCommandSQLParsingException("Cluster '" + clusterName + "' was not found");

      } else if (temp.startsWith("CLASS:")) {
        if (clusterName != null)
          throw new OCommandSQLParsingException(
              "Cannot define multiple sources. Found both cluster and class.");

        className = temp.substring("CLASS:".length());

        clazz = database.getMetadata().getSchema().getClass(className);

        if (clazz == null)
          throw new OCommandSQLParsingException("Class '" + className + "' was not found");

      } else if (temp.equals(KEYWORD_SET)) {
        fields = new ArrayList<OPair<String, Object>>();
        parseSetFields(clazz, fields);

      } else if (temp.equals(KEYWORD_MERGE)) {
        merge = parseJSON();

      } else if (temp.equals(KEYWORD_BATCH)) {
        temp = parserNextWord(true);
        if (temp != null) batch = Integer.parseInt(temp);
      }

      temp = parserOptionalWord(true);
      if (parserIsEnded()) break;
    }

    return this;
  }

  /** Executes the command and return the ODocument object created. */
  public Object execute(final Map<Object, Object> iArgs) {

    ODatabaseDocumentInternal db = getDatabase();

    db.begin();

    if (className == null && clusterName == null)
      throw new OCommandExecutionException(
          "Cannot execute the command because it has not been parsed yet");

    OModifiableBoolean shutdownGraph = new OModifiableBoolean();
    final boolean txAlreadyBegun = getDatabase().getTransaction().isActive();

    try {
      final Set<OIdentifiable> sourceRIDs =
          OSQLEngine.getInstance().parseRIDTarget(db, source, context, iArgs);

      // CREATE EDGES
      final List<ODocument> result = new ArrayList<ODocument>(sourceRIDs.size());

      for (OIdentifiable from : sourceRIDs) {
        final OVertex fromVertex = toVertex(from);
        if (fromVertex == null) continue;

        final ORID oldVertex = fromVertex.getIdentity().copy();
        final ORID newVertex = fromVertex.moveTo(className, clusterName);

        final ODocument newVertexDoc = newVertex.getRecord();

        if (fields != null) {
          // EVALUATE FIELDS
          for (final OPair<String, Object> f : fields) {
            if (f.getValue() instanceof OSQLFunctionRuntime)
              f.setValue(
                  ((OSQLFunctionRuntime) f.getValue())
                      .getValue(newVertex.getRecord(), null, context));
          }

          OSQLHelper.bindParameters(newVertexDoc, fields, new OCommandParameters(iArgs), context);
        }

        if (merge != null) newVertexDoc.merge(merge, true, false);

        // SAVE CHANGES
        newVertexDoc.save();

        // PUT THE MOVE INTO THE RESULT
        result.add(
            new ODocument()
                .setTrackingChanges(false)
                .field("old", oldVertex, OType.LINK)
                .field("new", newVertex, OType.LINK));

        if (batch > 0 && result.size() % batch == 0) {
          db.commit();
          db.begin();
        }
      }

      db.commit();

      return result;
    } finally {
      //      if (!txAlreadyBegun)
      //        db.commit();

    }
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.WRITE;
  }

  @Override
  public String getSyntax() {
    return "MOVE VERTEX <source> TO <destination> [SET [<field>=<value>]* [,]] [MERGE <JSON>] [BATCH <batch-size>]";
  }

  private OVertex toVertex(OIdentifiable item) {
    if (item != null && item instanceof OElement) {
      return ((OElement) item).asVertex().orElse(null);
    } else {
      item = getDatabase().load(item.getIdentity());
      if (item != null && item instanceof OElement) {
        return ((OElement) item).asVertex().orElse(null);
      }
    }
    return null;
  }
}
