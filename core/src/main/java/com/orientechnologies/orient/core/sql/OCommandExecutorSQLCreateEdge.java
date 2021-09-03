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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItem;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * SQL CREATE EDGE command.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OCommandExecutorSQLCreateEdge extends OCommandExecutorSQLSetAware
    implements OCommandDistributedReplicateRequest {
  public static final String NAME = "CREATE EDGE";
  private static final String KEYWORD_BATCH = "BATCH";

  private String from;
  private String to;
  private OClass clazz;
  private String edgeLabel;
  private String clusterName;
  private List<OPair<String, Object>> fields;
  private int batch = 100;

  @SuppressWarnings("unchecked")
  public OCommandExecutorSQLCreateEdge parse(final OCommandRequest iRequest) {
    final OCommandRequestText textRequest = (OCommandRequestText) iRequest;

    String queryText = textRequest.getText();
    String originalQuery = queryText;
    try {
      queryText = preParse(queryText, iRequest);
      textRequest.setText(queryText);

      final ODatabaseDocument database = getDatabase();

      init((OCommandRequestText) iRequest);

      parserRequiredKeyword("CREATE");
      parserRequiredKeyword("EDGE");

      String className = null;

      String tempLower = parseOptionalWord(false);
      String temp = tempLower == null ? null : tempLower.toUpperCase(Locale.ENGLISH);

      while (temp != null) {
        if (temp.equals("CLUSTER")) {
          clusterName = parserRequiredWord(false);

        } else if (temp.equals(KEYWORD_FROM)) {
          from = parserRequiredWord(false, "Syntax error", " =><,\r\n");

        } else if (temp.equals("TO")) {
          to = parserRequiredWord(false, "Syntax error", " =><,\r\n");

        } else if (temp.equals(KEYWORD_SET)) {
          fields = new ArrayList<OPair<String, Object>>();
          parseSetFields(clazz, fields);

        } else if (temp.equals(KEYWORD_CONTENT)) {
          parseContent();

        } else if (temp.equals(KEYWORD_BATCH)) {
          temp = parserNextWord(true);
          if (temp != null) batch = Integer.parseInt(temp);

        } else if (className == null && temp.length() > 0) {
          className = tempLower;

          clazz =
              ((OMetadataInternal) database.getMetadata())
                  .getImmutableSchemaSnapshot()
                  .getClass(temp);
          if (clazz == null) {
            final int committed;
            if (database.getTransaction().isActive()) {
              OLogManager.instance()
                  .warn(
                      this,
                      "Requested command '"
                          + this.toString()
                          + "' must be executed outside active transaction: the transaction will be committed and reopen right after it. To avoid this behavior execute it outside a transaction");
              committed = database.getTransaction().amountOfNestedTxs();
              database.commit(true);
            } else committed = 0;

            try {
              OSchema schema = database.getMetadata().getSchema();
              OClass e = schema.getClass("E");
              clazz = schema.createClass(className, e);
            } finally {
              // RESTART TRANSACTION
              for (int i = 0; i < committed; ++i) database.begin();
            }
          }
        }

        temp = parseOptionalWord(true);
        if (parserIsEnded()) break;
      }

      if (className == null) {
        // ASSIGN DEFAULT CLASS
        className = "E";
        clazz =
            ((OMetadataInternal) database.getMetadata())
                .getImmutableSchemaSnapshot()
                .getClass(className);
      }

      // GET/CHECK CLASS NAME
      if (clazz == null)
        throw new OCommandSQLParsingException("Class '" + className + "' was not found");

      edgeLabel = className;
    } finally {
      textRequest.setText(originalQuery);
    }
    return this;
  }

  /** Execute the command and return the ODocument object created. */
  public Object execute(final Map<Object, Object> iArgs) {
    if (clazz == null)
      throw new OCommandExecutionException(
          "Cannot execute the command because it has not been parsed yet");

    ODatabaseDocumentInternal db = getDatabase();
    final List<Object> edges = new ArrayList<Object>();
    Set<OIdentifiable> fromIds = null;
    Set<OIdentifiable> toIds = null;
    db.begin();
    try {
      fromIds = OSQLEngine.getInstance().parseRIDTarget(db, from, context, iArgs);
      toIds = OSQLEngine.getInstance().parseRIDTarget(db, to, context, iArgs);

      // CREATE EDGES
      for (OIdentifiable from : fromIds) {
        final OVertex fromVertex = toVertex(from);
        if (fromVertex == null)
          throw new OCommandExecutionException("Source vertex '" + from + "' does not exist");

        for (OIdentifiable to : toIds) {
          final OVertex toVertex;
          if (from.equals(to)) {
            toVertex = fromVertex;
          } else {
            toVertex = toVertex(to);
          }
          if (toVertex == null) {
            throw new OCommandExecutionException("Source vertex '" + to + "' does not exist");
          }

          if (fields != null)
            // EVALUATE FIELDS
            for (final OPair<String, Object> f : fields) {
              if (f.getValue() instanceof OSQLFunctionRuntime) {
                f.setValue(((OSQLFunctionRuntime) f.getValue()).getValue(to, null, context));
              } else if (f.getValue() instanceof OSQLFilterItem) {
                f.setValue(((OSQLFilterItem) f.getValue()).getValue(to, null, context));
              }
            }

          OEdge edge = null;

          if (content != null) {
            if (fields != null)
              // MERGE CONTENT WITH FIELDS
              fields.addAll(OPair.convertFromMap(content.toMap()));
            else fields = OPair.convertFromMap(content.toMap());
          }

          edge = fromVertex.addEdge(toVertex, edgeLabel);
          if (fields != null && !fields.isEmpty()) {
            OSQLHelper.bindParameters(
                edge.getRecord(), fields, new OCommandParameters(iArgs), context);
          }

          edge.save(clusterName);
          fromVertex.save();
          toVertex.save();

          edges.add(edge);

          if (batch > 0 && edges.size() % batch == 0) {
            db.commit();
            db.begin();
          }
        }
      }

    } finally {
      db.commit();
    }

    if (edges.isEmpty()) {
      if (fromIds.isEmpty())
        throw new OCommandExecutionException(
            "No edge has been created because no source vertices: " + this.toString());
      else if (toIds.isEmpty())
        throw new OCommandExecutionException(
            "No edge has been created because no target vertices: " + this.toString());
      throw new OCommandExecutionException(
          "No edge has been created between " + fromIds + " and " + toIds + ": " + this.toString());
    }
    return edges;
  }

  private OVertex toVertex(OIdentifiable item) {
    if (item == null) {
      return null;
    }
    if (item instanceof OElement) {
      return ((OElement) item).asVertex().orElse(null);
    } else {
      item = getDatabase().load(item.getIdentity());
      if (item != null && item instanceof OElement) {
        return ((OElement) item).asVertex().orElse(null);
      }
    }
    return null;
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.WRITE;
  }

  @Override
  public Set<String> getInvolvedClusters() {
    if (clazz != null)
      return Collections.singleton(
          getDatabase().getClusterNameById(clazz.getClusterSelection().getCluster(clazz, null)));
    else if (clusterName != null)
      return getInvolvedClustersOfClusters(Collections.singleton(clusterName));

    return Collections.EMPTY_SET;
  }

  @Override
  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.LOCAL;
  }

  @Override
  public String getSyntax() {
    return "CREATE EDGE [<class>] [CLUSTER <cluster>] "
        + "FROM <rid>|(<query>|[<rid>]*) TO <rid>|(<query>|[<rid>]*) "
        + "[SET <field> = <expression>[,]*]|CONTENT {<JSON>} "
        + "[RETRY <retry> [WAIT <pauseBetweenRetriesInMs]] [BATCH <batch-size>]";
  }
}
