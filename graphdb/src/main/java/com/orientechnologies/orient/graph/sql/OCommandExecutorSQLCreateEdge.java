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
package com.orientechnologies.orient.graph.sql;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLRetryAbstract;
import com.orientechnologies.orient.core.sql.OCommandParameters;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * SQL CREATE EDGE command.
 * 
 * @author Luca Garulli
 */
public class OCommandExecutorSQLCreateEdge extends OCommandExecutorSQLRetryAbstract implements OCommandDistributedReplicateRequest {
  public static final String  NAME          = "CREATE EDGE";
  private static final String KEYWORD_BATCH = "BATCH";

  private String                      from;
  private String                      to;
  private OClass                      clazz;
  private String                      edgeLabel;
  private String                      clusterName;
  private List<OPair<String, Object>> fields;
  private int                         batch = 100;

  @SuppressWarnings("unchecked")
  public OCommandExecutorSQLCreateEdge parse(final OCommandRequest iRequest) {
    final OCommandRequestText textRequest = (OCommandRequestText) iRequest;

    String queryText = textRequest.getText();
    String originalQuery = queryText;
    try {
      // System.out.println("NEW PARSER FROM: " + queryText);
      queryText = preParse(queryText, iRequest);
      // System.out.println("NEW PARSER TO: " + queryText);
      textRequest.setText(queryText);

      final ODatabaseDocument database = getDatabase();

      init((OCommandRequestText) iRequest);

      parserRequiredKeyword("CREATE");
      parserRequiredKeyword("EDGE");

      String className = null;

      String tempLower = parseOptionalWord(false);
      String temp = tempLower == null ? null : tempLower.toUpperCase();

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

        } else if (temp.equals(KEYWORD_RETRY)) {
          parseRetry();

        } else if (temp.equals(KEYWORD_BATCH)) {
          temp = parserNextWord(true);
          if (temp != null)
            batch = Integer.parseInt(temp);

        } else if (className == null && temp.length() > 0) {
          className = tempLower;
          OrientBaseGraph graph = OrientBaseGraph.getActiveGraph();
          if (graph != null && graph.isUseClassForEdgeLabel()) {
            clazz = ((OMetadataInternal) database.getMetadata()).getImmutableSchemaSnapshot().getClass(temp);
          } else {
            clazz = ((OMetadataInternal) database.getMetadata()).getImmutableSchemaSnapshot().getClass("E");
          }
        }

        temp = parseOptionalWord(true);
        if (parserIsEnded())
          break;
      }

      if (className == null) {
        // ASSIGN DEFAULT CLASS
        className = OrientEdgeType.CLASS_NAME;
        clazz = ((OMetadataInternal) database.getMetadata()).getImmutableSchemaSnapshot().getClass(className);
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

  /**
   * Execute the command and return the ODocument object created.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (clazz == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    return OGraphCommandExecutorSQLFactory.runInConfiguredTxMode(new OGraphCommandExecutorSQLFactory.GraphCallBack<List<Object>>() {
      @Override
      public List<Object> call(OrientBaseGraph graph) {
        final Set<OIdentifiable> fromIds = OSQLEngine.getInstance().parseRIDTarget(graph.getRawGraph(), from, context, iArgs);
        final Set<OIdentifiable> toIds = OSQLEngine.getInstance().parseRIDTarget(graph.getRawGraph(), to, context, iArgs);

        // CREATE EDGES
        final List<Object> edges = new ArrayList<Object>();
        for (OIdentifiable from : fromIds) {
          final OrientVertex fromVertex = graph.getVertex(from);
          if (fromVertex == null)
            throw new OCommandExecutionException("Source vertex '" + from + "' not exists");

          for (OIdentifiable to : toIds) {
            final OrientVertex toVertex;
            if (from.equals(to)) {
              toVertex = fromVertex;
            } else {
              toVertex = graph.getVertex(to);
            }

            final String clsName = clazz.getName();

            if (fields != null)
              // EVALUATE FIELDS
              for (final OPair<String, Object> f : fields) {
                if (f.getValue() instanceof OSQLFunctionRuntime)
                  f.setValue(((OSQLFunctionRuntime) f.getValue()).getValue(to, null, context));
              }

            OrientEdge edge = null;
            if (content != null) {
              if (fields != null)
                // MERGE CONTENT WITH FIELDS
                fields.addAll(OPair.convertFromMap(content.toMap()));
              else
                fields = OPair.convertFromMap(content.toMap());
            }

            edge = fromVertex.addEdge(null, toVertex, edgeLabel, clusterName, fields);

            if (fields != null && !fields.isEmpty()) {
              if (edge.isLightweight())
                edge.convertToDocument();

              OSQLHelper.bindParameters(edge.getRecord(), fields, new OCommandParameters(iArgs), context);
            }

            edge.save(clusterName);

            edges.add(edge);

            if (batch > 0 && edges.size() % batch == 0) {
              graph.commit();
              graph.begin();
            }
          }
        }

        if (edges.isEmpty()) {
          if (fromIds.isEmpty())
            throw new OCommandExecutionException("No edge has been created because no source vertices");
          else if (toIds.isEmpty())
            throw new OCommandExecutionException("No edge has been created because no target vertices");
          throw new OCommandExecutionException("No edge has been created between " + fromIds + " and " + toIds);
        }

        return edges;
      }
    });
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.WRITE;
  }

  @Override
  public Set<String> getInvolvedClusters() {
    if (clazz != null)
      return Collections.singleton(getDatabase().getClusterNameById(clazz.getClusterSelection().getCluster(clazz, null)));
    else if (clusterName != null)
      return getInvolvedClustersOfClusters(Collections.singleton(clusterName));

    return Collections.EMPTY_SET;
  }

  @Override
  public OCommandDistributedReplicateRequest.DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.LOCAL;
  }

  @Override
  public String getSyntax() {
    return "CREATE EDGE [<class>] [CLUSTER <cluster>] FROM <rid>|(<query>|[<rid>]*) TO <rid>|(<query>|[<rid>]*) [SET <field> = <expression>[,]*]|CONTENT {<JSON>} [RETRY <retry> [WAIT <pauseBetweenRetriesInMs]] [BATCH <batch-size>]";
  }
}
