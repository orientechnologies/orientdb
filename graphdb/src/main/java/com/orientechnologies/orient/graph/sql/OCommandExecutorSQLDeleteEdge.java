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

import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.orient.core.command.*;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLSetAware;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;
import com.orientechnologies.orient.core.sql.parser.ODeleteEdgeStatement;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.impls.orient.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * SQL DELETE EDGE command.
 *
 * @author Luca Garulli
 */
public class OCommandExecutorSQLDeleteEdge extends OCommandExecutorSQLSetAware
    implements OCommandDistributedReplicateRequest, OCommandResultListener {
  public static final String               NAME          = "DELETE EDGE";
  private static final String              KEYWORD_BATCH = "BATCH";
  private List<ORecordId>                  rids;
  private String                           fromExpr;
  private String                           toExpr;
  private int                              removed       = 0;
  private OCommandRequest                  query;
  private OSQLFilter                       compiledFilter;
  private AtomicReference<OrientBaseGraph> currentGraph  = new AtomicReference<OrientBaseGraph>();
  private String                           label;
  private OModifiableBoolean               shutdownFlag  = new OModifiableBoolean();
  private boolean                          txAlreadyBegun;
  private int                              batch         = 100;

  @SuppressWarnings("unchecked")
  public OCommandExecutorSQLDeleteEdge parse(final OCommandRequest iRequest) {
    final OCommandRequestText textRequest = (OCommandRequestText) iRequest;

    String queryText = textRequest.getText();
    String originalQuery = queryText;

    try {
      // System.out.println("NEW PARSER FROM: " + queryText);
      queryText = preParse(queryText, iRequest);
      // System.out.println("NEW PARSER TO: " + queryText);
      textRequest.setText(queryText);

      init((OCommandRequestText) iRequest);

      parserRequiredKeyword("DELETE");
      parserRequiredKeyword("EDGE");

      OClass clazz = null;
      String where = null;

      String temp = parseOptionalWord(true);
      String originalTemp = null;

      int limit = -1;

      if (temp != null && !parserIsEnded())
        originalTemp = parserText.substring(parserGetPreviousPosition(), parserGetCurrentPosition()).trim();

      final OModifiableBoolean shutdownFlag = new OModifiableBoolean();
      ODatabaseDocumentInternal curDb = ODatabaseRecordThreadLocal.INSTANCE.get();
      final OrientGraph graph = OGraphCommandExecutorSQLFactory.getGraph(false, shutdownFlag);
      try {
        while (temp != null) {

          if (temp.equals("FROM")) {
            fromExpr = parserRequiredWord(false, "Syntax error", " =><,\r\n");
            if (rids != null)
              throwSyntaxErrorException("FROM '" + fromExpr + "' is not allowed when specify a RIDs (" + rids + ")");

          } else if (temp.equals("TO")) {
            toExpr = parserRequiredWord(false, "Syntax error", " =><,\r\n");
            if (rids != null)
              throwSyntaxErrorException("TO '" + toExpr + "' is not allowed when specify a RID (" + rids + ")");

          } else if (temp.startsWith("#")) {
            rids = new ArrayList<ORecordId>();
            rids.add(new ORecordId(temp));
            if (fromExpr != null || toExpr != null)
              throwSyntaxErrorException("Specifying the RID " + rids + " is not allowed with FROM/TO");

          } else if (temp.startsWith("[") && temp.endsWith("]")) {
            temp = temp.substring(1, temp.length() - 1);
            rids = new ArrayList<ORecordId>();
            for (String rid : temp.split(",")) {
              rid = rid.trim();
              if (!rid.startsWith("#")) {
                throwSyntaxErrorException("Not a valid RID: " + rid);
              }
              rids.add(new ORecordId(rid));
            }
          } else if (temp.equals(KEYWORD_WHERE)) {
            if (clazz == null)
              // ASSIGN DEFAULT CLASS
              clazz = graph.getEdgeType(OrientEdgeType.CLASS_NAME);

            where = parserGetCurrentPosition() > -1 ? " " + parserText.substring(parserGetCurrentPosition()) : "";
            if(this.preParsedStatement!=null){
              StringBuilder builder = new StringBuilder();
              ((ODeleteEdgeStatement)this.preParsedStatement).getWhereClause().toString(parameters, builder);
              where = builder.toString();
            }

            compiledFilter = OSQLEngine.getInstance().parseCondition(where, getContext(), KEYWORD_WHERE);
            break;

          } else if (temp.equals(KEYWORD_BATCH)) {
            temp = parserNextWord(true);
            if (temp != null)
              batch = Integer.parseInt(temp);

          } else if (temp.equals(KEYWORD_LIMIT)) {
            temp = parserNextWord(true);
            if (temp != null)
              limit = Integer.parseInt(temp);

          } else if (temp.length() > 0) {
            // GET/CHECK CLASS NAME
            label = originalTemp;
            clazz = graph.getEdgeType(temp);
            if (clazz == null)
              throw new OCommandSQLParsingException("Class '" + temp + "' was not found");
          }

          temp = parseOptionalWord(true);
          if (parserIsEnded())
            break;
        }

        if (where == null)
          if (limit > -1) {
            where = " LIMIT " + limit;
          } else {
            where = "";
          }
        else
          where = " WHERE " + where;

        if (fromExpr == null && toExpr == null && rids == null)
          if (clazz == null)
            // DELETE ALL THE EDGES
            query = graph.getRawGraph().command(new OSQLAsynchQuery<ODocument>("select from E" + where, this));
          else
            // DELETE EDGES OF CLASS X
            query = graph.getRawGraph().command(new OSQLAsynchQuery<ODocument>("select from " + clazz.getName() + where, this));

        return this;
      } finally {
        if (shutdownFlag.getValue())
          graph.shutdown(false, false);
        ODatabaseRecordThreadLocal.INSTANCE.set(curDb);
      }
    } finally {
      textRequest.setText(originalQuery);
    }

  }

  /**
   * Execute the command and return the ODocument object created.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (fromExpr == null && toExpr == null && rids == null && query == null && compiledFilter == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    txAlreadyBegun = getDatabase().getTransaction().isActive();

    if (rids != null) {
      // REMOVE PUNCTUAL RID
      OGraphCommandExecutorSQLFactory.runInConfiguredTxMode(new OGraphCommandExecutorSQLFactory.GraphCallBack<Object>() {
        @Override
        public Object call(OrientBaseGraph graph) {
          for (ORecordId rid : rids) {
            final OrientEdge e = graph.getEdge(rid);
            if (e != null) {
              e.remove();
              removed++;
            }
          }
          return null;
        }
      });
      // CLOSE PENDING TX
      end();

    } else {
      // MULTIPLE EDGES
      final Set<OrientEdge> edges = new HashSet<OrientEdge>();
      if (query == null) {
        OGraphCommandExecutorSQLFactory.runInConfiguredTxMode(new OGraphCommandExecutorSQLFactory.GraphCallBack<Object>() {

          @Override
          public Object call(OrientBaseGraph graph) {
            Set<OIdentifiable> fromIds = null;
            if (fromExpr != null)
              fromIds = OSQLEngine.getInstance().parseRIDTarget(graph.getRawGraph(), fromExpr, context, iArgs);
            Set<OIdentifiable> toIds = null;
            if (toExpr != null)
              toIds = OSQLEngine.getInstance().parseRIDTarget(graph.getRawGraph(), toExpr, context, iArgs);
            if(label == null )
              label = OrientEdgeType.CLASS_NAME;
            
            if (fromIds != null && toIds != null) {
              int fromCount = 0;
              int toCount = 0;
              for (OIdentifiable fromId : fromIds) {
                final OrientVertex v = graph.getVertex(fromId);
                if (v != null)
                  fromCount += v.countEdges(Direction.OUT, label);
              }
              for (OIdentifiable toId : toIds) {
                final OrientVertex v = graph.getVertex(toId);
                if (v != null)
                  toCount += v.countEdges(Direction.IN, label);
              }
              if (fromCount <= toCount) {
                // REMOVE ALL THE EDGES BETWEEN VERTICES
                for (OIdentifiable fromId : fromIds) {
                  final OrientVertex v = graph.getVertex(fromId);
                  if (v != null)
                    for (Edge e : v.getEdges(Direction.OUT, label)) {
                      final OIdentifiable inV = ((OrientEdge) e).getInVertex();
                      if (inV != null && toIds.contains(inV.getIdentity()))
                        edges.add((OrientEdge) e);
                    }
                }
              } else {
                for (OIdentifiable toId : toIds) {
                  final OrientVertex v = graph.getVertex(toId);
                  if (v != null)
                    for (Edge e : v.getEdges(Direction.IN, label)) {
                      final OIdentifiable outV = ((OrientEdge) e).getOutVertex();
                      if (outV != null && fromIds.contains(outV.getIdentity()))
                        edges.add((OrientEdge) e);
                    }
                }
              }
            } else if (fromIds != null) {
              // REMOVE ALL THE EDGES THAT START FROM A VERTEXES
              for (OIdentifiable fromId : fromIds) {
                
                final OrientVertex v = graph.getVertex(fromId);
                if (v != null) {
                  for (Edge e : v.getEdges(Direction.OUT, label)) {
                    edges.add((OrientEdge) e);
                  }
                }
              }
            } else if (toIds != null) {
              // REMOVE ALL THE EDGES THAT ARRIVE TO A VERTEXES
              for (OIdentifiable toId : toIds) {
                final OrientVertex v = graph.getVertex(toId);
                if (v != null) {
                  for (Edge e : v.getEdges(Direction.IN, label)) {
                    edges.add((OrientEdge) e);
                  }
                }
              }
            } else
              throw new OCommandExecutionException("Invalid target: " + toIds);

            if (compiledFilter != null) {
              // ADDITIONAL FILTERING
              for (Iterator<OrientEdge> it = edges.iterator(); it.hasNext();) {
                final OrientEdge edge = it.next();
                if (!(Boolean) compiledFilter.evaluate(edge.getRecord(), null, context))
                  it.remove();
              }
            }

            // DELETE THE FOUND EDGES
            removed = edges.size();
            for (OrientEdge edge : edges)
              edge.remove();

            return null;
          }
        });

        // CLOSE PENDING TX
        end();

      } else {
        OGraphCommandExecutorSQLFactory.runInConfiguredTxMode(new OGraphCommandExecutorSQLFactory.GraphCallBack<OrientGraph>() {
          @Override
          public OrientGraph call(final OrientBaseGraph iGraph) {
            // TARGET IS A CLASS + OPTIONAL CONDITION
            currentGraph.set(iGraph);
            query.setContext(getContext());
            query.execute(iArgs);
            return null;
          }
        });
      }
    }

    return removed;
  }

  /**
   * Delete the current edge.
   */
  public boolean result(final Object iRecord) {
    final OIdentifiable id = (OIdentifiable) iRecord;

    if (compiledFilter != null) {
      // ADDITIONAL FILTERING
      if (!(Boolean) compiledFilter.evaluate(id.getRecord(), null, context))
        return true;
    }

    if (id.getIdentity().isValid()) {
      final OrientBaseGraph g = currentGraph.get();

      final OrientEdge e = g.getEdge(id);

      if (e != null) {
        e.remove();

        if (!txAlreadyBegun && batch > 0 && (removed + 1) % batch == 0) {
          if (g instanceof OrientGraph) {
            g.commit();
            ((OrientGraph) g).begin();
          }
        }

        removed++;
      }
    }

    return true;
  }

  @Override
  public String getSyntax() {
    return "DELETE EDGE <rid>|FROM <rid>|TO <rid>|<[<class>] [WHERE <conditions>]> [BATCH <batch-size>]";
  }

  @Override
  public void end() {
    final OrientBaseGraph g = currentGraph.get();
    if (g != null) {
      if (!txAlreadyBegun) {
        g.commit();

        if (shutdownFlag.getValue())
          g.shutdown(false, false);
      }
    }
  }

  @Override
  public int getSecurityOperationType() {
    return ORole.PERMISSION_DELETE;
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.WRITE;
  }

  public DISTRIBUTED_RESULT_MGMT getDistributedResultManagement() {
    return getDistributedExecutionMode() == DISTRIBUTED_EXECUTION_MODE.LOCAL ? DISTRIBUTED_RESULT_MGMT.CHECK_FOR_EQUALS
        : DISTRIBUTED_RESULT_MGMT.MERGE;
  }

  @Override
  public Object getResult() {
    return null;
  }

  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.LOCAL;
  }

  @Override
  public Set<String> getInvolvedClusters() {
    final HashSet<String> result = new HashSet<String>();
    if (rids != null) {
      final ODatabaseDocumentInternal database = getDatabase();
      for (ORecordId rid : rids) {
        result.add(database.getClusterNameById(rid.getClusterId()));
      }
    } else if (query != null) {
      final OCommandExecutor executor = OCommandManager.instance().getExecutor((OCommandRequestInternal) query);
      // COPY THE CONTEXT FROM THE REQUEST
      executor.setContext(context);
      executor.parse(query);
      return executor.getInvolvedClusters();
    }
    return result;
  }

  /**
   * setLimit() for DELETE EDGE is ignored. Please use LIMIT keyword in the SQL statement
   */
  public <RET extends OCommandExecutor> RET setLimit(final int iLimit) {
    // do nothing
    return (RET) this;
  }
}
