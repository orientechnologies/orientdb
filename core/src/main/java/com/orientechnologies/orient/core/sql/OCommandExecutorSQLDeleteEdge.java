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
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestInternal;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * SQL DELETE EDGE command.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OCommandExecutorSQLDeleteEdge extends OCommandExecutorSQLSetAware
    implements OCommandDistributedReplicateRequest, OCommandResultListener {
  public static final String NAME = "DELETE EDGE";
  private static final String KEYWORD_BATCH = "BATCH";
  private List<ORecordId> rids;
  private String fromExpr;
  private String toExpr;
  private int removed = 0;
  private OCommandRequest query;
  private OSQLFilter compiledFilter;
  //  private AtomicReference<OrientBaseGraph> currentGraph  = new
  // AtomicReference<OrientBaseGraph>();
  private String label;
  private OModifiableBoolean shutdownFlag = new OModifiableBoolean();
  private boolean txAlreadyBegun;
  private int batch = 100;

  @SuppressWarnings("unchecked")
  public OCommandExecutorSQLDeleteEdge parse(final OCommandRequest iRequest) {
    final OCommandRequestText textRequest = (OCommandRequestText) iRequest;

    String queryText = textRequest.getText();
    String originalQuery = queryText;

    try {
      queryText = preParse(queryText, iRequest);
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
        originalTemp =
            parserText.substring(parserGetPreviousPosition(), parserGetCurrentPosition()).trim();

      final OModifiableBoolean shutdownFlag = new OModifiableBoolean();
      ODatabaseDocumentInternal curDb = ODatabaseRecordThreadLocal.instance().get();
      //      final OrientGraph graph = OGraphCommandExecutorSQLFactory.getGraph(false,
      // shutdownFlag);
      try {
        while (temp != null) {

          if (temp.equals("FROM")) {
            fromExpr = parserRequiredWord(false, "Syntax error", " =><,\r\n");
            if (rids != null)
              throwSyntaxErrorException(
                  "FROM '" + fromExpr + "' is not allowed when specify a RIDs (" + rids + ")");

          } else if (temp.equals("TO")) {
            toExpr = parserRequiredWord(false, "Syntax error", " =><,\r\n");
            if (rids != null)
              throwSyntaxErrorException(
                  "TO '" + toExpr + "' is not allowed when specify a RID (" + rids + ")");

          } else if (temp.startsWith("#")) {
            rids = new ArrayList<ORecordId>();
            rids.add(new ORecordId(temp));
            if (fromExpr != null || toExpr != null)
              throwSyntaxErrorException(
                  "Specifying the RID " + rids + " is not allowed with FROM/TO");

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
              clazz = curDb.getMetadata().getImmutableSchemaSnapshot().getClass("E");

            where =
                parserGetCurrentPosition() > -1
                    ? " " + parserText.substring(parserGetCurrentPosition())
                    : "";

            compiledFilter =
                OSQLEngine.getInstance().parseCondition(where, getContext(), KEYWORD_WHERE);
            break;

          } else if (temp.equals(KEYWORD_BATCH)) {
            temp = parserNextWord(true);
            if (temp != null) batch = Integer.parseInt(temp);

          } else if (temp.equals(KEYWORD_LIMIT)) {
            temp = parserNextWord(true);
            if (temp != null) limit = Integer.parseInt(temp);

          } else if (temp.length() > 0) {
            // GET/CHECK CLASS NAME
            label = originalTemp;
            clazz = curDb.getMetadata().getSchema().getClass(temp);
            if (clazz == null)
              throw new OCommandSQLParsingException("Class '" + temp + "' was not found");
          }

          temp = parseOptionalWord(true);
          if (parserIsEnded()) break;
        }

        if (where == null)
          if (limit > -1) {
            where = " LIMIT " + limit;
          } else {
            where = "";
          }
        else where = " WHERE " + where;

        if (fromExpr == null && toExpr == null && rids == null)
          if (clazz == null)
            // DELETE ALL THE EDGES
            query = curDb.command(new OSQLAsynchQuery<ODocument>("select from E" + where, this));
          else
            // DELETE EDGES OF CLASS X
            query =
                curDb.command(
                    new OSQLAsynchQuery<ODocument>(
                        "select from `" + clazz.getName() + "` " + where, this));

        return this;
      } finally {
        ODatabaseRecordThreadLocal.instance().set(curDb);
      }
    } finally {
      textRequest.setText(originalQuery);
    }
  }

  /** Execute the command and return the ODocument object created. */
  public Object execute(final Map<Object, Object> iArgs) {
    if (fromExpr == null
        && toExpr == null
        && rids == null
        && query == null
        && compiledFilter == null)
      throw new OCommandExecutionException(
          "Cannot execute the command because it has not been parsed yet");
    ODatabaseDocumentInternal db = getDatabase();
    txAlreadyBegun = db.getTransaction().isActive();

    if (rids != null) {
      // REMOVE PUNCTUAL RID
      db.begin();
      for (ORecordId rid : rids) {
        final OEdge e = toEdge(rid);
        if (e != null) {
          e.delete();
          removed++;
        }
      }
      db.commit();
      return removed;
    } else {
      // MULTIPLE EDGES
      final Set<OEdge> edges = new HashSet<OEdge>();
      if (query == null) {
        db.begin();
        Set<OIdentifiable> fromIds = null;
        if (fromExpr != null)
          fromIds = OSQLEngine.getInstance().parseRIDTarget(db, fromExpr, context, iArgs);
        Set<OIdentifiable> toIds = null;
        if (toExpr != null)
          toIds = OSQLEngine.getInstance().parseRIDTarget(db, toExpr, context, iArgs);
        if (label == null) label = "E";

        if (fromIds != null && toIds != null) {
          int fromCount = 0;
          int toCount = 0;
          for (OIdentifiable fromId : fromIds) {
            final OVertex v = toVertex(fromId);
            if (v != null) fromCount += count(v.getEdges(ODirection.OUT, label));
          }
          for (OIdentifiable toId : toIds) {
            final OVertex v = toVertex(toId);
            if (v != null) toCount += count(v.getEdges(ODirection.IN, label));
          }
          if (fromCount <= toCount) {
            // REMOVE ALL THE EDGES BETWEEN VERTICES
            for (OIdentifiable fromId : fromIds) {
              final OVertex v = toVertex(fromId);
              if (v != null)
                for (OEdge e : v.getEdges(ODirection.OUT, label)) {
                  final OIdentifiable inV = ((OEdge) e).getTo();
                  if (inV != null && toIds.contains(inV.getIdentity())) edges.add(e);
                }
            }
          } else {
            for (OIdentifiable toId : toIds) {
              final OVertex v = toVertex(toId);
              if (v != null)
                for (OEdge e : v.getEdges(ODirection.IN, label)) {
                  final OIdentifiable outV = ((OEdge) e).getFrom();
                  if (outV != null && fromIds.contains(outV.getIdentity())) edges.add(e);
                }
            }
          }
        } else if (fromIds != null) {
          // REMOVE ALL THE EDGES THAT START FROM A VERTEXES
          for (OIdentifiable fromId : fromIds) {

            final OVertex v = toVertex(fromId);
            if (v != null) {
              for (OEdge e : v.getEdges(ODirection.OUT, label)) {
                edges.add(e);
              }
            }
          }
        } else if (toIds != null) {
          // REMOVE ALL THE EDGES THAT ARRIVE TO A VERTEXES
          for (OIdentifiable toId : toIds) {
            final OVertex v = toVertex(toId);
            if (v != null) {
              for (OEdge e : v.getEdges(ODirection.IN, label)) {
                edges.add(e);
              }
            }
          }
        } else throw new OCommandExecutionException("Invalid target: " + toIds);

        if (compiledFilter != null) {
          // ADDITIONAL FILTERING
          for (Iterator<OEdge> it = edges.iterator(); it.hasNext(); ) {
            final OEdge edge = it.next();
            if (!(Boolean) compiledFilter.evaluate(edge.getRecord(), null, context)) it.remove();
          }
        }

        // DELETE THE FOUND EDGES
        removed = edges.size();
        for (OEdge edge : edges) edge.delete();

        db.commit();
        return removed;

      } else {
        db.begin();
        // TARGET IS A CLASS + OPTIONAL CONDITION
        query.setContext(getContext());
        query.execute(iArgs);
        db.commit();
        return removed;
      }
    }
  }

  private int count(Iterable<OEdge> edges) {
    int result = 0;
    for (OEdge x : edges) {
      result++;
    }
    return result;
  }

  /** Delete the current edge. */
  public boolean result(final Object iRecord) {
    final OIdentifiable id = (OIdentifiable) iRecord;

    if (compiledFilter != null) {
      // ADDITIONAL FILTERING
      if (!(Boolean) compiledFilter.evaluate(id.getRecord(), null, context)) return true;
    }

    if (id.getIdentity().isValid()) {

      final OEdge e = toEdge(id);

      if (e != null) {
        e.delete();

        if (!txAlreadyBegun && batch > 0 && (removed + 1) % batch == 0) {
          getDatabase().commit();
          getDatabase().begin();
        }

        removed++;
      }
    }

    return true;
  }

  private OEdge toEdge(OIdentifiable item) {
    if (item != null && item instanceof OElement) {
      final OIdentifiable a = item;
      return ((OElement) item)
          .asEdge()
          .orElseThrow(
              () -> new OCommandExecutionException("" + (a.getIdentity()) + " is not an edge"));
    } else {
      item = getDatabase().load(item.getIdentity());
      if (item != null && item instanceof OElement) {
        final OIdentifiable a = item;
        return ((OElement) item)
            .asEdge()
            .orElseThrow(
                () -> new OCommandExecutionException("" + (a.getIdentity()) + " is not an edge"));
      }
    }
    return null;
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

  @Override
  public String getSyntax() {
    return "DELETE EDGE <rid>|FROM <rid>|TO <rid>|<[<class>] [WHERE <conditions>]> [BATCH <batch-size>]";
  }

  @Override
  public void end() {}

  @Override
  public int getSecurityOperationType() {
    return ORole.PERMISSION_DELETE;
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.WRITE;
  }

  public DISTRIBUTED_RESULT_MGMT getDistributedResultManagement() {
    if (getDistributedExecutionMode() == DISTRIBUTED_EXECUTION_MODE.LOCAL) {
      return DISTRIBUTED_RESULT_MGMT.CHECK_FOR_EQUALS;
    } else {
      return DISTRIBUTED_RESULT_MGMT.MERGE;
    }
  }

  @Override
  public Object getResult() {
    return null;
  }

  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    if (query != null && !getDatabase().getTransaction().isActive()) {
      return DISTRIBUTED_EXECUTION_MODE.REPLICATE;
    } else {
      return DISTRIBUTED_EXECUTION_MODE.LOCAL;
    }
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

      final OCommandExecutor executor =
          getDatabase()
              .getSharedContext()
              .getOrientDB()
              .getScriptManager()
              .getCommandManager()
              .getExecutor((OCommandRequestInternal) query);
      // COPY THE CONTEXT FROM THE REQUEST
      executor.setContext(context);
      executor.parse(query);
      return executor.getInvolvedClusters();
    }
    return result;
  }

  /** setLimit() for DELETE EDGE is ignored. Please use LIMIT keyword in the SQL statement */
  public <RET extends OCommandExecutor> RET setLimit(final int iLimit) {
    // do nothing
    return (RET) this;
  }
}
