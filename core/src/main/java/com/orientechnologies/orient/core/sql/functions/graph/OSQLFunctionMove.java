package com.orientechnologies.orient.core.sql.functions.graph;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionConfigurableAbstract;
import java.util.ArrayList;
import java.util.List;

/** Created by luigidellaquila on 03/01/17. */
public abstract class OSQLFunctionMove extends OSQLFunctionConfigurableAbstract {
  public static final String NAME = "move";

  public OSQLFunctionMove() {
    super(NAME, 1, 2);
  }

  public OSQLFunctionMove(final String iName, final int iMin, final int iMax) {
    super(iName, iMin, iMax);
  }

  protected abstract Object move(
      final ODatabase db, final OIdentifiable iRecord, final String[] iLabels);

  public String getSyntax() {
    return "Syntax error: " + name + "([<labels>])";
  }

  public Object execute(
      final Object iThis,
      final OIdentifiable iCurrentRecord,
      final Object iCurrentResult,
      final Object[] iParameters,
      final OCommandContext iContext) {

    ODatabase db =
        iContext != null
            ? iContext.getDatabase()
            : ODatabaseRecordThreadLocal.instance().getIfDefined();

    final String[] labels;
    if (iParameters != null && iParameters.length > 0 && iParameters[0] != null)
      labels =
          OMultiValue.array(
              iParameters,
              String.class,
              new OCallable<Object, Object>() {

                @Override
                public Object call(final Object iArgument) {
                  return OIOUtils.getStringContent(iArgument);
                }
              });
    else labels = null;

    return OSQLEngine.foreachRecord(
        new OCallable<Object, OIdentifiable>() {
          @Override
          public Object call(final OIdentifiable iArgument) {
            return move(db, iArgument, labels);
          }
        },
        iThis,
        iContext);
  }

  protected Object v2v(
      final ODatabase graph,
      final OIdentifiable iRecord,
      final ODirection iDirection,
      final String[] iLabels) {
    if (iRecord != null) {
      OElement rec = iRecord.getRecord();
      if (rec != null && rec.isVertex()) {
        return rec.asVertex().get().getVertices(iDirection, iLabels);
      } else {
        return null;
      }
    } else {
      return null;
    }
  }

  protected Object v2e(
      final ODatabase graph,
      final OIdentifiable iRecord,
      final ODirection iDirection,
      final String[] iLabels) {
    if (iRecord != null) {
      OElement rec = iRecord.getRecord();
      if (rec != null && rec.isVertex()) {
        return rec.asVertex().get().getEdges(iDirection, iLabels);
      } else {
        return null;
      }
    } else {
      return null;
    }
  }

  protected Object e2v(
      final ODatabase graph,
      final OIdentifiable iRecord,
      final ODirection iDirection,
      final String[] iLabels) {
    if (iRecord != null) {
      OElement rec = iRecord.getRecord();
      if (rec.isEdge()) {
        if (iDirection == ODirection.BOTH) {
          List results = new ArrayList();
          results.add(rec.asEdge().get().getVertex(ODirection.OUT));
          results.add(rec.asEdge().get().getVertex(ODirection.IN));
          return results;
        }
        return rec.asEdge().get().getVertex(iDirection);
      } else {
        return null;
      }
    } else {
      return null;
    }
  }
}
