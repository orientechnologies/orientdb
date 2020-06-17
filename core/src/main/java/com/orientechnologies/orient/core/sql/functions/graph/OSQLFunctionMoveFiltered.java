package com.orientechnologies.orient.core.sql.functions.graph;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionFiltered;

/** Created by luigidellaquila on 03/01/17. */
public abstract class OSQLFunctionMoveFiltered extends OSQLFunctionMove
    implements OSQLFunctionFiltered {

  protected static int supernodeThreshold = 1000; // move to some configuration

  public OSQLFunctionMoveFiltered() {
    super(NAME, 1, 2);
  }

  public OSQLFunctionMoveFiltered(final String iName, final int iMin, final int iMax) {
    super(iName, iMin, iMax);
  }

  @Override
  public Object execute(
      final Object iThis,
      final OIdentifiable iCurrentRecord,
      final Object iCurrentResult,
      final Object[] iParameters,
      final Iterable<OIdentifiable> iPossibleResults,
      final OCommandContext iContext) {
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
            return move(iContext.getDatabase(), iArgument, labels, iPossibleResults);
          }
        },
        iThis,
        iContext);
  }

  protected abstract Object move(
      ODatabase graph,
      OIdentifiable iArgument,
      String[] labels,
      Iterable<OIdentifiable> iPossibleResults);
}
