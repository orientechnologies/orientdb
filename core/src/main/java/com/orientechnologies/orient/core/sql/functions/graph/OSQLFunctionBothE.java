package com.orientechnologies.orient.core.sql.functions.graph;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ODirection;

/** Created by luigidellaquila on 03/01/17. */
public class OSQLFunctionBothE extends OSQLFunctionMove {
  public static final String NAME = "bothE";

  public OSQLFunctionBothE() {
    super(NAME, 0, -1);
  }

  @Override
  protected Object move(
      final ODatabase graph, final OIdentifiable iRecord, final String[] iLabels) {
    return v2e(graph, iRecord, ODirection.BOTH, iLabels);
  }
}
