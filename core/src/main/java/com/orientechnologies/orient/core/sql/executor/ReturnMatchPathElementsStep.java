package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by luigidellaquila on 12/10/16.
 */
public class ReturnMatchPathElementsStep extends AbstractUnrollStep {

  public ReturnMatchPathElementsStep(OCommandContext context) {
    super(context);
  }

  @Override protected Collection<OResult> unroll(OResult doc, OCommandContext iContext) {
    List<OResult> result = new ArrayList<>();
    for (String s : doc.getPropertyNames()) {
      Object elem = doc.getProperty(s);
      if (elem instanceof OIdentifiable) {
        OResultInternal newelem = new OResultInternal();
        newelem.setElement((OIdentifiable) elem);
        elem = newelem;
      }
      if (elem instanceof OResult) {
        result.add((OResult) elem);
      }
      //else...? TODO
    }
    return result;
  }

  @Override public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ UNROLL $pathElements";
  }
}
