package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.OElement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by luigidellaquila on 12/10/16.
 */
public class ReturnMatchPathElementsStep extends AbstractUnrollStep {

  private Set alreadyIn = new HashSet();
  
  public ReturnMatchPathElementsStep(OCommandContext context, boolean profilingEnabled) {
    super(context, profilingEnabled);
  }

  @Override protected Collection<OResult> unroll(OResult doc, OCommandContext iContext) {    
    List<OResult> result = new ArrayList<>();
    for (String s : doc.getPropertyNames()) {
      Object elem = doc.getProperty(s);
      if (alreadyIn.contains(elem)){
        continue;
      }
      if (elem instanceof OIdentifiable) {
        OResultInternal newelem = new OResultInternal();
        newelem.setElement((OIdentifiable) elem);
        elem = newelem;
      }
      if (elem instanceof OResult) {
        result.add((OResult) elem);
        OElement el = ((OResult) elem).getElement().orElse(null);
        if (el != null){
          alreadyIn.add(el);
        }
      }
      alreadyIn.add(elem);
      //else...? TODO
    }
    return result;    
  }

  @Override public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ UNROLL $pathElements";
  }
}
