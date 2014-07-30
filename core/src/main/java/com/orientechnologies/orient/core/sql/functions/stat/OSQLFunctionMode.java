package com.orientechnologies.orient.core.sql.functions.stat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;

/**
 * Compute the mode (or multimodal) value for a field. The scores in the field's distribution that occurs more frequently.
 */
public class OSQLFunctionMode extends OSQLFunctionAbstract {

  public static final String   NAME     = "mode";

  private Map<Object, Integer> seen     = new HashMap<Object, Integer>();
  private int                  max      = 0;
  private List<Object>         maxElems = new ArrayList<Object>();

  public OSQLFunctionMode() {
    super("mode", 1, 1);
  }

  @Override
  public Object execute(Object iThis, OIdentifiable iCurrentRecord, Object iCurrentResult, Object[] iParams,
      OCommandContext iContext) {

    if (OMultiValue.isMultiValue(iParams[0])) {
      for (Object o : OMultiValue.getMultiValueIterable(iParams[0])) {
        max = computeMode(o, 1, seen, maxElems, max);
      }
    } else {
      max = computeMode(iParams[0], 1, seen, maxElems, max);
    }
    return getResult();
  }

  @Override
  public Object getResult() {
    if (returnDistributedResult()) {
      return seen;
    } else {
      return this.maxElems;
    }
  }

  @Override
  public String getSyntax() {
    return NAME + "(<field>)";
  }

  @Override
  public boolean aggregateResults() {
    return true;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object mergeDistributedResult(List<Object> resultsToMerge) {
    Map<Object, Integer> dSeen = new HashMap<Object, Integer>();
    int dMax = 0;
    List<Object> dMaxElems = new ArrayList<Object>();
    for (Object iParameter : resultsToMerge) {
      final Map<Object, Integer> mSeen = (Map<Object, Integer>) iParameter;
      for (Entry<Object, Integer> o : mSeen.entrySet()) {
        dMax = this.computeMode(o.getKey(), o.getValue(), dSeen, dMaxElems, dMax);
      }
    }
    return dMaxElems;
  }

  private int computeMode(Object value, int times, Map<Object, Integer> iSeen, List<Object> iMaxElems, int iMax) {
    if (value != null) {
      if (iSeen.containsKey(value)) {
        iSeen.put(value, iSeen.get(value) + times);
      } else {
        iSeen.put(value, times);
      }
      if (iSeen.get(value) > iMax) {
        iMax = iSeen.get(value);
        iMaxElems.clear();
        iMaxElems.add(value);
      } else if (iSeen.get(value) == iMax) {
        iMaxElems.add(value);
      }
    }
    return iMax;
  }

}
