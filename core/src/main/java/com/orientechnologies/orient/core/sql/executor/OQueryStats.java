package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class contains statistics about graph structure and query execution.
 * <p>
 * To obtain a copy of this object, use
 *
 * @author Luigi Dell'Aquila
 */
public class OQueryStats {

  public Map<String, Long> stats = new ConcurrentHashMap<>();

  public static OQueryStats get(ODatabaseDocumentInternal db) {
    return db.getSharedContext().getQueryStats();
  }

  public long getAverageOutEdgeSpan(String vertexClass, String edgeClass) {
    String key = generateKey(vertexClass, "-", edgeClass, "->");
    Long val = stats.get(key);
    if (val != null) {
      return val;
    }
    return -1;
  }

  public long getAverageInEdgeSpan(String vertexClass, String edgeClass) {
    String key = generateKey(vertexClass, "<-", edgeClass, "-");
    Long val = stats.get(key);
    if (val != null) {
      return val;
    }
    return -1;
  }

  public long getAverageBothEdgeSpan(String vertexClass, String edgeClass) {
    String key = generateKey(vertexClass, "-", edgeClass, "-");
    Long val = stats.get(key);
    if (val != null) {
      return val;
    }
    return -1;
  }

  public void pushAverageOutEdgeSpan(String vertexClass, String edgeClass, Long value) {
    String key = generateKey(vertexClass, "-", edgeClass, "->");
    pushValue(key, value);
  }

  public void pushAverageInEdgeSpan(String vertexClass, String edgeClass, Long value) {
    String key = generateKey(vertexClass, "<-", edgeClass, "-");
    pushValue(key, value);
  }

  public void pushAverageBothEdgeSpan(String vertexClass, String edgeClass, Long value) {
    String key = generateKey(vertexClass, "-", edgeClass, "-");
    pushValue(key, value);
  }

  private void pushValue(String key, Long value) {
    if (value == null) {
      return;
    }
    Long val = stats.get(key);

    if (val == null) {
      val = value;
    } else {
      //refine this ;-)
      val = ((Double) ((val * .99) + (value * .01))).longValue();
      if (value > 0 && val == 0) {
        val = 1l;
      }
    }
    stats.put(key, val);
  }

  protected String generateKey(String... keys) {
    StringBuilder result = new StringBuilder();
    for (String s : keys) {
      result.append(".->");
      result.append(s);
    }
    return result.toString();
  }
}
