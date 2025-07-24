package com.orientechnologies.orient.core.command.script.transformer;

import com.orientechnologies.orient.core.command.script.OScriptResultSet;
import com.orientechnologies.orient.core.command.script.OScriptResultSets;
import com.orientechnologies.orient.core.command.script.transformer.result.OResultTransformer;
import com.orientechnologies.orient.core.command.script.transformer.resultset.OResultSetTransformer;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/** Created by Enrico Risa on 27/01/17. */
public abstract class OScriptTransformerAbstract implements OScriptTransformer {

  protected Map<Class, OResultSetTransformer> resultSetTransformers = new HashMap<>();
  protected Map<Class, OResultTransformer> transformers = new LinkedHashMap<>(2);

  @Override
  public OResultSet toResultSet(Object value) {

    if (value == null) {
      return OScriptResultSets.empty();
    }
    if (value instanceof OResultSet) {
      return (OResultSet) value;
    } else if (value instanceof Iterator) {
      return new OScriptResultSet((Iterator) value, this);
    }
    OResultSetTransformer oResultSetTransformer = resultSetTransformers.get(value.getClass());

    if (oResultSetTransformer != null) {
      return oResultSetTransformer.transform(value);
    }
    return defaultResultSet(value);
  }

  protected OResultSet defaultResultSet(Object value) {
    return new OScriptResultSet(Collections.singletonList(value).iterator(), this);
  }

  @Override
  public OResult toResult(Object value) {

    OResultTransformer transformer = getTransformer(value.getClass());

    if (transformer == null) {
      return defaultTransformer(value);
    }
    return transformer.transform(value);
  }

  public OResultTransformer getTransformer(final Class clazz) {
    if (clazz != null)
      for (Map.Entry<Class, OResultTransformer> entry : transformers.entrySet()) {
        if (entry.getKey().isAssignableFrom(clazz)) return entry.getValue();
      }
    return null;
  }

  @Override
  public boolean doesHandleResult(Object value) {
    return getTransformer(value.getClass()) != null;
  }

  protected OResult defaultTransformer(Object value) {
    OResultInternal internal = new OResultInternal();
    internal.setProperty("value", value);
    return internal;
  }

  public void registerResultTransformer(Class clazz, OResultTransformer transformer) {
    transformers.put(clazz, transformer);
  }

  public void registerResultSetTransformer(Class clazz, OResultSetTransformer transformer) {
    resultSetTransformers.put(clazz, transformer);
  }
}
