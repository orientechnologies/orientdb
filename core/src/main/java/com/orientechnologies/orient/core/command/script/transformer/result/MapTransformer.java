package com.orientechnologies.orient.core.command.script.transformer.result;

import com.orientechnologies.orient.core.command.script.transformer.OScriptTransformer;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;

import java.util.Map;

/**
 * Created by Enrico Risa on 24/01/17.
 */
public class MapTransformer implements OResultTransformer<Map<Object, Object>> {

  private OScriptTransformer transformer;

  public MapTransformer(OScriptTransformer transformer) {
    this.transformer = transformer;
  }

  @Override
  public OResult transform(Map<Object, Object> element) {
    OResultInternal internal = new OResultInternal();
    element.forEach((key, val) -> {
      if (transformer.doesHandleResult(val)) {
        internal.setProperty(key.toString(), transformer.toResult(val));
      } else {
        internal.setProperty(key.toString(), val);
      }

    });
    return internal;
  }
}
