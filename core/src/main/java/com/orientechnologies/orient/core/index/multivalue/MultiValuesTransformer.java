package com.orientechnologies.orient.core.index.multivalue;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.engine.IndexEngineValuesTransformer;
import java.util.Collection;

public final class MultiValuesTransformer implements IndexEngineValuesTransformer {
  public static final MultiValuesTransformer INSTANCE = new MultiValuesTransformer();

  @Override
  public Collection<ORID> transformFromValue(Object value) {
    //noinspection unchecked
    return (Collection<ORID>) value;
  }
}
