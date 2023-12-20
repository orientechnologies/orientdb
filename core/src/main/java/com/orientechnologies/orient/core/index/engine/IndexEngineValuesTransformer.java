package com.orientechnologies.orient.core.index.engine;

import com.orientechnologies.orient.core.id.ORID;
import java.util.Collection;

public interface IndexEngineValuesTransformer {
  Collection<ORID> transformFromValue(Object value);
}
