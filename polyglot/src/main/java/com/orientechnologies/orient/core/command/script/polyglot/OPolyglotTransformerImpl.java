package com.orientechnologies.orient.core.command.script.polyglot;

import com.orientechnologies.orient.core.command.script.transformer.OScriptTransformer;
import com.orientechnologies.orient.core.command.script.transformer.OScriptTransformerAbstract;
import com.orientechnologies.orient.core.command.script.transformer.result.MapTransformer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.graalvm.polyglot.Value;

public class OPolyglotTransformerImpl extends OScriptTransformerAbstract
    implements OScriptTransformer {

  public OPolyglotTransformerImpl() {
    registerResultTransformer(Map.class, new MapTransformer(this));
  }

  @Override
  public OResultSet toResultSet(Object value) {
    if (value instanceof Value) {
      final Value v = (Value) value;
      if (v.isNull()) return null;
      else if (v.hasArrayElements()) {
        final List<Object> array = new ArrayList<>((int) v.getArraySize());
        for (int i = 0; i < v.getArraySize(); ++i)
          array.add(new OResultInternal((OIdentifiable) v.getArrayElement(i).asHostObject()));
        value = array;
      } else if (v.isHostObject()) value = v.asHostObject();
      else if (v.isString()) value = v.asString();
      else if (v.isNumber()) value = v.asDouble();
      else value = v;
    }
    return super.toResultSet(value);
  }
}
