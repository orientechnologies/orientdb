package com.orientechnologies.orient.core.command.script.nashorn;

import com.orientechnologies.orient.core.command.script.transformer.OScriptTransformer;
import com.orientechnologies.orient.core.command.script.transformer.OScriptTransformerAbstract;
import com.orientechnologies.orient.core.command.script.transformer.result.MapTransformer;
import com.orientechnologies.orient.core.command.script.transformer.result.OResultTransformer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.openjdk.nashorn.api.scripting.JSObject;

/** Created by Enrico Risa on 27/01/17. */
public class ONashornTransformerImpl extends OScriptTransformerAbstract
    implements OScriptTransformer {

  public ONashornTransformerImpl() {
    super();
    registerResultTransformer(
        JSObject.class,
        new OResultTransformer() {
          @Override
          public OResult transform(Object value) {
            OResultInternal internal = new OResultInternal();

            final List res = new ArrayList();
            internal.setProperty("value", res);

            for (Object v : ((Map) value).values()) res.add(new OResultInternal((OIdentifiable) v));

            return internal;
          }
        });
    registerResultTransformer(Map.class, new MapTransformer(this));
  }
}
