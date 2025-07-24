package org.apache.tinkerpop.gremlin.orientdb.executor.transformer;

import com.orientechnologies.orient.core.command.script.transformer.OScriptTransformer;
import com.orientechnologies.orient.core.command.script.transformer.OScriptTransformerAbstract;
import com.orientechnologies.orient.core.command.script.transformer.result.MapTransformer;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Spliterator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.tinkerpop.gremlin.orientdb.OrientElement;

/** Created by Enrico Risa on 14/06/2018. */
public class OGremlinTransformer extends OScriptTransformerAbstract implements OScriptTransformer {

  public OGremlinTransformer() {
    registerResultTransformer(Map.class, new MapTransformer(this));
    registerResultTransformer(HashMap.class, new OGremlinMapTransformer(this));
    registerResultTransformer(LinkedHashMap.class, new OGremlinMapTransformer(this));
  }

  @Override
  public OResult toResult(Object value) {

    if (value instanceof Iterable) {
      Spliterator spliterator = ((Iterable) value).spliterator();
      Object collect =
          StreamSupport.stream(spliterator, false)
              .map(
                  (e) -> {
                    if (e instanceof OrientElement) {
                      return super.toResult(e);
                    } else {
                      return e;
                    }
                  })
              .collect(Collectors.toList());

      return super.toResult(collect);
    } else {
      return super.toResult(value);
    }
  }
}
