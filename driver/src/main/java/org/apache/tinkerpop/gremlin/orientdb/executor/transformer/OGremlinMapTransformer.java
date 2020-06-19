package org.apache.tinkerpop.gremlin.orientdb.executor.transformer;

import com.orientechnologies.orient.core.command.script.transformer.OScriptTransformer;
import com.orientechnologies.orient.core.command.script.transformer.result.OResultTransformer;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import java.util.Map;
import java.util.Spliterator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.tinkerpop.gremlin.orientdb.OrientElement;

/** Created by Enrico Risa on 17/01/2019. */
public class OGremlinMapTransformer implements OResultTransformer<Map<Object, Object>> {

  protected OScriptTransformer transformer;

  public OGremlinMapTransformer(OScriptTransformer transformer) {
    this.transformer = transformer;
  }

  @Override
  public OResult transform(Map<Object, Object> element) {
    OResultInternal internal = new OResultInternal();
    element.forEach(
        (key, val) -> {
          if (this.transformer.doesHandleResult(val)) {
            internal.setProperty(key.toString(), transformer.toResult(val));
          } else {

            if (val instanceof Iterable) {
              Spliterator spliterator = ((Iterable) val).spliterator();
              Object collect =
                  StreamSupport.stream(spliterator, false)
                      .map(
                          (e) -> {
                            if (e instanceof OrientElement) {
                              return ((OrientElement) e).getIdentity();
                            }
                            return this.transformer.toResult(e);
                          })
                      .collect(Collectors.toList());
              internal.setProperty(key.toString(), collect);
            } else {
              internal.setProperty(key.toString(), val);
            }
          }
        });
    return internal;
  }
}
