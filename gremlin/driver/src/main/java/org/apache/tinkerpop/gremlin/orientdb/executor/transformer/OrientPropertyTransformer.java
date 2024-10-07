package org.apache.tinkerpop.gremlin.orientdb.executor.transformer;

import com.orientechnologies.orient.core.command.script.transformer.OScriptTransformer;
import com.orientechnologies.orient.core.command.script.transformer.result.OResultTransformer;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import java.util.Collection;
import java.util.stream.Collectors;
import org.apache.tinkerpop.gremlin.orientdb.OrientProperty;

/** Created by Enrico Risa on 24/01/17. */
public class OrientPropertyTransformer implements OResultTransformer<OrientProperty> {

  private final OScriptTransformer transformer;

  public OrientPropertyTransformer(OScriptTransformer transformer) {
    this.transformer = transformer;
  }

  @Override
  public OResult transform(OrientProperty element) {

    OResultInternal internal = new OResultInternal();

    Object value = element.value();
    if (value instanceof Collection) {
      internal.setProperty(
          element.key(),
          ((Collection<Object>) value)
              .stream().map(e -> this.transformer.toResult(e)).collect(Collectors.toList()));
    } else {
      internal.setProperty(element.key(), value);
    }
    return internal;
  }
}
