package org.apache.tinkerpop.gremlin.orientdb.executor.transformer;

import com.orientechnologies.orient.core.command.script.transformer.result.OResultTransformer;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import org.apache.tinkerpop.gremlin.orientdb.OrientElement;

/** Created by Enrico Risa on 24/01/17. */
public class OElementTransformer implements OResultTransformer<OrientElement> {

  @Override
  public OResult transform(OrientElement element) {
    OResultInternal internal = new OResultInternal();
    internal.setElement(element.getRawElement());
    return internal;
  }
}
