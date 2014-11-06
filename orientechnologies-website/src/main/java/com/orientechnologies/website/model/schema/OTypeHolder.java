package com.orientechnologies.website.model.schema;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;

/**
 * Created by Enrico Risa on 04/11/14.
 */
public interface OTypeHolder<T> {
  public OType getType();

  public T fromDoc(ODocument doc, OrientBaseGraph graph);

  public ODocument toDoc(T doc, OrientBaseGraph graph);

}