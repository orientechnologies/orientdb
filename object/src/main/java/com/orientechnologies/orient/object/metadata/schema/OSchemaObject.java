package com.orientechnologies.orient.object.metadata.schema;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;

/** Created by tglman on 12/01/17. */
public interface OSchemaObject extends OSchema {
  OClass createClass(Class<?> iClass);

  OClass createAbstractClass(Class<?> iClass);
}
