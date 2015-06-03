package com.orientechnologies.website.repository.impl;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.model.schema.Identity;
import com.orientechnologies.website.model.schema.OTypeHolder;
import com.orientechnologies.website.model.schema.SchemaManager;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Created by Enrico Risa on 03/06/15.
 */
public abstract class OrientBaseAutoRepository<T extends Identity> extends OrientBaseRepository<T> {

  @Autowired
  protected SchemaManager schemaManager;

  @Override
  public OTypeHolder<T> getHolder() {
    return null;
  }

  @Override
  public ODocument toDoc(T entity) {
    return schemaManager.toDoc(entity);
  }

  @Override
  public T fromDoc(ODocument doc) {
    return (T) schemaManager.fromDoc(doc, getEntityClass());
  }
}
