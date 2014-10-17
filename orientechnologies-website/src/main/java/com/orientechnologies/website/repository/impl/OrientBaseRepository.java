package com.orientechnologies.website.repository.impl;

import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Created by Enrico Risa on 17/10/14.
 */
public abstract class OrientBaseRepository<T> {

  public abstract ODocument toDoc(T entity);

  public abstract T fromDoc(ODocument doc);
}
