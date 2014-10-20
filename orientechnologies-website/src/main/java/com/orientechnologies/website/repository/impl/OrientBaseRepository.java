package com.orientechnologies.website.repository.impl;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.repository.BaseRepository;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Collection;

/**
 * Created by Enrico Risa on 17/10/14.
 */
public abstract class OrientBaseRepository<T> implements BaseRepository<T> {

  @Autowired
  protected OrientDBFactory dbFactory;

  public abstract ODocument toDoc(T entity);

  public abstract T fromDoc(ODocument doc);

  @Override
  public T save(T entity) {

    OrientGraph graph = dbFactory.getGraph();
    ODocument doc = graph.getRawGraph().save(toDoc(entity));
    return fromDoc(doc);
  }

  @Override
  public void save(Collection<T> entities) {

  }

}
