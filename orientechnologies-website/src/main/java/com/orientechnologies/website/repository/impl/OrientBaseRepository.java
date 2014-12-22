package com.orientechnologies.website.repository.impl;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.model.schema.OTypeHolder;
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

  public ODocument toDoc(T entity) {
    return getHolder().toDoc(entity, dbFactory.getGraph());
  }

  public T fromDoc(ODocument doc) {
    return getHolder().fromDoc(doc, dbFactory.getGraph());
  }

  @Override
  public T save(T entity) {

    OrientGraph graph = dbFactory.getGraph();
    ODocument doc = graph.getRawGraph().save(toDoc(entity));
    return fromDoc(doc);
  }

  @Override
  public T load(T entity) {
    OrientGraph graph = dbFactory.getGraph();
    ODocument doc = graph.getRawGraph().load(toDoc(entity));
    return fromDoc(doc);
  }

  @Override
  public void save(Collection<T> entities) {

  }

  public abstract OTypeHolder<T> getHolder();
}
