package com.orientechnologies.website.repository.impl;

import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.model.schema.OTypeHolder;
import com.orientechnologies.website.repository.BaseRepository;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
  public T saveAndCommit(T entity) {

    OrientGraph graph = dbFactory.getGraph();
    ODocument doc = graph.getRawGraph().save(toDoc(entity));
    graph.commit();
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

  @Override
  public void delete(T entity) {
    OrientGraph graph = dbFactory.getGraph();
    ODocument doc = toDoc(entity);
    OrientVertex vertex = new OrientVertex(graph, doc);
    vertex.remove();
  }

  @Override
  public Iterable<T> findAll() {
    OrientGraph graph = dbFactory.getGraph();
    List<T> entities = new ArrayList<T>();
    ORecordIteratorClass<ODocument> oDocuments = graph.getRawGraph().browseClass(getEntityClass().getSimpleName());
    for (ODocument oDocument : oDocuments) {
      entities.add(fromDoc(oDocument));
    }
    return entities;
  }

  public abstract OTypeHolder<T> getHolder();
}
