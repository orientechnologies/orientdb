package com.orientechnologies.website.repository.impl;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.website.model.schema.*;
import com.orientechnologies.website.model.schema.dto.Environment;
import com.orientechnologies.website.repository.EnvironmentRepository;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * Created by Enrico Risa on 05/12/14.
 */
@Repository
public class EnvironmentRepositoryImpl extends OrientBaseRepository<Environment> implements EnvironmentRepository {

  @Override
  public OTypeHolder<Environment> getHolder() {

    return OEnvironment.NAME;
  }

  @Override
  public Class<Environment> getEntityClass() {
    return Environment.class;
  }

  @Override
  public Environment findById(Long environmentId) {
    OrientGraph graph = dbFactory.getGraph();

    String query = String.format("select from Environment where eid = %d", environmentId);
    List<ODocument> vertexes = graph.getRawGraph().query(new OSQLSynchQuery<Object>(query));
    try {
      return fromDoc(vertexes.iterator().next());
    } catch (NoSuchElementException e) {
      return null;
    }
  }
}
