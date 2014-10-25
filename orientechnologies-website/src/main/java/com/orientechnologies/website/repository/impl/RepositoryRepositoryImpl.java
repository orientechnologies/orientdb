package com.orientechnologies.website.repository.impl;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.website.model.schema.OSiteSchema;
import com.orientechnologies.website.model.schema.dto.Repository;
import com.orientechnologies.website.repository.RepositoryRepository;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * Created by Enrico Risa on 21/10/14.
 */
@org.springframework.stereotype.Repository
public class RepositoryRepositoryImpl extends OrientBaseRepository<Repository> implements RepositoryRepository {

  @Override
  public OSiteSchema.OTypeHolder<Repository> getHolder() {
    return OSiteSchema.Repository.NAME;
  }

  @Override
  public Class<Repository> getEntityClass() {
    return Repository.class;
  }

  @Override
  public Repository findByOrgAndName(String org, String name) {

    OrientGraph graph = dbFactory.getGraph();

    String query = String.format("select from %s where %s = '%s' and in('%s')[0].%s = '%s'", getEntityClass().getSimpleName(),
        OSiteSchema.Repository.CODENAME, name, OSiteSchema.HasRepo.class.getSimpleName(), OSiteSchema.Organization.CODENAME, org);
    List<ODocument> vertexes = graph.getRawGraph().query(new OSQLSynchQuery<Object>(query));

    try {
      return fromDoc(vertexes.iterator().next());
    } catch (NoSuchElementException e) {
      return null;
    }

  }
}
