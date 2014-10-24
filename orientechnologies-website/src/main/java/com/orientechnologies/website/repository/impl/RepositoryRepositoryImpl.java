package com.orientechnologies.website.repository.impl;

import com.orientechnologies.website.model.schema.OSiteSchema;
import com.orientechnologies.website.model.schema.dto.Repository;
import com.orientechnologies.website.repository.RepositoryRepository;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;

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

    return null;
  }
}
