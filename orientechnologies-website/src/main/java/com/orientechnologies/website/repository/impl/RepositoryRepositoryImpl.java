package com.orientechnologies.website.repository.impl;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.website.model.schema.*;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.Repository;
import com.orientechnologies.website.repository.RepositoryRepository;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * Created by Enrico Risa on 21/10/14.
 */
@org.springframework.stereotype.Repository
public class RepositoryRepositoryImpl extends OrientBaseRepository<Repository> implements RepositoryRepository {

  @Override
  public OTypeHolder<Repository> getHolder() {
    return ORepository.NAME;
  }

  @Override
  public Class<Repository> getEntityClass() {
    return Repository.class;
  }

  @Override
  public Repository findByOrgAndName(String org, String name) {

    OrientGraph graph = dbFactory.getGraph();

    String query = String.format("select from %s where %s = '%s' and in('%s')[0].%s = '%s'", getEntityClass().getSimpleName(),
        ORepository.CODENAME, name, HasRepo.class.getSimpleName(), OOrganization.CODENAME, org);
    List<ODocument> vertexes = graph.getRawGraph().query(new OSQLSynchQuery<Object>(query));

    try {
      return fromDoc(vertexes.iterator().next());
    } catch (NoSuchElementException e) {
      return null;
    }

  }

  @Override
  public Issue findIssueByRepoAndNumber(String repo, Integer number) {

    OrientGraph graph = dbFactory.getGraph();
    String query = String.format("select from (select expand(out('%s')) from %s where %s = '%s') where %s = %d",
        HasIssue.class.getSimpleName(), getEntityClass().getSimpleName(), ORepository.CODENAME, repo, OIssue.NUMBER, number);
    List<ODocument> vertexes = graph.getRawGraph().query(new OSQLSynchQuery<Object>(query));

    try {
      return OIssue.NUMBER.fromDoc(vertexes.iterator().next(), graph);
    } catch (NoSuchElementException e) {
      return null;
    }
  }
}
