package com.orientechnologies.website.repository.impl;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.website.model.schema.*;
import com.orientechnologies.website.model.schema.dto.*;
import com.orientechnologies.website.repository.RepositoryRepository;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.util.ArrayList;
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
        ORepository.NAME, name, HasRepo.class.getSimpleName(), OOrganization.NAME, org);
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
        HasIssue.class.getSimpleName(), getEntityClass().getSimpleName(), ORepository.NAME, repo, OIssue.NUMBER, number);
    List<ODocument> vertexes = graph.getRawGraph().query(new OSQLSynchQuery<Object>(query));

    try {
      return OIssue.NUMBER.fromDoc(vertexes.iterator().next(), graph);
    } catch (NoSuchElementException e) {
      return null;
    }
  }

  @Override
  public Event findIssueEventByRepoAndNumberAndEventNumber(String repo, Integer iNumber, Integer eNumber) {
    OrientGraph graph = dbFactory.getGraph();
    String query = String.format(
        "select expand(out('HasIssue')[number = %d].out('HasEvent')[event_id = %d]) from Repository where name = '%s'", iNumber,
        eNumber, repo);
    List<ODocument> vertexes = graph.getRawGraph().query(new OSQLSynchQuery<Object>(query));

    try {
      return OEvent.CREATED_AT.fromDoc(vertexes.iterator().next(), graph);
    } catch (NoSuchElementException e) {
      return null;
    }
  }

  @Override
  public List<Label> findLabelsByRepo(String repo) {
    OrientGraph db = dbFactory.getGraph();
    String query = String.format("select expand(out('HasLabel')) from Repository  where name = '%s')", repo);

    List<Label> labels = new ArrayList<Label>();
    Iterable<OrientVertex> vertices = db.command(new OCommandSQL(query)).execute();
    for (OrientVertex vertice : vertices) {
      labels.add(OLabel.NAME.fromDoc(vertice.getRecord(), db));
    }
    return labels;
  }

  @Override
  public Label findLabelsByRepoAndName(String repo, String name) {
    OrientGraph db = dbFactory.getGraph();
    String query = String.format("select expand(out('HasLabel')[name = '%s']) from Repository  where name = '%s')", name, repo);

    Iterable<OrientVertex> vertices = db.command(new OCommandSQL(query)).execute();
    try {
      return OLabel.NAME.fromDoc(vertices.iterator().next().getRecord(), db);
    } catch (NoSuchElementException e) {
      return null;
    }
  }

  @Override
  public List<Milestone> findMilestoneByRepo(String repo) {
    OrientGraph db = dbFactory.getGraph();
    String query = String.format("select expand(out('HasMilestone')) from Repository  where name = '%s')", repo);

    List<Milestone> labels = new ArrayList<Milestone>();
    Iterable<OrientVertex> vertices = db.command(new OCommandSQL(query)).execute();
    for (OrientVertex vertice : vertices) {
      labels.add(OMilestone.NUMBER.fromDoc(vertice.getRecord(), db));
    }
    return labels;
  }

  @Override
  public Milestone findMilestoneByRepoAndName(String repo, Integer number) {
    OrientGraph db = dbFactory.getGraph();
    String query = String.format("select expand(out('HasMilestone')[number = %d]) from Repository  where name = '%s')", number,
        repo);

    Iterable<OrientVertex> vertices = db.command(new OCommandSQL(query)).execute();
    try {
      return OMilestone.NUMBER.fromDoc(vertices.iterator().next().getRecord(), db);
    } catch (NoSuchElementException e) {
      return null;
    }
  }

  @Override
  public Scope findScope(String name, Integer scope) {
    OrientGraph db = dbFactory.getGraph();
    String query = String.format("select expand(out('HasScope')[number = %d]) from Repository  where name = '%s')", scope, name);

    Iterable<OrientVertex> vertices = db.command(new OCommandSQL(query)).execute();
    try {
      return OScope.NAME.fromDoc(vertices.iterator().next().getRecord(), db);
    } catch (NoSuchElementException e) {
      return null;
    }
  }
}
