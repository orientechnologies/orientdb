package com.orientechnologies.website.repository.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;

import com.orientechnologies.website.model.schema.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.model.schema.dto.Comment;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.Organization;
import com.orientechnologies.website.repository.OrganizationRepository;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

@Repository
public class OrganizationRepositoryImpl extends OrientBaseRepository<Organization> implements OrganizationRepository {

  @Autowired
  private OrientDBFactory dbFactory;

  @Override
  public Organization findOneByName(String name) {

    OrientGraph db = dbFactory.getGraph();
    String query = String.format("select from %s where codename = '%s'", getEntityClass().getSimpleName(), name);
    Iterable<OrientVertex> vertices = db.command(new OCommandSQL(query)).execute();

    try {
      ODocument doc = vertices.iterator().next().getRecord();

      return fromDoc(doc);
    } catch (NoSuchElementException e) {
      return null;
    }

  }

  @Override
  public List<Issue> findOrganizationIssues(String name) {
    OrientGraph db = dbFactory.getGraph();
    String query = String.format("select expand(out('HasRepo').out('HasIssue')) from Organization where codename = '%s'", name);
    Iterable<OrientVertex> vertices = db.command(new OCommandSQL(query)).execute();

    List<Issue> issues = new ArrayList<Issue>();
    for (OrientVertex vertice : vertices) {
      ODocument doc = vertice.getRecord();
      issues.add(OIssue.NUMBER.fromDoc(doc, db));
    }
    return issues;
  }

  @Override
  public List<com.orientechnologies.website.model.schema.dto.Repository> findOrganizationRepositories(String name) {
    OrientGraph db = dbFactory.getGraph();
    String query = String.format("select expand(out('HasRepo')) from Organization where codename = '%s'", name);
    Iterable<OrientVertex> vertices = db.command(new OCommandSQL(query)).execute();

    List<com.orientechnologies.website.model.schema.dto.Repository> issues = new ArrayList<com.orientechnologies.website.model.schema.dto.Repository>();
    for (OrientVertex vertice : vertices) {
      ODocument doc = vertice.getRecord();
      issues.add(ORepository.CODENAME.fromDoc(doc, db));
    }
    return issues;
  }

  @Override
  public Issue findSingleOrganizationIssueByRepoAndNumber(String name, String repo, String number) {

    OrientGraph db = dbFactory.getGraph();
    String query = String
        .format(
            "select from (select expand(out('HasIssue')) from (select expand(out('HasRepo')) from Organization where codename = '%s') where codename = '%s') where number = '%s'",
            name, repo, number);

    Iterable<OrientVertex> vertices = db.command(new OCommandSQL(query)).execute();
    try {
      ODocument doc = vertices.iterator().next().getRecord();
      return OIssue.NUMBER.fromDoc(doc, db);
    } catch (NoSuchElementException e) {
      return null;
    }
  }

  @Override
  public List<Comment> findSingleOrganizationIssueCommentByRepoAndNumber(String owner, String repo, String number) {

    OrientGraph db = dbFactory.getGraph();
    Issue issue = findSingleOrganizationIssueByRepoAndNumber(owner, repo, number);

    OrientVertex vertex = new OrientVertex(db, new ORecordId(issue.getId()));

    List<Comment> comments = new ArrayList<Comment>();
    for (Vertex vertex1 : vertex.getVertices(Direction.OUT, HasComment.class.getSimpleName())) {
      OrientVertex v = (OrientVertex) vertex1;
      comments.add(OComment.COMMENT_ID.fromDoc(v.getRecord(), db));
    }

    return comments;
  }

  @Override
  public Organization save(Organization entity) {

    OrientGraph db = dbFactory.getGraph();
    ODocument doc = db.getRawGraph().save(toDoc(entity));
    return fromDoc(doc);
  }

  @Override
  public void save(Collection<Organization> entities) {

  }

  @Override
  public OTypeHolder<Organization> getHolder() {
    return OOrganization.NAME;
  }

  @Override
  public Class<Organization> getEntityClass() {
    return Organization.class;
  }

}
