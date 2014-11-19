package com.orientechnologies.website.repository.impl;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.model.schema.*;
import com.orientechnologies.website.model.schema.dto.*;
import com.orientechnologies.website.model.schema.dto.User;
import com.orientechnologies.website.repository.OrganizationRepository;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;

@Repository
public class OrganizationRepositoryImpl extends OrientBaseRepository<Organization> implements OrganizationRepository {

  @Autowired
  private OrientDBFactory dbFactory;

  @Override
  public Organization findOneByName(String name) {

    OrientGraph db = dbFactory.getGraph();
    String query = String.format("select from %s where name = '%s'", getEntityClass().getSimpleName(), name);
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
    String query = String.format("select expand(out('HasRepo').out('HasIssue')) from Organization where name = '%s'", name);
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
    String query = String.format("select expand(out('HasRepo')) from Organization where name = '%s'", name);
    Iterable<OrientVertex> vertices = db.command(new OCommandSQL(query)).execute();

    List<com.orientechnologies.website.model.schema.dto.Repository> issues = new ArrayList<com.orientechnologies.website.model.schema.dto.Repository>();
    for (OrientVertex vertice : vertices) {
      ODocument doc = vertice.getRecord();
      issues.add(ORepository.NAME.fromDoc(doc, db));
    }
    return issues;
  }

  @Override
  public com.orientechnologies.website.model.schema.dto.Repository findOrganizationRepository(String name, String repo) {
    OrientGraph db = dbFactory.getGraph();
    String query = String.format("select expand(out('HasRepo')[name = '%s']) from Organization where name = '%s'", repo, name);
    Iterable<OrientVertex> vertices = db.command(new OCommandSQL(query)).execute();

    try {
      ODocument doc = vertices.iterator().next().getRecord();
      return ORepository.NAME.fromDoc(doc, db);
    } catch (NoSuchElementException e) {
      return null;
    }

  }

  @Override
  public Issue findSingleOrganizationIssueByRepoAndNumber(String name, String repo, String number) {

    OrientGraph db = dbFactory.getGraph();
    String query = String.format(
        "select expand(out('HasRepo')[name = '%s'].out('HasIssue')[uuid = '%s'])  from Organization where name = '%s') ", repo,
        number, name);

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

    OrientVertex vertex = db.getVertex(new ORecordId(issue.getId()));

    List<Comment> comments = new ArrayList<Comment>();
    for (Vertex vertex1 : vertex.getVertices(Direction.OUT, HasEvent.class.getSimpleName())) {
      OrientVertex v = (OrientVertex) vertex1;
      comments.add(OComment.COMMENT_ID.fromDoc(v.getRecord(), db));
    }

    return comments;
  }

  @Override
  public List<Event> findEventsByOwnerRepoAndIssueNumber(String owner, String repo, String number) {
    OrientGraph db = dbFactory.getGraph();
    Issue issue = findSingleOrganizationIssueByRepoAndNumber(owner, repo, number);

    if (issue == null) {
      return null;
    }
    OrientVertex vertex = db.getVertex(new ORecordId(issue.getId()));

    List<Event> events = new ArrayList<Event>();
    for (Vertex vertex1 : vertex.getVertices(Direction.OUT, HasEvent.class.getSimpleName())) {
      OrientVertex v = (OrientVertex) vertex1;
      events.add(OEvent.CREATED_AT.fromDoc(v.getRecord(), db));
    }
    return events;
  }

  @Override
  public List<User> findTeamMembers(String owner, String repo) {

    // Todo Change the query when the team concept is introduced to repository
    OrientGraph db = dbFactory.getGraph();
    String query = String.format("select expand(out('HasMember')) from Organization where name = '%s'", owner);
    Iterable<OrientVertex> vertices = db.command(new OCommandSQL(query)).execute();

    List<User> users = new ArrayList<User>();
    for (OrientVertex vertice : vertices) {
      ODocument doc = vertice.getRecord();
      users.add(com.orientechnologies.website.model.schema.OUser.NAME.fromDoc(doc, db));
    }
    return users;
  }

  @Override
  public Milestone findMilestoneByOwnerRepoAndNumberIssueAndNumberMilestone(String owner, String repo, Integer iNumber,
      Integer mNumber) {
    OrientGraph db = dbFactory.getGraph();
    String query = String
        .format(
            "select expand(out('HasRepo')[name = '%s'].out('HasIssue')[uuid = %d].out('HasMilestone')[number = %d])  from Organization  where name = '%s')",
            repo, iNumber, mNumber, owner);

    Iterable<OrientVertex> vertices = db.command(new OCommandSQL(query)).execute();
    try {
      ODocument doc = vertices.iterator().next().getRecord();
      return OMilestone.NUMBER.fromDoc(doc, db);
    } catch (NoSuchElementException e) {
      return null;
    }
  }

  @Override
  public List<Milestone> findRepoMilestones(String owner, String repo) {
    OrientGraph db = dbFactory.getGraph();
    String query = String.format(
        "select expand(out('HasRepo')[name = '%s'].out('HasMilestone'))   from Organization  where name = '%s')", repo, owner);

    Iterable<OrientVertex> vertices = db.command(new OCommandSQL(query)).execute();
    List<Milestone> milestones = new ArrayList<Milestone>();
    for (OrientVertex vertice : vertices) {
      ODocument doc = vertice.getRecord();
      milestones.add(OMilestone.NUMBER.fromDoc(doc, db));
    }
    return milestones;
  }

  @Override
  public List<Label> findRepoLabels(String owner, String repo) {
    OrientGraph db = dbFactory.getGraph();
    String query = String.format(
        "select expand(out('HasRepo')[name = '%s'].out('HasLabel'))   from Organization  where name = '%s')", repo, owner);

    Iterable<OrientVertex> vertices = db.command(new OCommandSQL(query)).execute();
    List<Label> labels = new ArrayList<Label>();
    for (OrientVertex vertice : vertices) {
      ODocument doc = vertice.getRecord();
      labels.add(OLabel.NAME.fromDoc(doc, db));
    }
    return labels;
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
