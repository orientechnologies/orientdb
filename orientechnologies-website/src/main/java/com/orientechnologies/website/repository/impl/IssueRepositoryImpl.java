package com.orientechnologies.website.repository.impl;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.website.model.schema.OIssue;
import com.orientechnologies.website.model.schema.OTypeHolder;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.repository.IssueRepository;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Enrico Risa on 24/10/14.
 */
@Repository
public class IssueRepositoryImpl extends OrientBaseRepository<Issue> implements IssueRepository {

  @Override
  public OTypeHolder<Issue> getHolder() {
    return OIssue.CREATED_AT;
  }

  @Override
  public Class<Issue> getEntityClass() {
    return Issue.class;
  }

  @Override
  public List<OUser> findInvolvedActors(Issue issue) {

    String rid = issue.getId();
    OrientGraph graph = dbFactory.getGraph();
    String query = String
        .format(
            "select  expand(set(user)) from (select unionAll( in('HasOpened'),out('HasEvent')[@class = 'Comment'].user,in('HasIssue').in('HasRepo').out('HasRepo').out('HasMember')) as user from %s )",
            rid);
    List<OIdentifiable> vertexes = graph.getRawGraph().query(new OSQLSynchQuery<Object>(query));
    List<OUser> users = new ArrayList<OUser>();
    for (OIdentifiable vertex : vertexes) {
      ODocument doc = vertex.getRecord();
      users.add(com.orientechnologies.website.model.schema.OUser.ID.fromDoc(doc, graph));
    }
    return users;
  }

  @Override
  public List<OUser> findToNotifyActors(Issue issue) {
    String rid = issue.getId();
    OrientGraph graph = dbFactory.getGraph();
    String query = String
        .format(
            "select  expand(set(user)) from (select unionAll( in('HasOpened'),out('IsAssigned'),out('HasEvent')[@class = 'Comment'].user) as user from %s )",
            rid);
    List<OIdentifiable> vertexes = graph.getRawGraph().query(new OSQLSynchQuery<Object>(query));
    List<OUser> users = new ArrayList<OUser>();
    for (OIdentifiable vertex : vertexes) {
      ODocument doc = vertex.getRecord();
      users.add(com.orientechnologies.website.model.schema.OUser.ID.fromDoc(doc, graph));
    }
    return users;
  }

  @Override
  public List<OUser> findToNotifyActorsWatching(Issue issue) {

    OrientGraph graph = dbFactory.getGraph();

    List<OUser> users = new ArrayList<OUser>();
    if (!Boolean.TRUE.equals(issue.getConfidential())) {
      String query = String.format("select from OUser where watching = true");
      List<OIdentifiable> vertexes = graph.getRawGraph().query(new OSQLSynchQuery<Object>(query));
      for (OIdentifiable vertex : vertexes) {
        ODocument doc = vertex.getRecord();
        users.add(com.orientechnologies.website.model.schema.OUser.ID.fromDoc(doc, graph));
      }
    }
    return users;
  }
}
