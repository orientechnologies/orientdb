package com.orientechnologies.website.services.impl;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.model.schema.HasIssue;
import com.orientechnologies.website.model.schema.HasLabel;
import com.orientechnologies.website.model.schema.HasMilestone;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.Label;
import com.orientechnologies.website.model.schema.dto.Milestone;
import com.orientechnologies.website.model.schema.dto.Repository;
import com.orientechnologies.website.repository.RepositoryRepository;
import com.orientechnologies.website.services.RepositoryService;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by Enrico Risa on 21/10/14.
 */
@Service
public class RepositoryServiceImpl implements RepositoryService {

  @Autowired
  private OrientDBFactory        dbFactory;
  @Autowired
  protected RepositoryRepository repositoryRepository;

  @Override
  public Repository createRepo(String name, String description) {
    Repository repo = new Repository();
    repo.setName(name);
    repo.setDescription(description);
    return repositoryRepository.save(repo);
  }

  @Override
  public void createIssue(Repository repository, Issue issue) {
    createHasIssueRelationship(repository, issue);
  }

  @Override
  public void addLabel(Repository repo, Label label) {

    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = new OrientVertex(graph, new ORecordId(repo.getId()));
    OrientVertex devVertex = new OrientVertex(graph, new ORecordId(label.getId()));
    orgVertex.addEdge(HasLabel.class.getSimpleName(), devVertex);
  }

  @Override
  public void addMilestone(Repository repo, Milestone milestone) {
    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = new OrientVertex(graph, new ORecordId(repo.getId()));
    OrientVertex devVertex = new OrientVertex(graph, new ORecordId(milestone.getId()));
    orgVertex.addEdge(HasMilestone.class.getSimpleName(), devVertex);
  }

  private void createHasIssueRelationship(Repository repository, Issue issue) {

    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = new OrientVertex(graph, new ORecordId(repository.getId()));
    OrientVertex devVertex = new OrientVertex(graph, new ORecordId(issue.getId()));
    orgVertex.addEdge(HasIssue.class.getSimpleName(), devVertex);
  }
}
