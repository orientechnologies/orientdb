package com.orientechnologies.website.services.impl;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.model.schema.HasIssue;
import com.orientechnologies.website.model.schema.dto.Issue;
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
    repo.setCodename(name);
    repo.setName(description);
    return repositoryRepository.save(repo);
  }

  @Override
  public void createIssue(Repository repository, Issue issue) {
    createHasIssueRelationship(repository, issue);
  }

  private void createHasIssueRelationship(Repository repository, Issue issue) {

    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = new OrientVertex(graph, new ORecordId(repository.getId()));
    OrientVertex devVertex = new OrientVertex(graph, new ORecordId(issue.getId()));
    orgVertex.addEdge(HasIssue.class.getSimpleName(), devVertex);
  }
}
