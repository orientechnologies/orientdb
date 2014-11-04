package com.orientechnologies.website.services.impl;

import com.orientechnologies.website.model.schema.HasComment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.model.schema.dto.Comment;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.services.IssueService;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * Created by Enrico Risa on 27/10/14.
 */
@Service
public class IssueServiceImpl implements IssueService {

  @Autowired
  private OrientDBFactory dbFactory;

  @Override
  public void commentIssue(Issue issue, Comment comment) {
    createCommentRelationship(issue, comment);
  }

  private void createCommentRelationship(Issue issue, Comment comment) {

    OrientGraph graph = dbFactory.getGraph();
    OrientVertex orgVertex = new OrientVertex(graph, new ORecordId(issue.getId()));
    OrientVertex devVertex = new OrientVertex(graph, new ORecordId(comment.getId()));
    orgVertex.addEdge(HasComment.class.getSimpleName(), devVertex);
  }
}
