package com.orientechnologies.website.repository.impl;

import java.util.List;
import java.util.NoSuchElementException;

import org.springframework.stereotype.Repository;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.website.model.schema.OSiteSchema;
import com.orientechnologies.website.model.schema.dto.Comment;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.repository.CommentRepository;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;

/**
 * Created by Enrico Risa on 27/10/14.
 */
@Repository
public class CommentRepositoryImpl extends OrientBaseRepository<Comment> implements CommentRepository {

  @Override
  public OSiteSchema.OTypeHolder<Comment> getHolder() {
    return OSiteSchema.Comment.BODY;
  }

  @Override
  public Class<Comment> getEntityClass() {
    return Comment.class;
  }

  @Override
  public Comment findByIssueAndCommentId(Issue issue, int id) {

    OrientGraph graph = dbFactory.getGraph();
    String query = String.format("select from (select expand(out('%s')) from %s) where %s = %d",
        OSiteSchema.HasComment.class.getSimpleName(), issue.getId(), OSiteSchema.Comment.COMMENT_ID, id);

    List<ODocument> vertexes = graph.getRawGraph().query(new OSQLSynchQuery<Object>(query));

    try {
      return fromDoc(vertexes.iterator().next());
    } catch (NoSuchElementException e) {
      return null;
    }

  }
}
