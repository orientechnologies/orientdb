package com.orientechnologies.website.repository.impl;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.website.model.schema.HasEvent;
import com.orientechnologies.website.model.schema.OComment;
import com.orientechnologies.website.model.schema.OIssue;
import com.orientechnologies.website.model.schema.OTypeHolder;
import com.orientechnologies.website.model.schema.dto.Comment;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.repository.CommentRepository;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * Created by Enrico Risa on 27/10/14.
 */
@Repository
public class CommentRepositoryImpl extends OrientBaseRepository<Comment> implements CommentRepository {

  @Override
  public OTypeHolder<Comment> getHolder() {
    return OComment.BODY;
  }

  @Override
  public Class<Comment> getEntityClass() {
    return Comment.class;
  }

  @Override
  public Comment findByIssueAndCommentId(Issue issue, int id) {

    OrientGraph graph = dbFactory.getGraph();
    String query = String.format("select from (select expand(out('%s')) from %s) where %s = %d", HasEvent.class.getSimpleName(),
        issue.getId(), OComment.COMMENT_ID, id);

    List<ODocument> vertexes = graph.getRawGraph().query(new OSQLSynchQuery<Object>(query));

    try {
      return fromDoc(vertexes.iterator().next());
    } catch (NoSuchElementException e) {
      return null;
    }

  }

  @Override
  public Comment findByIssueAndCommentUUID(Issue issue, String uuid) {
    OrientGraph graph = dbFactory.getGraph();
    String query = String.format("select from (select expand(out('%s')) from %s) where %s = '%s'", HasEvent.class.getSimpleName(),
        issue.getId(), OComment.UUID, uuid);
    List<ODocument> vertexes = graph.getRawGraph().query(new OSQLSynchQuery<Object>(query));

    try {
      return fromDoc(vertexes.iterator().next());
    } catch (NoSuchElementException e) {
      return null;
    }

  }

  @Override
  public Issue findIssueByComment(Comment comment) {
    OrientGraph graph = dbFactory.getGraph();

    Vertex commVertex = graph.getVertex(new ORecordId(comment.getId()));

    Iterable<Vertex> vertices = commVertex.getVertices(Direction.IN, HasEvent.class.getSimpleName());

    try {
      OrientVertex v = (OrientVertex) vertices.iterator().next();
      return OIssue.NUMBER.fromDoc(v.getRecord(), graph);
    } catch (NoSuchElementException e) {
      return null;
    }
  }

}
