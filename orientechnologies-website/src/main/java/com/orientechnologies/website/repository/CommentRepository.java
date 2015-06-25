package com.orientechnologies.website.repository;

import com.orientechnologies.website.model.schema.dto.Comment;
import com.orientechnologies.website.model.schema.dto.Issue;

/**
 * Created by Enrico Risa on 27/10/14.
 */
public interface CommentRepository extends BaseRepository<Comment> {
  public Comment findByIssueAndCommentId(Issue issue, int id);

  public Comment findByIssueAndCommentUUID(Issue issue, String uuid);

  public Issue findIssueByComment(Comment comment);

}
