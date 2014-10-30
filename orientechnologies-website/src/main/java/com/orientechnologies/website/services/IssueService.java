package com.orientechnologies.website.services;

import com.orientechnologies.website.model.schema.dto.Comment;
import com.orientechnologies.website.model.schema.dto.Issue;

/**
 * Created by Enrico Risa on 24/10/14.
 */
public interface IssueService {

  public void commentIssue(Issue issue, Comment comment);
}
