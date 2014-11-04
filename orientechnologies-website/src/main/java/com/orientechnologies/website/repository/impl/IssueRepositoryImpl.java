package com.orientechnologies.website.repository.impl;

import com.orientechnologies.website.model.schema.OIssue;
import com.orientechnologies.website.model.schema.OSiteSchema;
import com.orientechnologies.website.model.schema.OTypeHolder;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.repository.IssueRepository;
import org.springframework.stereotype.Repository;

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
}
