package com.orientechnologies.website.services;

import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.Label;
import com.orientechnologies.website.model.schema.dto.Milestone;
import com.orientechnologies.website.model.schema.dto.Repository;
import com.orientechnologies.website.model.schema.dto.web.IssueDTO;
import org.springframework.transaction.annotation.Transactional;

public interface RepositoryService {

  public Repository createRepo(String name, String description);

  public void createIssue(Repository repo, Issue issue);

  public Issue openIssue(Repository repository, IssueDTO issue);

  public Issue patchIssue(Issue original,OUser user, IssueDTO patch);

  public void addLabel(Repository repo, Label label);

  public void addMilestone(Repository repoDtp, Milestone m);

  public void syncRepository(Repository repository);

  @Transactional
  void escalateIssue(Issue i);
}
