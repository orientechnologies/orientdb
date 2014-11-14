package com.orientechnologies.website.services.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.github.GIssue;
import com.orientechnologies.website.github.GRepo;
import com.orientechnologies.website.github.GitHub;
import com.orientechnologies.website.helper.SecurityHelper;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.Label;
import com.orientechnologies.website.model.schema.dto.Milestone;
import com.orientechnologies.website.model.schema.dto.Repository;
import com.orientechnologies.website.model.schema.dto.web.IssueDTO;
import com.orientechnologies.website.services.RepositoryService;

import java.io.IOException;

/**
 * Created by Enrico Risa on 14/11/14.
 */
public class RepositoryServiceGithub implements RepositoryService {

  private RepositoryServiceImpl repositoryService;

  public RepositoryServiceGithub(RepositoryServiceImpl repositoryService) {
    this.repositoryService = repositoryService;
  }

  @Override
  public Repository createRepo(String name, String description) {
    return null;
  }

  @Override
  public void createIssue(Repository repo, Issue issue) {

  }

  @Override
  public Issue openIssue(Repository repository, IssueDTO issue) {

    GitHub github = new GitHub(SecurityHelper.currentToken());

    ODocument doc = new ODocument();

    String iPropertyValue = repository.getOrganization().getName() + "/" + repository.getName();
    doc.field("full_name", iPropertyValue);
    try {
      GRepo repo = github.repo(iPropertyValue, doc.toJSON());
      ObjectMapper mapper = new ObjectMapper();

      String value = mapper.writeValueAsString(issue);

      GIssue gIssue = repo.openIssue(value);

      Issue i = new Issue();
      i.setCreatedAt(gIssue.getCreatedAt());
      i.setTitle(gIssue.getTitle());
      i.setBody(gIssue.getBody());
      i.setNumber(gIssue.getNumber());
      i.setConfidential(false);
      i.setState(gIssue.getState().name());
      i = repositoryService.issueRepository.save(i);
      repositoryService.createIssue(repository, i);
      return i;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public Issue patchIssue(Issue original, IssueDTO patch) {
    GitHub github = new GitHub(SecurityHelper.currentToken());
    return null;
  }

  @Override
  public void addLabel(Repository repo, Label label) {

  }

  @Override
  public void addMilestone(Repository repoDtp, Milestone m) {

  }
}
