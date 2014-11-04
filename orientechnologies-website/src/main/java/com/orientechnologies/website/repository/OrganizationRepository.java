package com.orientechnologies.website.repository;

import com.orientechnologies.website.model.schema.dto.Comment;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.Organization;
import com.orientechnologies.website.model.schema.dto.Repository;

import java.util.List;

public interface OrganizationRepository extends BaseRepository<Organization> {

  public Organization findOneByName(String name);

  public List<Issue> findOrganizationIssues(String name);

  public List<Repository> findOrganizationRepositories(String name);

  public Issue findSingleOrganizationIssueByRepoAndNumber(String name, String repo, String number);

  public List<Comment> findSingleOrganizationIssueCommentByRepoAndNumber(String owner, String repo, String number);
}
