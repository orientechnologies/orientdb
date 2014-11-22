package com.orientechnologies.website.repository;

import com.orientechnologies.website.model.schema.dto.*;

import java.util.List;

public interface OrganizationRepository extends BaseRepository<Organization> {

  public Organization findOneByName(String name);

  public List<Issue> findOrganizationIssues(String name, String q);

  public List<Repository> findOrganizationRepositories(String name);

  public List<Client> findClients(String name);

  public Client findClient(String name, Integer clientId);

  public Repository findOrganizationRepository(String name, String repo);

  public Issue findSingleOrganizationIssueByRepoAndNumber(String name, String repo, String number);

  public List<Comment> findSingleOrganizationIssueCommentByRepoAndNumber(String owner, String repo, String number);

  public List<Event> findEventsByOwnerRepoAndIssueNumber(String owner, String repo, String number);

  public List<OUser> findClientMembers(String org, Integer clientId);

  public List<OUser> findTeamMembers(String owner, String repo);

  public Milestone findMilestoneByOwnerRepoAndNumberIssueAndNumberMilestone(String owner, String repo, Integer iNumber,
      Integer mNumber);

  public List<Milestone> findRepoMilestones(String owner, String repo);

  public List<Label> findRepoLabels(String owner, String repo);
}
