package com.orientechnologies.website.repository;

import com.orientechnologies.website.model.schema.dto.*;

import java.util.List;

/**
 * Created by Enrico Risa on 21/10/14.
 */
public interface RepositoryRepository extends BaseRepository<Repository> {

  public Repository findByOrgAndName(String org, String name);

  public Issue findIssueByRepoAndNumber(String repo, Integer number);

  public Event findIssueEventByRepoAndNumberAndEventNumber(String repo, Integer iNumber, Integer eNumber);

  public List<Label> findLabelsByRepo(String repo);

  public Label findLabelsByRepoAndName(String repo, String name);

  public List<Milestone> findMilestoneByRepo(String repo);

  public Milestone findMilestoneByRepoAndName(String repo, Integer number);

  public Scope findScope(String name, Integer scope);
}
