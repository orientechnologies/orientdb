package com.orientechnologies.website.services.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.events.IssueCreatedEvent;
import com.orientechnologies.website.github.GIssue;
import com.orientechnologies.website.github.GRepo;
import com.orientechnologies.website.github.GitHub;
import com.orientechnologies.website.helper.SecurityHelper;
import com.orientechnologies.website.model.schema.dto.*;
import com.orientechnologies.website.model.schema.dto.web.IssueDTO;
import com.orientechnologies.website.services.RepositoryService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
  public Issue openIssue(Repository repository, final IssueDTO issue) {

    GitHub github = new GitHub(SecurityHelper.currentToken(), repositoryService.gitHubConfiguration);

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
      repositoryService.issueService.changeUser(i, SecurityHelper.currentUser());
      repositoryService.createIssue(repository, i);
      if (repositoryService.userService.isTeamMember(SecurityHelper.currentUser(), repository)) {
        repositoryService.handleMilestone(repository, i, issue.getMilestone());
      }
      repositoryService.handleEnvironment(repository, i, issue.getEnvironment());
      repositoryService.handleScope(repository, i, issue.getScope());
      repositoryService.handleVersion(repository, i, issue.getVersion());
      repositoryService.handleClient(repository, i, issue.getClient());
      repositoryService.handlePriority(repository, i, issue.getPriority());
      Issue issue1 = repositoryService.issueRepository.save(i);
      repositoryService.eventManager.pushInternalEvent(IssueCreatedEvent.EVENT, issue1);
      List<OUser> bots = repositoryService.organizationRepo.findBots(repository.getOrganization().getName());
      if (bots.size() > 0 && issue.getType() != null) {
        repositoryService.issueService.addLabels(issue1, new ArrayList<String>() {
          {
            add(issue.getType());
          }
        }, bots.iterator().next(), false, true);
      }
      return issue1;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public Issue patchIssue(Issue original, OUser user, IssueDTO patch) {

    String token = user != null ? user.getToken() : SecurityHelper.currentToken();
    GitHub github = new GitHub(token, repositoryService.gitHubConfiguration);
    ODocument doc = new ODocument();

    String iPropertyValue = original.getRepository().getOrganization().getName() + "/" + original.getRepository().getName();
    doc.field("full_name", iPropertyValue);
    try {
      GRepo repo = github.repo(iPropertyValue, doc.toJSON());
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode node = mapper.createObjectNode();
      if (patch.getMilestone() != null) {
        node.put("milestone", patch.getMilestone());
      }
      if (patch.getState() != null) {
        node.put("state", patch.getState());

        if (fireLogging(user, original)) {
          OUser userByLogin = repositoryService.userRepo.findUserByLogin(patch.getAssignee());
          String evt = patch.getState().equalsIgnoreCase(Issue.IssueState.OPEN.toString()) ? "reopened" : "closed";
          repositoryService.eventService.fireEvent(original, SecurityHelper.currentUser(), evt, userByLogin, null);
        }
      }
      if (patch.getTitle() != null) {
        node.put("title", patch.getTitle());
      }
      if (patch.getBody() != null) {
        node.put("body", patch.getBody());
      }
      if (patch.getAssignee() != null) {
        node.put("assignee", patch.getAssignee());
        if (fireLogging(user, original)) {
          OUser userByLogin = repositoryService.userRepo.findUserByLogin(patch.getAssignee());
          repositoryService.eventService.fireEvent(original, SecurityHelper.currentUser(), "assigned", userByLogin, null);
        }
      }
      if (node.size() > 0) {
        String value = mapper.writeValueAsString(node);
        repo.patchIssue(original.getNumber(), value);
      }

    } catch (IOException e) {

    }
    return original;
  }

  protected boolean fireLogging(OUser user, Issue issue) {
    return user != null ? repositoryService.securityManager.isSupport(user, issue.getRepository().getOrganization().getName())
        : false;
  }

  @Override
  public void addLabel(Repository repo, Label label) {

  }

  @Override
  public void addMilestone(Repository repoDtp, Milestone m) {

  }

  @Override
  public void syncRepository(Repository repository) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void escalateIssue(Issue i) {
    throw new UnsupportedOperationException();
  }
}
