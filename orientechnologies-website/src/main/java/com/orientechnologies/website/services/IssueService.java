package com.orientechnologies.website.services;

import com.orientechnologies.website.model.schema.dto.*;

import java.util.List;

/**
 * Created by Enrico Risa on 24/10/14.
 */
public interface IssueService {

  public void commentIssue(Issue issue, Comment comment);

  public Comment createNewCommentOnIssue(Issue issue, Comment comment);

  public void changeMilestone(Issue issue, Milestone milestone, OUser actor, boolean fire);

  public void changeLabels(Issue issue, List<Label> labels, boolean replace);

  public List<Label> addLabels(Issue issue, List<String> labels, OUser actor, boolean fire, boolean remote);

  public void removeLabel(Issue issue, String label, OUser actor, boolean remote);

  public void fireEvent(Issue issueDto, Event e);

  public void changeUser(Issue issue, OUser user);

  public void assign(Issue issue, OUser assignee, OUser actor, boolean fire);

  public void unassign(Issue issue, OUser assignee, OUser actor, boolean fire);

  public void changeVersion(Issue issue, Milestone milestone);

  public void changePriority(Issue issue, Priority priority);

  public Issue changeState(Issue issue, String state, OUser actor, boolean fire);

  public Issue synchIssue(Issue issue);

  public void clearEvents(Issue issue);

  public void changeScope(Issue issue, Scope scope);

  public void changeClient(Issue issue, Client client);

  public void changeEnvironment(Issue issue, Environment e);
}
