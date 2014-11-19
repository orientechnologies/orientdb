package com.orientechnologies.website.services;

import com.orientechnologies.website.model.schema.dto.*;

import java.util.List;

/**
 * Created by Enrico Risa on 24/10/14.
 */
public interface IssueService {

  public void commentIssue(Issue issue, Comment comment);

  public Comment createNewCommentOnIssue(Issue issue, Comment comment);

  public void changeMilestone(Issue issue, Milestone milestone,User actor, boolean fire);

  public void changeLabels(Issue issue, List<Label> labels, boolean replace);

  public List<Label> addLabels(Issue issue, List<String> labels, User actor, boolean fire);

  public void removeLabel(Issue issue, String label, User actor);

  public void fireEvent(Issue issueDto, Event e);

  public void changeUser(Issue issue, User user);

  public void changeAssignee(Issue issue, User assignee, User actor, boolean fire);

  public void changeVersion(Issue issue, Milestone milestone);

  public Issue changeState(Issue issue, String state);
}
