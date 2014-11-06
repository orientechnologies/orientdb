package com.orientechnologies.website.services;

import com.orientechnologies.website.model.schema.dto.*;

import java.util.List;

/**
 * Created by Enrico Risa on 24/10/14.
 */
public interface IssueService {

  public void commentIssue(Issue issue, Comment comment);

  public void changeMilestone(Issue issue, Milestone milestone);

  public void changeLabels(Issue issue, List<Label> labels);

  public void fireEvent(Issue issueDto, Event e);
}
