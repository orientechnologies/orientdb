package com.orientechnologies.website.daemon;

import com.orientechnologies.website.model.schema.dto.*;
import com.orientechnologies.website.model.schema.dto.web.IssueDTO;
import com.orientechnologies.website.repository.OrganizationRepository;
import com.orientechnologies.website.services.IssueService;
import com.orientechnologies.website.services.RepositoryService;
import com.orientechnologies.website.services.impl.IssueServiceImpl;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;

/**
 * Created by Enrico Risa on 25/09/15.
 */

@Component
public class AutoCloseDaemon {

  @Autowired
  protected OrganizationRepository organizationRepository;

  @Autowired
  protected RepositoryService      repositoryService;

  @Autowired
  protected IssueService           issueService;

  // @Scheduled(fixedDelay = 12 * 60 * 60 * 1000)
  public void autoClose() {

    Iterable<Organization> organizations = organizationRepository.findAll();

    for (Organization organization : organizations) {
      List<OUser> bots = organizationRepository.findBots(organization.getName());
      OUser user;
      if (bots.size() > 0) {
        user = bots.iterator().next();
      } else {
        continue;
      }
      List<Issue> organizationIssuesByLabel = organizationRepository.findOrganizationIssuesByLabel(organization.getName(),
          IssueServiceImpl.WAIT_FOR_REPLY);

      for (Issue issue : organizationIssuesByLabel) {
        try {
          if (issue.isClosed()) {
            issueService.removeLabel(issue, IssueServiceImpl.WAIT_FOR_REPLY, user, !Boolean.TRUE.equals(issue.getConfidential()));
          } else if (isToClose(organization, issue.getRepository(), issue)) {
            IssueDTO patch = new IssueDTO();
            patch.setState("CLOSED");
            repositoryService.patchIssue(issue, user, patch);
            Comment comment = new Comment();
            comment.setBody(organization.getClosingMessage());
            issueService.createNewCommentOnIssue(issue, comment, user);
            issueService.removeLabel(issue, IssueServiceImpl.WAIT_FOR_REPLY, user, !Boolean.TRUE.equals(issue.getConfidential()));
          }
        } catch (Exception e) {

        }
      }
    }
  }

  private boolean isToClose(Organization organization, Repository repository, Issue issue) {

    if (organization.getClosingDays() == null || organization.getClosingMessage() == null) {
      return false;
    }
    List<Event> events = organizationRepository.findEventsByOrgRepoAndIssueNumber(organization.getName(), repository.getName(),
        issue.getIid());

    Date time = null;
    Date now = new Date();
    for (Event event : events) {
      if (event instanceof IssueEvent) {
        IssueEvent issueEvent = (IssueEvent) event;
        if (issueEvent.getLabel() != null && issueEvent.getLabel().getName().equals(IssueServiceImpl.WAIT_FOR_REPLY)) {
          time = issueEvent.getCreatedAt();
        }
      }
    }
    if (time != null) {

      DateTime dateTime = new DateTime(time);
      DateTime nowDateTime = new DateTime(now);
      Days d = Days.daysBetween(dateTime, nowDateTime);
      int days = d.getDays();
      return days > organization.getClosingDays();
    }
    return false;
  }
}
