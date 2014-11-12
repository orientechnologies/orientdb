package com.orientechnologies.website.services.reactor;

import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.github.*;
import com.orientechnologies.website.model.schema.dto.*;
import com.orientechnologies.website.repository.*;
import com.orientechnologies.website.services.IssueService;
import com.orientechnologies.website.services.RepositoryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.event.Event;
import reactor.function.Consumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Enrico Risa on 24/10/14.
 */
@Service
public class GitHubIssueImporter implements Consumer<Event<GitHubIssueImporter.GitHubIssueMessage>> {

  protected Logger               log = LoggerFactory.getLogger(this.getClass());
  @Autowired
  private OrientDBFactory        dbFactory;

  @Autowired
  private RepositoryRepository   repoRepository;

  @Autowired
  private RepositoryService      repositoryService;

  @Autowired
  private OrganizationRepository organizationRepository;
  @Autowired
  private IssueRepository        issueRepo;

  @Autowired
  private IssueService           issueService;
  @Autowired
  private UserRepository         userRepo;

  @Autowired
  private EventRepository        eventRepository;
  @Autowired
  private LabelRepository        labelRepository;
  @Autowired
  private CommentRepository      commentRepository;

  @Autowired
  private MilestoneRepository    milestoneRepository;

  @Override
  public void accept(Event<GitHubIssueMessage> event) {
    GitHubIssueMessage message = event.getData();

    try {

      dbFactory.getGraph().begin();

      Repository repoDtp = repoRepository.findByOrgAndName(message.org, message.repo);

      importLabels(message, repoDtp);
      importMilestones(message, repoDtp);
      importIssue(message, repoDtp, GIssueState.CLOSED);

      dbFactory.getGraph().commit();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void importLabels(GitHubIssueMessage message, Repository repoDtp) throws IOException {

    List<GLabel> labels = message.repository.getLabels();

    boolean labelNew = false;
    for (GLabel label : labels) {
      Label l = repoRepository.findLabelsByRepoAndName(repoDtp.getName(), label.getName());

      labelNew = false;
      if (l == null) {
        l = new Label();
        labelNew = true;

      }
      l.setName(label.getName());
      l.setColor(label.getColor());
      l = labelRepository.save(l);
      if (labelNew) {
        createRepositoryLabelAssociation(repoDtp, l);
      }
    }
  }

  private void createRepositoryLabelAssociation(Repository repoDtp, Label l) {

    repositoryService.addLabel(repoDtp, l);
  }

  private void importMilestones(GitHubIssueMessage message, Repository repoDtp) throws IOException {
    List<GMilestone> milestones = message.repository.getMilestones();

    boolean milestoneNew = false;
    for (GMilestone milestone : milestones) {
      Milestone m = repoRepository.findMilestoneByRepoAndName(repoDtp.getName(), milestone.getNumber());
      milestoneNew = false;
      if (m == null) {
        m = new Milestone();
        milestoneNew = true;
      }
      m = fillMilestone(milestone, m);
      if (milestoneNew) {
        createRepositoryMilestoneAssociation(repoDtp, m);
      }
    }

  }

  private void createRepositoryMilestoneAssociation(Repository repoDtp, Milestone m) {
    repositoryService.addMilestone(repoDtp, m);
  }

  private void importIssue(GitHubIssueMessage message, Repository repoDtp, GIssueState state) throws IOException {
    List<GIssue> issues = message.repository.getIssues(state);

    int i = 0;
    for (GIssue issue : issues) {

      importSingleIssue(message, repoDtp, issue);
      i++;
      log.info("Imported %d issues", i);
    }
  }

  private void importSingleIssue(GitHubIssueMessage message, Repository repoDtp, GIssue issue) throws IOException {
    Issue issueDto = repoRepository.findIssueByRepoAndNumber(repoDtp.getName(), issue.getNumber());

    boolean isNew = false;
    if (issueDto == null) {
      issueDto = new Issue();
      isNew = true;
    }
    issueDto.setNumber(issue.getNumber());
    issueDto.setBody(issue.getBody());
    issueDto.setTitle(issue.getTitle());
    issueDto.setState(issue.getState().toString());
    // import labels

    GMilestone m = issue.getMilestone();

    issueDto.setCreatedAt(issue.getCreatedAt());
    issueDto.setClosedAt(issue.getClosedAt());

    issueDto = issueRepo.save(issueDto);

    importIssueLabels(repoDtp, issue, issueDto);

    User user = userRepo.findUserOrCreateByLogin(issue.getUser().getLogin());

    // IMPORT ISSUE CREATOR
    importIssueUser(issueDto, user);

    // IMPORT ISSUE ASSIGNEE

    String login = issue.getAssignee() != null ? issue.getAssignee().getLogin() : null;
    if (login != null) {
      User assignee = userRepo.findUserOrCreateByLogin(login);
      importIssueAssignee(issueDto, user);
    }
    // IMPORT COMMENTS
    importIssueComments(issue, issueDto);

    // IMPORT EVENTS
    importIssueEvents(repoDtp, issue, issueDto);

    // IMPORT ISSUE MILESTONE
    importIssueMilestone(repoDtp, issueDto, m);

    if (isNew)
      repositoryService.createIssue(repoDtp, issueDto);
  }

  private void importIssueAssignee(Issue issueDto, User user) {
    issueService.changeAssignee(issueDto, user);
  }

  private void importIssueUser(Issue issueDto, User user) {
    issueService.changeUser(issueDto, user);
  }

  private void importIssueEvents(Repository repoDtp, GIssue issue, Issue issueDto) throws IOException {

    for (GEvent event : issue.getEvents()) {
      IssueEvent e = (IssueEvent) repoRepository.findIssueEventByRepoAndNumberAndEventNumber(repoDtp.getName(),
          issueDto.getNumber(), event.getId());
      if (e == null) {
        e = new IssueEvent();
        e.setCreatedAt(event.getCreatedAt());
        e.setEventId(event.getId());
        e.setEvent(event.getEvent());
        e.setActor(userRepo.findUserOrCreateByLogin(event.getActor().getLogin()));
        e = (IssueEvent) eventRepository.save(e);
        createIssueEventAssociation(issueDto, e);
      }
    }
  }

  private void createIssueEventAssociation(Issue issueDto, com.orientechnologies.website.model.schema.dto.Event e) {
    issueService.fireEvent(issueDto, e);
  }

  private void importIssueComments(GIssue issue, Issue issueDto) throws IOException {
    boolean isNewComment = false;
    for (GComment ghIssueComment : issue.getComments()) {
      Comment comment = commentRepository.findByIssueAndCommentId(issueDto, ghIssueComment.getId());

      isNewComment = false;
      if (comment == null) {
        comment = new Comment();
        isNewComment = true;
      }
      comment.setCommentId(ghIssueComment.getId());
      comment.setBody(ghIssueComment.getBody());
      comment.setUser(userRepo.findUserOrCreateByLogin(ghIssueComment.getUser().getLogin()));
      comment.setCreatedAt(ghIssueComment.getCreatedAt());
      comment.setUpdatedAt(ghIssueComment.getUpdatedAt());

      comment = commentRepository.save(comment);
      if (isNewComment)
        issueService.commentIssue(issueDto, comment);

    }
  }

  private void importIssueMilestone(Repository repoDtp, Issue issueDto, GMilestone m) {
    Milestone milestone = null;

    if (m != null) {
      milestone = repoRepository.findMilestoneByRepoAndName(repoDtp.getName(), m.getNumber());
    }

    if (milestone != null)
      issueService.changeMilestone(issueDto, milestone);
  }

  private void importIssueLabels(Repository repoDtp, GIssue issue, Issue issueDto) {
    List<Label> labels = new ArrayList<Label>();
    for (GLabel label : issue.getLabels()) {

      Label l = repoRepository.findLabelsByRepoAndName(repoDtp.getName(), label.getName());
      if (l != null) {
        labels.add(l);
      }
    }
    issueService.changeLabels(issueDto, labels, true);
  }

  private Milestone fillMilestone(GMilestone m, Milestone milestone) {
    milestone.setNumber(m.getNumber());
    milestone.setTitle(m.getTitle());
    milestone.setDescription(m.getDescription());
    milestone.setState(m.getState().name());
    milestone.setCreatedAt(m.getCreatedAt());
    milestone.setDueOn(m.getDueOn());

    milestone = milestoneRepository.save(milestone);
    return milestone;
  }

  public static class GitHubIssueMessage {

    private final String org;
    private String       repo;
    private GRepo        repository;

    public GitHubIssueMessage(GRepo repository) {
      this.repository = repository;
      org = repository.getFullName().split("/")[0];
      repo = repository.getFullName().split("/")[1];

    }
  }

}
