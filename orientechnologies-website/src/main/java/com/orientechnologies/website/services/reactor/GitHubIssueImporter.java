package com.orientechnologies.website.services.reactor;

import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.github.*;
import com.orientechnologies.website.model.schema.dto.Comment;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.Milestone;
import com.orientechnologies.website.model.schema.dto.Repository;
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
      importIssue(message, repoDtp, GIssueState.CLOSED);
      importIssue(message, repoDtp, GIssueState.OPEN);

      dbFactory.getGraph().commit();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void importLabels(GitHubIssueMessage message, Repository repoDtp) {

  }

  private void importIssue(GitHubIssueMessage message, Repository repoDtp, GIssueState state) throws IOException {
    List<GIssue> issues = message.repository.getIssues(state);

    int i = 0;
    for (GIssue issue : issues) {

      Issue issueDto = repoRepository.findIssueByRepoAndNumber(repoDtp.getCodename(), issue.getNumber());

      boolean isNew = false;
      if (issueDto == null) {
        issueDto = new Issue();
        isNew = true;
      }
      issueDto.setNumber(issue.getNumber());
      issueDto.setBody(issue.getBody());
      issueDto.setTitle(issue.getTitle());
      issueDto.setState(issue.getState().name());
      for (GLabel label : issue.getLabels()) {
        issueDto.addLabel(label.getName());
      }

      GMilestone m = issue.getMilestone();
      Milestone milestone = null;

      if (m != null) {
        milestone = organizationRepository.findMilestoneByOwnerRepoAndNumberIssueAndNumberMilestone(message.org, message.repo,
            issue.getNumber(), m.getNumber());
        if (milestone == null) {
          milestone = new Milestone();
        }

        milestone.setNumber(m.getNumber());
        milestone.setTitle(m.getTitle());
        milestone.setDescription(m.getDescription());
        milestone.setState(m.getState().name());
        milestone.setCreatedAt(m.getCreatedAt());
        milestone.setDueOn(m.getDueOn());

        milestone = milestoneRepository.save(milestone);
      }

      issueDto.setUser(userRepo.findUserOrCreateByLogin(issue.getUser().getLogin()));

      issueDto.setCreatedAt(issue.getCreatedAt());
      issueDto.setClosedAt(issue.getClosedAt());
      String login = issue.getAssignee() != null ? issue.getAssignee().getLogin() : null;
      if (login != null) {
        issueDto.setAssignee(userRepo.findUserOrCreateByLogin(login));
      }

      issueDto = issueRepo.save(issueDto);

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
        comment.setUpdatedAt(comment.getUpdatedAt());

        comment = commentRepository.save(comment);
        if (isNewComment)
          issueService.commentIssue(issueDto, comment);

      }

      if (milestone != null)
        issueService.changeMilestone(issueDto, milestone);
      if (isNew)
        repositoryService.createIssue(repoDtp, issueDto);

      i++;
      log.info("Imported %d issues", i);
    }
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
