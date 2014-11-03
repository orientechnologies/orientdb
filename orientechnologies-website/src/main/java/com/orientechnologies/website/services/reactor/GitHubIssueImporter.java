package com.orientechnologies.website.services.reactor;

import com.orientechnologies.website.model.schema.dto.Comment;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.Repository;
import com.orientechnologies.website.repository.CommentRepository;
import com.orientechnologies.website.repository.IssueRepository;
import com.orientechnologies.website.repository.RepositoryRepository;
import com.orientechnologies.website.repository.UserRepository;
import com.orientechnologies.website.services.IssueService;
import com.orientechnologies.website.services.RepositoryService;
import org.kohsuke.github.GHIssue;
import org.kohsuke.github.GHIssueComment;
import org.kohsuke.github.GHIssueState;
import org.kohsuke.github.GHRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.event.Event;
import reactor.function.Consumer;

import com.orientechnologies.website.OrientDBFactory;

import java.io.IOException;
import java.util.List;

/**
 * Created by Enrico Risa on 24/10/14.
 */
@Service
public class GitHubIssueImporter implements Consumer<Event<GitHubIssueImporter.GitHubIssueMessage>> {

  protected Logger             log = LoggerFactory.getLogger(this.getClass());
  @Autowired
  private OrientDBFactory      dbFactory;

  @Autowired
  private RepositoryRepository repoRepository;

  @Autowired
  private RepositoryService    repositoryService;

  @Autowired
  private IssueRepository      issueRepo;

  @Autowired
  private IssueService         issueService;
  @Autowired
  private UserRepository       userRepo;

  @Autowired
  private CommentRepository    commentRepository;

  @Override
  public void accept(Event<GitHubIssueMessage> event) {
    GitHubIssueMessage message = event.getData();

    try {

      dbFactory.getGraph().begin();

      Repository repoDtp = repoRepository.findByOrgAndName(message.org, message.repo);
      importIssue(message, repoDtp, GHIssueState.CLOSED);
      importIssue(message, repoDtp, GHIssueState.OPEN);

      dbFactory.getGraph().commit();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void importIssue(GitHubIssueMessage message, Repository repoDtp, GHIssueState state) throws IOException {
    List<GHIssue> issues = message.repository.getIssues(state);
    int i = 0;
    for (GHIssue issue : issues) {

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
      for (GHIssue.Label label : issue.getLabels()) {
        issueDto.addLabel(label.getName());
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
      for (GHIssueComment ghIssueComment : issue.getComments()) {
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

      if (isNew)
        repositoryService.createIssue(repoDtp, issueDto);

      i++;
      log.info("Imported %d issues", i);
    }
  }

  public static class GitHubIssueMessage {

    private final String org;
    private String       repo;
    private GHRepository repository;

    public GitHubIssueMessage(GHRepository repository) {
      this.repository = repository;
      org = repository.getFullName().split("/")[0];
      repo = repository.getFullName().split("/")[1];

    }
  }

}
