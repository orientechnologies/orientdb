package com.orientechnologies.website.services.reactor;

import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.Repository;
import com.orientechnologies.website.repository.IssueRepository;
import com.orientechnologies.website.repository.RepositoryRepository;
import com.orientechnologies.website.repository.UserRepository;
import com.orientechnologies.website.services.RepositoryService;
import org.kohsuke.github.GHIssue;
import org.kohsuke.github.GHIssueState;
import org.kohsuke.github.GHRepository;
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

  @Autowired
  private OrientDBFactory      dbFactory;

  @Autowired
  private RepositoryRepository repoRepository;

  @Autowired
  private RepositoryService    repositoryService;

  @Autowired
  private IssueRepository      issueRepo;

  @Autowired
  private UserRepository       userRepo;

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
    for (GHIssue issue : issues) {

      Issue issueDto = new Issue();

      issueDto.setNumber(issue.getNumber());
      issueDto.setDescription(issue.getBody());
      issueDto.setTitle(issue.getTitle());
      issueDto.setState(issue.getState().name());
      for (GHIssue.Label label : issue.getLabels()) {
        issueDto.addLabel(label.getName());
      }

      issueDto.setCreatedAt(issue.getCreatedAt());
      issueDto.setClosedAt(issue.getClosedAt());
      String login = issue.getAssignee().getLogin();
      if (login != null) {
        issueDto.setAssignee(userRepo.findUserByLogin(login));
      }

      issueDto = issueRepo.save(issueDto);

      repositoryService.createIssue(repoDtp, issueDto);

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
