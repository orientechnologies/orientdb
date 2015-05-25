package com.orientechnologies.website.daemon;

import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.github.GIssueState;
import com.orientechnologies.website.github.GRepo;
import com.orientechnologies.website.github.GitHub;
import com.orientechnologies.website.model.schema.ORepository;
import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.model.schema.dto.Repository;
import com.orientechnologies.website.repository.OrganizationRepository;
import com.orientechnologies.website.services.IssueService;
import com.orientechnologies.website.services.reactor.GitHubIssueImporter;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.Reactor;
import reactor.event.Event;

import java.util.List;
import java.util.concurrent.*;

/**
 * Created by Enrico Risa on 15/05/15.
 */
@Component
public class IssueAlignDaemon {

  @Autowired
  private OrientDBFactory        dbFactory;

  @Autowired
  private IssueService           issueService;

  @Autowired
  private Reactor                reactor;

  @Autowired
  private OrganizationRepository organizationRepository;

  @Autowired
  private GitHubIssueImporter    issueImporter;

  @Scheduled(fixedDelay = 60 * 1000)
  public void alignIssues() {

    // OrientGraph db = dbFactory.getGraph();
    //
    // ORecordIteratorClass<ODocument> oDocuments = db.getRawGraph().browseClass(Issue.class.getSimpleName());
    //
    // while (oDocuments.hasNext()) {
    //
    // ODocument doc = oDocuments.next();
    //
    // Issue issue = OIssue.IID.fromDoc(doc, db);
    //
    // List<OUser> bots = organizationRepository.findBots(issue.getRepository().getOrganization().getName());
    // if (bots.size() > 0) {
    // if (issue.getNumber() != null) {
    // OUser next = bots.iterator().next();
    // issueService.conditionalSynchIssue(issue, next);
    // }
    // }
    // }

  }

  @Scheduled(cron = "0 30 23 * * ?")
  public void importIssues() {

    OrientGraph db = dbFactory.getGraph();

    ORecordIteratorClass<ODocument> oDocuments = db.getRawGraph().browseClass(Repository.class.getSimpleName());

    ExecutorService executor = Executors.newSingleThreadExecutor();
    while (oDocuments.hasNext()) {
      ODocument doc = oDocuments.next();
      Repository repo = ORepository.NAME.fromDoc(doc, db);
      List<OUser> bots = organizationRepository.findBots(repo.getOrganization().getName());
      if (bots.size() > 0) {
        OUser next = bots.iterator().next();
        GitHub github = new GitHub(next.getToken());
        try {
          GRepo repository = github.repo(repo.getOrganization().getName() + '/' + repo.getName());
          GitHubIssueImporter.GitHubIssueMessage gitHubIssueMessage = new GitHubIssueImporter.GitHubIssueMessage(repository);
          gitHubIssueMessage.setState(GIssueState.OPEN);

          importIssues(executor, gitHubIssueMessage);
          gitHubIssueMessage = new GitHubIssueImporter.GitHubIssueMessage(repository);
          gitHubIssueMessage.setState(GIssueState.CLOSED);

          importIssues(executor, gitHubIssueMessage);

        } catch (Exception e) {

        }

      }

    }

  }

  public void importIssues(ExecutorService executor, final GitHubIssueImporter.GitHubIssueMessage message)
      throws ExecutionException, InterruptedException {

    Future<Object> submit = executor.submit(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        issueImporter.accept(Event.wrap(message));
        return null;
      }
    });

    submit.get();

  }
}
