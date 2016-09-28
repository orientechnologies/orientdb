package com.orientechnologies.website.controllers;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.configuration.ApiVersion;
import com.orientechnologies.website.model.schema.dto.Comment;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.Label;
import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.model.schema.dto.web.IssueDTO;
import com.orientechnologies.website.model.schema.dto.web.MockComment;
import com.orientechnologies.website.model.schema.dto.web.MockIssueDTO;
import com.orientechnologies.website.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.view.RedirectView;
import reactor.core.Reactor;
import reactor.event.Event;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * Created by Enrico Risa on 24/06/15.
 */
@RestController
@EnableAutoConfiguration
@RequestMapping(value = ApiUrls.GITHUB_V1 + "/mock")
@ApiVersion(1)
@Profile(value = { "development", "test"})
public class GithubMockController {

  @Autowired
  private Reactor        reactor;

  @Autowired
  private UserRepository userRepository;

  @RequestMapping(value = "/repos/{owner}/{repo}/issues", method = RequestMethod.POST, consumes = { "application/json",
      "application/octet-stream" })
  public Issue openMockIssue(@PathVariable("owner") String owner, @PathVariable("repo") String repo, @RequestBody MockIssueDTO i) {
    Issue issue = new Issue();
    issue.setState("open");
    issue.setNumber(1);
    issue.setTitle(i.getTitle());
    issue.setBody(i.getBody());
    issue.setCreatedAt(new Date());
    issue.setUpdatedAt(new Date());

    return issue;
  }

  @RequestMapping(value = "/repos/{owner}/{repo}/issues/{number}/labels/{labelName}", method = RequestMethod.DELETE, consumes = {
      "application/json", "application/octet-stream" })
  public List<Label> deleteLabel(@PathVariable("owner") final String owner, @PathVariable("repo") final String repo,
      @PathVariable("number") final Integer number, @PathVariable("labelName") final String label,
      @RequestHeader(value = "Authorization") String auth) {

    List<Label> labels1 = new ArrayList<Label>();

    final OUser byGithubToken = userRepository.findByGithubToken(auth.replace("token ", ""));
    final Label l = new Label();
    l.setName(label);
    labels1.add(l);
    runInThread(new Runnable() {
      @Override
      public void run() {
        ODocument doc = new ODocument();
        doc.field("action", "unlabeled");
        doc.field("label", new ODocument().field("name", label));
        doc.field("organization", new ODocument().field("login", owner));
        doc.field("repository", new ODocument().field("name", repo));
        doc.field("issue", new ODocument().field("number", number));
        doc.field("sender", new ODocument().field("login", byGithubToken.getName()).field("id", byGithubToken.getId()));
        reactor.notify(doc.field("action"), Event.wrap(doc));
      }
    });
    return labels1;
  }

  @RequestMapping(value = "/repos/{owner}/{repo}/issues/{number}/labels", method = RequestMethod.POST, consumes = {
      "application/json", "application/octet-stream" })
  public List<Label> postLabel(@PathVariable("owner") final String owner, @PathVariable("repo") final String repo,
      @PathVariable("number") final Integer number, @RequestBody List<String> labels,
      @RequestHeader(value = "Authorization") String auth) {

    List<Label> labels1 = new ArrayList<Label>();

    final OUser byGithubToken = userRepository.findByGithubToken(auth.replace("token ", ""));
    for (final String label : labels) {
      final Label l = new Label();
      l.setName(label);
      labels1.add(l);
      runInThread(new Runnable() {
        @Override
        public void run() {
          ODocument doc = new ODocument();
          doc.field("action", "labeled");
          doc.field("label", new ODocument().field("name", label));
          doc.field("organization", new ODocument().field("login", owner));
          doc.field("repository", new ODocument().field("name", repo));
          doc.field("issue", new ODocument().field("number", number));
          doc.field("sender", new ODocument().field("login", byGithubToken.getName()).field("id", byGithubToken.getId()));
          reactor.notify(doc.field("action"), Event.wrap(doc));
        }
      });
    }

    return labels1;
  }

  @RequestMapping(value = "/repos/{owner}/{repo}/issues/{number}", method = RequestMethod.PATCH, consumes = { "application/json",
      "application/octet-stream" })
  public IssueDTO patchIssue(@PathVariable("owner") final String owner, @PathVariable("repo") final String repo,
      @PathVariable("number") final Integer number, @RequestBody final IssueDTO issue,
      @RequestHeader(value = "Authorization") String auth) {

    final OUser byGithubToken = userRepository.findByGithubToken(auth.replace("token ", ""));

    if (issue.getAssignee() != null) {

      final OUser assignee = userRepository.findUserByLogin(issue.getAssignee());
      runInThread(new Runnable() {
        @Override
        public void run() {
          ODocument doc = new ODocument();
          doc.field("action", "assigned");
          doc.field("assignee", new ODocument().field("login", assignee.getName()).field("id", assignee.getId()));
          doc.field("organization", new ODocument().field("login", owner));
          doc.field("repository", new ODocument().field("name", repo));
          doc.field("issue", new ODocument().field("number", number));
          doc.field("sender", new ODocument().field("login", byGithubToken.getName()).field("id", byGithubToken.getId()));
          reactor.notify(doc.field("action"), Event.wrap(doc));
        }
      });
    }
    if (issue.getState() != null) {

      runInThread(new Runnable() {
        @Override
        public void run() {
          ODocument doc = new ODocument();
          String event = issue.getState().equalsIgnoreCase("CLOSED") ? "closed" : "reopened";
          doc.field("action", event);
          doc.field("organization", new ODocument().field("login", owner));
          doc.field("repository", new ODocument().field("name", repo));
          doc.field("issue", new ODocument().field("number", number).field("state", issue.getState()));
          doc.field("sender", new ODocument().field("login", byGithubToken.getName()).field("id", byGithubToken.getId()));
          reactor.notify(doc.field("action"), Event.wrap(doc));
        }
      });
    }
    return issue;
  }

  @RequestMapping(value = "/repos/{owner}/{repo}/issues/{number}/comments", method = RequestMethod.POST, consumes = {
      "application/json", "application/octet-stream" })
  public Comment postIssueComment(@PathVariable("owner") final String owner, @PathVariable("repo") final String repo,
      @PathVariable("number") final Integer number, @RequestBody MockComment comment,
      @RequestHeader(value = "Authorization") String auth) {

    final OUser byGithubToken = userRepository.findByGithubToken(auth.replace("token ", ""));

    comment.setCommentId(new Random().nextInt());
    comment.setCreatedAt(new Date());
    comment.setUpdatedAt(new Date());

    runInThread(new Runnable() {
      @Override
      public void run() {
        ODocument doc = new ODocument();
        doc.field("action", "created");
        doc.field("organization", new ODocument().field("login", owner));
        doc.field("repository", new ODocument().field("name", repo));
        doc.field("issue", new ODocument().field("number", number));
        doc.field("comment",
            new ODocument().field("created_at", comment.getCreatedAt()).field("updated_at", comment.getUpdatedAt())
                .field("id", comment.getCommentId())
                .field("user", new ODocument().field("login", byGithubToken.getName()).field("id", byGithubToken.getId())));
        doc.field("sender", new ODocument().field("login", byGithubToken.getName()).field("id", byGithubToken.getId()));
        reactor.notify(doc.field("action"), Event.wrap(doc));
      }
    });
    return comment;

  }

  @RequestMapping(value = "/user", method = RequestMethod.GET)
  public OUser findUser(@RequestHeader(value = "Authorization") String auth) {

    final OUser byGithubToken = userRepository.findByGithubToken(auth.replace("token ", ""));

    return byGithubToken;

  }

  @RequestMapping(value = "/loginAs/{username}", method = RequestMethod.GET)
  public RedirectView loginAs(@PathVariable("username") final String username, HttpServletResponse res) {

    final OUser byGithubToken = userRepository.findUserByLogin(username);

    Cookie cookie = new Cookie("prjhub_token", byGithubToken.getToken());
    cookie.setMaxAge(2000);
    cookie.setPath("/");
    res.addCookie(cookie);

    RedirectView view = new RedirectView();
    view.setUrl("/");
    return view;

  }

  public void runInThread(final Runnable runnable) {

    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        runnable.run();
      }
    }).start();
  }
}
