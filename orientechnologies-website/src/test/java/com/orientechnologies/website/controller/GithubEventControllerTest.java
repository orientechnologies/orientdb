package com.orientechnologies.website.controller;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import com.jayway.restassured.specification.RequestSpecification;
import com.orientechnologies.website.Application;
import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.events.EventManager;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.model.schema.dto.Organization;
import com.orientechnologies.website.model.schema.dto.Repository;
import com.orientechnologies.website.repository.IssueRepository;
import com.orientechnologies.website.repository.OrganizationRepository;
import com.orientechnologies.website.repository.RepositoryRepository;
import com.orientechnologies.website.repository.UserRepository;
import com.orientechnologies.website.services.IssueService;
import com.orientechnologies.website.services.OrganizationService;
import com.orientechnologies.website.services.RepositoryService;
import com.orientechnologies.website.services.impl.OrganizationServiceImpl;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.testng.Assert;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.jayway.restassured.RestAssured.given;

/**
 * Created by Enrico Risa on 20/11/14.
 */

@ActiveProfiles("test")
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = Application.class)
@WebAppConfiguration
@IntegrationTest("server.port:0")
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class GithubEventControllerTest {

  public static final int    MILLIS = 2000;
  public static Organization test;
  public static boolean      setup  = false;
  static {
    test = new Organization();
    test.setName("romeshell");
  }
  @Autowired
  OrganizationRepository     repository;

  @Autowired
  RepositoryRepository       repoRepository;
  @Autowired
  IssueRepository            issueRepository;
  @Autowired
  RepositoryService          repositoryService;

  @Autowired
  IssueService               issueService;
  @Autowired
  UserRepository             userRepository;
  @Autowired
  OrganizationService        organizationService;
  @Autowired
  OrientDBFactory            dbFactory;
  @Value("${local.server.port}")
  int                        port;

  @Autowired
  EventManager               eventManager;

  @Before
  public void setUp() {

    if (!setup) {

      OUser user = new OUser();
      user.setName("maggiolo00");
      user.setToken("testToken");
      user = userRepository.save(user);
      test = repository.save(test);
      Repository repo = repositoryService.createRepo("gnome-shell-extension-aam", null);

      try {
        OrganizationServiceImpl impl = getTargetObject(organizationService, OrganizationServiceImpl.class);
        impl.createHasRepoRelationship(test, repo);
      } catch (Exception e) {
        e.printStackTrace();
      }
      Issue issue = new Issue();
      issue.setNumber(1);
      issue.setTitle("test");
      issue.setState("open");
      issue.setUser(user);

      issue = issueRepository.save(issue);
      repositoryService.createIssue(repo, issue);
      issueService.changeUser(issue, user);
      dbFactory.getGraph().commit();
      RestAssured.port = port;
    }
    setup = true;
  }

  @SuppressWarnings({ "unchecked" })
  protected <T> T getTargetObject(Object proxy, Class<T> targetClass) throws Exception {
    if (AopUtils.isJdkDynamicProxy(proxy)) {
      return (T) ((Advised) proxy).getTargetSource().getTarget();
    } else {
      return (T) proxy; // expected to be cglib proxy then, which is simply a specialized class
    }
  }

  @After
  public void deInit() {
    // dbFactory.getGraph().drop();
  }

  @Test
  public void test1Assign() {

    InputStream stream = ClassLoader.getSystemResourceAsStream("assigne.json");
    try {
      String content = IOUtils.toString(stream);
      given().body(content).when().post("/api/v1/github/events");

      Thread.sleep(MILLIS);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    Response response = header().get("/api/v1/repos/{name}/gnome-shell-extension-aam/issues/1", test.getName());

    Assert.assertEquals(response.statusCode(), 200);

    Issue issue = response.getBody().as(Issue.class);

    Assert.assertEquals(issue.getAssignee().getName(), "maggiolo00");

    Assert.assertEquals(eventManager.firedEvents.get(), 1);

  }

  @Test
  public void test2UnAssign() {

    InputStream stream = ClassLoader.getSystemResourceAsStream("unassigned.json");
    try {
      String content = IOUtils.toString(stream);
      given().body(content).when().post("/api/v1/github/events");

      Thread.sleep(MILLIS);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    Response response = header().get("/api/v1/repos/{name}/gnome-shell-extension-aam/issues/1", test.getName());

    Assert.assertEquals(response.statusCode(), 200);

    Issue issue = response.getBody().as(Issue.class);

    Assert.assertNull(issue.getAssignee());

  }

  @Test
  public void test3Label() {

    InputStream stream = ClassLoader.getSystemResourceAsStream("label.json");
    try {
      String content = IOUtils.toString(stream);
      given().body(content).when().post("/api/v1/github/events");

      Thread.sleep(MILLIS);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    Response response = header().get("/api/v1/repos/{name}/gnome-shell-extension-aam/issues/1", test.getName());

    Assert.assertEquals(response.statusCode(), 200);

    Issue issue = response.getBody().as(Issue.class);

    Assert.assertEquals(issue.getLabels().size(), 1);
  }

  @Test
  public void test4UnLabel() {

    InputStream stream = ClassLoader.getSystemResourceAsStream("unlabel.json");
    try {
      String content = IOUtils.toString(stream);
      given().body(content).when().post("/api/v1/github/events");

      Thread.sleep(MILLIS);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    Response response = header().get("/api/v1/repos/{name}/gnome-shell-extension-aam/issues/1", test.getName());

    Assert.assertEquals(response.statusCode(), 200);

    Issue issue = response.getBody().as(Issue.class);

    Assert.assertEquals(issue.getLabels().size(), 0);
  }

  @Test
  public void test5Closed() {

    InputStream stream = ClassLoader.getSystemResourceAsStream("closed.json");
    try {
      String content = IOUtils.toString(stream);
      given().body(content).when().post("/api/v1/github/events");

      Thread.sleep(MILLIS);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    Response response = header().get("/api/v1/repos/{name}/gnome-shell-extension-aam/issues/1", test.getName());

    Assert.assertEquals(response.statusCode(), 200);

    Issue issue = response.getBody().as(Issue.class);

    Assert.assertEquals(issue.getState(), "CLOSED");

    Assert.assertEquals(eventManager.firedEvents.get(), 2);
  }

  @Test
  public void test6Reopened() {

    InputStream stream = ClassLoader.getSystemResourceAsStream("reopened.json");
    try {
      String content = IOUtils.toString(stream);
      given().body(content).when().post("/api/v1/github/events");

      Thread.sleep(MILLIS);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    Response response = header().get("/api/v1/repos/{name}/gnome-shell-extension-aam/issues/1", test.getName());

    Assert.assertEquals(response.statusCode(), 200);

    Issue issue = response.getBody().as(Issue.class);

    Assert.assertEquals(issue.getState(), "OPEN");

    Assert.assertEquals(eventManager.firedEvents.get(), 3);
  }

  @Test
  public void test7Comment() {

    InputStream stream = ClassLoader.getSystemResourceAsStream("comment.json");
    try {
      String content = IOUtils.toString(stream);
      given().body(content).when().post("/api/v1/github/events");

      Thread.sleep(MILLIS);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    Response response = header().get("/api/v1/repos/{name}/gnome-shell-extension-aam/issues/1", test.getName());

    Assert.assertEquals(response.statusCode(), 200);

    Issue issue = response.getBody().as(Issue.class);

    Assert.assertEquals(issue.getComments().longValue(), 1);

    Assert.assertEquals(eventManager.firedEvents.get(), 4);
  }

  @Test
  public void test8Events() {

    Response response = header().get("/api/v1/repos/{name}/gnome-shell-extension-aam/issues/1/events", test.getName());

    Assert.assertEquals(response.statusCode(), 200);

    List<Map> events = Arrays.asList(response.getBody().as(Map[].class));

    Assert.assertEquals(events.size(), 7);
  }

  @Test
  public void test9Opened() {
    InputStream stream = ClassLoader.getSystemResourceAsStream("opened.json");
    try {
      String content = IOUtils.toString(stream);
      given().body(content).when().post("/api/v1/github/events");

      Thread.sleep(MILLIS);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    Response response = header().get("/api/v1/repos/{name}/gnome-shell-extension-aam/issues/2", test.getName());

    Assert.assertEquals(response.statusCode(), 200);

    Assert.assertEquals(eventManager.firedEvents.get(), 5);

  }

  public static RequestSpecification header() {
    return given().header("X-AUTH-TOKEN", "testToken").given().contentType("application/json");
  }
}
