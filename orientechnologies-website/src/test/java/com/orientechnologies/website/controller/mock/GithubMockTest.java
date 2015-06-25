package com.orientechnologies.website.controller.mock;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import com.jayway.restassured.specification.RequestSpecification;
import com.orientechnologies.website.Application;
import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.configuration.GitHubConfiguration;
import com.orientechnologies.website.model.schema.dto.*;
import com.orientechnologies.website.model.schema.dto.web.IssueDTO;
import com.orientechnologies.website.model.schema.dto.web.hateoas.ScopeDTO;
import com.orientechnologies.website.repository.*;
import com.orientechnologies.website.services.IssueService;
import com.orientechnologies.website.services.OrganizationService;
import com.orientechnologies.website.services.RepositoryService;
import com.orientechnologies.website.services.impl.IssueServiceImpl;
import com.orientechnologies.website.services.impl.OrganizationServiceImpl;
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

import java.util.*;

import static com.jayway.restassured.RestAssured.given;

/**
 * Created by Enrico Risa on 24/06/15.
 */

@ActiveProfiles("test")
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = Application.class)
@WebAppConfiguration
@IntegrationTest("server.port:0")
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class GithubMockTest {

  public static final int    MILLIS = 2000;
  public static boolean      dbInit = false;

  public static Organization test;
  public static OUser        bot;
  public static OUser        user;
  public static OUser        normalUser;
  public static Scope        scope;
  public static boolean      setup  = false;
  static {
    test = new Organization();
    test.setName("romeshell");
  }

  @Value("${local.server.port}")
  int                        port;

  @Autowired
  OrganizationRepository     repository;

  @Autowired
  RepositoryRepository       repoRepository;
  @Autowired
  IssueRepository            issueRepository;
  @Autowired
  RepositoryService          repositoryService;

  @Autowired
  GitHubConfiguration        gitHubConfiguration;
  @Autowired
  IssueService               issueService;
  @Autowired
  UserRepository             userRepository;

  @Autowired
  LabelRepository            labelRepository;
  @Autowired
  OrganizationService        organizationService;
  @Autowired
  OrientDBFactory            dbFactory;

  @Before
  public void setUp() {

    if (!dbInit) {
      gitHubConfiguration.setPort(port);
      user = new OUser();
      user.setName("maggiolo00");
      user.setToken("testToken");
      user.setId(1l);
      user = userRepository.save(user);

      bot = new OUser();
      bot.setName("testBot");
      bot.setToken("testBot");
      bot.setId(2l);
      bot = userRepository.save(bot);

      normalUser = new OUser();
      normalUser.setName("testNormal");
      normalUser.setToken("testNormal");
      normalUser.setId(3l);
      normalUser = userRepository.save(normalUser);
      test = repository.save(test);

      organizationService.createMembership(test, user);
      Repository repo = repositoryService.createRepo("gnome-shell-extension-aam", null);

      // createLabel("bug", repo);
      // createLabel("waiting reply", repo);

      try {
        OrganizationServiceImpl impl = getTargetObject(organizationService, OrganizationServiceImpl.class);
        impl.createHasRepoRelationship(test, repo);
      } catch (Exception e) {
        e.printStackTrace();
      }

      dbFactory.getGraph().commit();

      ScopeDTO scopeDTO = new ScopeDTO();
      scopeDTO.setName("TestScope");
      scopeDTO.setOwner("maggiolo00");
      scopeDTO.setMembers(new ArrayList<String>());
      scopeDTO.setRepository("gnome-shell-extension-aam");
      scope = organizationService.registerScope("romeshell", scopeDTO, null);
      organizationService.registerBot("romeshell", bot.getName());

      dbFactory.getGraph().commit();

      RestAssured.port = port;
      dbInit = true;
    }
  }

  protected void createLabel(String name, Repository repo) {
    Label label = new Label();
    label.setName(name);

    label = labelRepository.save(label);
    repositoryService.addLabel(repo, label);

  }

  @Test
  public void test1OpenIssue() {

    IssueDTO issueDTO = new IssueDTO();
    issueDTO.setTitle("error");
    issueDTO.setBody("Error");
    issueDTO.setType("bug");
    issueDTO.setScope(scope.getNumber());
    Response post = header(user.getToken()).body(issueDTO).when().post("/api/v1/orgs/romeshell/issues");

    Assert.assertEquals(post.statusCode(), 200);
    Issue as = post.as(Issue.class);
    Assert.assertNotNull(as.getIid());
    Assert.assertNotNull(as.getNumber());
    Assert.assertEquals(as.getUser().getName(), "maggiolo00");
    Assert.assertEquals(as.getTitle(), issueDTO.getTitle());
    Assert.assertEquals(as.getBody(), issueDTO.getBody());

    try {
      Thread.sleep(MILLIS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    Response get = header(user.getToken()).get("/api/v1/orgs/romeshell/issues/" + as.getIid());

    as = get.as(Issue.class);
    Assert.assertNotNull(as.getIid());
    Assert.assertNotNull(as.getNumber());

    Assert.assertEquals(as.getUser().getName(), "maggiolo00");
    Assert.assertEquals(as.getTitle(), issueDTO.getTitle());
    Assert.assertEquals(as.getBody(), issueDTO.getBody());
    Assert.assertEquals(as.getLabels().size(), 1);
    Assert.assertEquals(as.getLabels().iterator().next().getName(), "bug");

    get = header(user.getToken()).get("/api/v1/repos/romeshell/gnome-shell-extension-aam/issues/" + as.getIid() + "/events");

    List<Map> events = Arrays.asList(get.getBody().as(Map[].class));

    // scoped + label
    Assert.assertEquals(events.size(), 2);
  }

  @Test
  public void test2AssignIssue() {

    Map<String, String> params = new HashMap<String, String>() {
      {
        put("assignee", "maggiolo00");
      }
    };

    Response patch = header(user.getToken()).body(params).when()
        .patch("/api/v1/repos/romeshell/gnome-shell-extension-aam/issues/1");

    Issue as = patch.as(Issue.class);

    Assert.assertEquals(patch.statusCode(), 200);

    try {
      Thread.sleep(MILLIS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Response get = header(user.getToken()).get("/api/v1/orgs/romeshell/issues/" + as.getIid());
    as = get.as(Issue.class);
    Assert.assertNotNull(as.getAssignee());
    Assert.assertEquals(as.getAssignee().getName(), "maggiolo00");

    get = header(user.getToken()).get("/api/v1/repos/romeshell/gnome-shell-extension-aam/issues/" + as.getIid() + "/events");

    List<Map> events = Arrays.asList(get.getBody().as(Map[].class));

    // scoped + label + assigne
    Assert.assertEquals(events.size(), 3);
  }

  @Test
  public void test3Comment() {

    Comment comment = new Comment();
    comment.setBody("Test");

    Response c = header(user.getToken()).body(comment).when()
        .post("/api/v1/repos/romeshell/gnome-shell-extension-aam/issues/1/comments");

    try {
      Thread.sleep(MILLIS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Assert.assertEquals(c.statusCode(), 200);
    Comment posted = c.as(Comment.class);
    Assert.assertEquals(posted.getBody(), comment.getBody());
    Assert.assertNotNull(posted.getCommentId());
    Assert.assertNotNull(posted.getCreatedAt());
    Response get = header(user.getToken()).get("/api/v1/repos/romeshell/gnome-shell-extension-aam/issues/1/events");

    List<Map> events = Arrays.asList(get.getBody().as(Map[].class));

    // scoped + label + assigne + comment
    Assert.assertEquals(events.size(), 4);
  }

  @Test
  public void test4Label() {

    List<String> labels = new ArrayList<String>() {
      {
        add(IssueServiceImpl.WAIT_FOR_REPLY);
      }
    };

    Response c = header(user.getToken()).body(labels).when()
        .post("/api/v1/repos/romeshell/gnome-shell-extension-aam/issues/1/labels");

    try {
      Thread.sleep(MILLIS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Assert.assertEquals(c.statusCode(), 200);

    Response get = header(user.getToken()).get("/api/v1/orgs/romeshell/issues/1");
    Issue as = get.as(Issue.class);

    Assert.assertEquals(as.getLabels().size(), 2);
    get = header(user.getToken()).get("/api/v1/repos/romeshell/gnome-shell-extension-aam/issues/1/events");

    List<Map> events = Arrays.asList(get.getBody().as(Map[].class));

    // scoped + label + assigne + comment + label
    Assert.assertEquals(events.size(), 5);
  }

  @Test
  public void test4userComment() {

    Comment comment = new Comment();
    comment.setBody("Test");

    Response c = header(normalUser.getToken()).body(comment).when()
        .post("/api/v1/repos/romeshell/gnome-shell-extension-aam/issues/1/comments");

    try {
      Thread.sleep(MILLIS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Assert.assertEquals(c.statusCode(), 200);
    Comment posted = c.as(Comment.class);
    Assert.assertEquals(posted.getBody(), comment.getBody());
    Assert.assertNotNull(posted.getCommentId());
    Assert.assertNotNull(posted.getCreatedAt());
    Response get = header(user.getToken()).get("/api/v1/repos/romeshell/gnome-shell-extension-aam/issues/1/events");

    List<Map> events = Arrays.asList(get.getBody().as(Map[].class));

    // scoped + label + assigne + comment + label + comment + remove wait for reply
    Assert.assertEquals(events.size(), 7);

    get = header(user.getToken()).get("/api/v1/orgs/romeshell/issues/1");
    Issue as = get.as(Issue.class);

    Assert.assertEquals(as.getLabels().size(), 1);
  }

  @SuppressWarnings({ "unchecked" })
  protected <T> T getTargetObject(Object proxy, Class<T> targetClass) throws Exception {
    if (AopUtils.isJdkDynamicProxy(proxy)) {
      return (T) ((Advised) proxy).getTargetSource().getTarget();
    } else {
      return (T) proxy; // expected to be cglib proxy then, which is simply a specialized class
    }
  }

  public static RequestSpecification header(String token) {
    return given().header("X-AUTH-TOKEN", token).given().contentType("application/json");
  }
}
