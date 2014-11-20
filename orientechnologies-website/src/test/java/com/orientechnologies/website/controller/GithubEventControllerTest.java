package com.orientechnologies.website.controller;

import com.jayway.restassured.RestAssured;
import com.orientechnologies.website.Application;
import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.Organization;
import com.orientechnologies.website.model.schema.dto.Repository;
import com.orientechnologies.website.repository.IssueRepository;
import com.orientechnologies.website.repository.OrganizationRepository;
import com.orientechnologies.website.repository.RepositoryRepository;
import com.orientechnologies.website.services.OrganizationService;
import com.orientechnologies.website.services.RepositoryService;
import com.orientechnologies.website.services.impl.OrganizationServiceImpl;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import java.io.IOException;
import java.io.InputStream;

import static com.jayway.restassured.RestAssured.given;

/**
 * Created by Enrico Risa on 20/11/14.
 */

@ActiveProfiles("test")
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = Application.class)
@WebAppConfiguration
@IntegrationTest("server.port:0")
public class GithubEventControllerTest {

  @Autowired
  OrganizationRepository repository;

  @Autowired
  RepositoryRepository   repoRepository;
  @Autowired
  IssueRepository        issueRepository;
  @Autowired
  RepositoryService      repositoryService;

  @Autowired
  OrganizationService    organizationService;
  @Autowired
  OrientDBFactory        dbFactory;
  @Value("${local.server.port}")
  int                    port;

  @Before
  public void setUp() {

    // OSiteSchema.fillSchema(dbFactory.getGraphtNoTx().getRawGraph());

    Organization test = new Organization();
    test.setName("romeshell");
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

    issue = issueRepository.save(issue);
    repositoryService.createIssue(repo, issue);
    dbFactory.getGraph().commit();
    RestAssured.port = port;
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
    dbFactory.getGraph().drop();
  }

  @Test
  public void testAssign() {

    InputStream stream = ClassLoader.getSystemResourceAsStream("assigne.json");
    InputStream stream2 = ClassLoader.getSystemResourceAsStream("assigne.json");
    try {
      String content = IOUtils.toString(stream);
      String content1 = IOUtils.toString(stream2);
      given().body(content).when().post("/api/v1/github/events");
      given().body(content1).when().post("/api/v1/github/events");

      given().body(content).when().post("/api/v1/github/events");
      given().body(content1).when().post("/api/v1/github/events");

      Thread.sleep(5000);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    // when().get("/org/{name}", test.getName()).then().statusCode(HttpStatus.OK.value()).body("name", Matchers.is(test.getName()))
    // .body("id", Matchers.not(Matchers.isEmptyOrNullString())).body("codename", Matchers.is(test.getId()));

  }

  // @Test
  // public void testUnAssign() {
  //
  // // when().get("/org/{name}", test.getName()).then().statusCode(HttpStatus.OK.value()).body("name", Matchers.is(test.getName()))
  // // .body("id", Matchers.not(Matchers.isEmptyOrNullString())).body("codename", Matchers.is(test.getId()));
  //
  // }
}
