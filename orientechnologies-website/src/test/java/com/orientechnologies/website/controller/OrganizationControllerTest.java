package com.orientechnologies.website.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import com.jayway.restassured.specification.RequestSpecification;
import com.orientechnologies.website.Application;
import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.model.schema.dto.*;
import com.orientechnologies.website.repository.OrganizationRepository;
import com.orientechnologies.website.repository.UserRepository;
import com.orientechnologies.website.services.OrganizationService;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static com.jayway.restassured.RestAssured.given;

/**
 * Created by Enrico Risa on 17/10/14.
 */

@ActiveProfiles("test")
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = Application.class)
@WebAppConfiguration
@IntegrationTest("server.port:0")
public class OrganizationControllerTest {

  public static boolean  dbInit = false;
  @Autowired
  OrganizationRepository repository;

  @Autowired
  OrganizationService    organizationService;

  @Autowired
  UserRepository         userRepository;

  @Autowired
  OrientDBFactory        dbFactory;
  @Value("${local.server.port}")
  int                    port;

  static Organization    test;

  static OUser           user;

  @Before
  public void setUp() {

    if (!dbInit) {
      user = new OUser();

      user.setName("Enrico");
      user.setToken("testToken");

      test = new Organization();
      test.setName("fake");

      test = repository.save(test);
      user = userRepository.save(user);

      Client client = new Client();
      client.setClientId(1);
      client.setName("Test client");

      organizationService.createMembership(test, user);
      organizationService.registerClient(test.getName(), client);
      dbFactory.getGraph().commit();
      RestAssured.port = port;
      dbInit = true;
    }
  }

  // @AfterClass
  // public void deInit() {
  // dbFactory.getGraph().drop();
  // }

  @Test
  public void testFetchOrganization() {

    header().when().get("/api/v1/orgs/{name}", test.getName()).then().statusCode(HttpStatus.OK.value())
        .body("name", Matchers.is(test.getName())).body("id", Matchers.not(Matchers.isEmptyOrNullString()));

  }

  @Test
  public void testAddAndAssociateContract() {

    Contract contract = new Contract();
    contract.setName("Developer Support");
    contract.getBusinessHours().add("10:00-19:00");
    contract.getBusinessHours().add("10:00-19:00");
    contract.getBusinessHours().add("10:00-19:00");
    contract.getBusinessHours().add("10:00-19:00");
    contract.getBusinessHours().add("10:00-19:00");

    contract.getSlas().put(1, 2);
    contract.getSlas().put(2, 4);
    contract.getSlas().put(3, 8);
    contract.getSlas().put(4, 24);
    Response post = header().given().body(contract).when().post("/api/v1/orgs/fake/contracts");

    Assert.assertEquals(post.statusCode(), 200);
    Contract response = post.getBody().as(Contract.class);

    String uuid = response.getUuid();
    Assert.assertNotNull(uuid);
    Assert.assertEquals(contract.getName(), response.getName());
    Assert.assertEquals(contract.getBusinessHours(), response.getBusinessHours());
    Assert.assertEquals(contract.getSlas(), response.getSlas());

    post = header().when().get("/api/v1/orgs/fake/contracts");

    Assert.assertEquals(post.statusCode(), 200);

    List<Contract> contracts = Arrays.asList(post.getBody().as(Contract[].class));

    Assert.assertEquals(contracts.size(), 1);

    response = contracts.iterator().next();

    Assert.assertEquals(contract.getName(), response.getName());
    Assert.assertEquals(contract.getBusinessHours(), response.getBusinessHours());
    Assert.assertEquals(contract.getSlas(), response.getSlas());

    Date fromDate = new Date();
    Date toDate = new Date();
    contract.setUuid(uuid);
    contract.setFrom(fromDate);
    contract.setTo(toDate);

    post = header().given().body(contract).when().post("/api/v1/orgs/fake/clients/1/contracts");

    Assert.assertEquals(200, post.statusCode());

    response = post.getBody().as(Contract.class);

    Assert.assertEquals(contract.getName(), response.getName());
    Assert.assertEquals(contract.getBusinessHours(), response.getBusinessHours());
    Assert.assertEquals(contract.getSlas(), response.getSlas());

    post = header().when().get("/api/v1/orgs/fake/clients/1/contracts");

    Assert.assertEquals(200, post.statusCode());

    contracts = Arrays.asList(post.getBody().as(Contract[].class));

    Assert.assertEquals(contracts.size(), 1);

    response = contracts.iterator().next();

    Assert.assertEquals(contract.getName(), response.getName());
    Assert.assertEquals(contract.getBusinessHours(), response.getBusinessHours());
    Assert.assertEquals(contract.getSlas(), response.getSlas());
    Assert.assertEquals(contract.getFrom(), response.getFrom());
    Assert.assertEquals(contract.getTo(), response.getTo());

  }

  @Test
  public void testTopic() throws IOException {

    final Tag tag = testCreateTag();

    Topic t = new Topic();
    t.setTitle("FirstTest");
    t.setBody("Test body");
    t.setTags(new ArrayList<Tag>() {
      {
        add(tag);
      }
    });
    Response post = header().given().body(t).when().post("/api/v1/orgs/fake/topics");

    Assert.assertEquals(200, post.statusCode());

    Topic response = post.getBody().as(Topic.class);

    Assert.assertNotNull(response);
    Assert.assertNotNull(response.getNumber());
    Assert.assertNotNull(response.getUuid());
    Assert.assertNotNull(response.getUser());
    Assert.assertNotNull(response.getCreatedAt());

    Assert.assertEquals(t.getTitle(), response.getTitle());
    Assert.assertEquals(t.getBody(), response.getBody());
    Assert.assertEquals(1, t.getTags().size());

    post = header().given().when().delete("/api/v1/orgs/fake/topics/" + response.getNumber() + "/tags/" + tag.getUuid());

    Assert.assertEquals(200, post.statusCode());

    post = header().given().when().get("/api/v1/orgs/fake/topics/" + response.getNumber());

    Assert.assertEquals(200, post.statusCode());

    response = post.getBody().as(Topic.class);

    Assert.assertNotNull(response);
    Assert.assertNotNull(response.getNumber());
    Assert.assertNotNull(response.getUuid());
    Assert.assertNotNull(response.getUser());
    Assert.assertNotNull(response.getCreatedAt());
    Assert.assertEquals(t.getTitle(), response.getTitle());
    Assert.assertEquals(t.getBody(), response.getBody());
    Assert.assertEquals(0, response.getTags().size());

    List<Tag> tags = new ArrayList<Tag>() {
      {
        add(tag);
      }
    };
    post = header().given().body(tags).when().post("/api/v1/orgs/fake/topics/" + response.getNumber() + "/tags");

    Assert.assertEquals(200, post.statusCode());

    post = header().given().when().get("/api/v1/orgs/fake/topics/" + response.getNumber());

    Assert.assertEquals(200, post.statusCode());

    response = post.getBody().as(Topic.class);

    Assert.assertNotNull(response);
    Assert.assertNotNull(response.getNumber());
    Assert.assertNotNull(response.getUuid());
    Assert.assertNotNull(response.getUser());
    Assert.assertNotNull(response.getCreatedAt());
    Assert.assertEquals(t.getTitle(), response.getTitle());
    Assert.assertEquals(t.getBody(), response.getBody());
    Assert.assertEquals(1, response.getTags().size());

    testDeleteTag(tag);

    for (int i = 0; i < 100; i++) {
      t = new Topic();
      t.setTitle("Test " + i);
      t.setBody("Test body " + i);
      post = header().given().body(t).when().post("/api/v1/orgs/fake/topics");
      Assert.assertEquals(200, post.statusCode());

    }

    post = header().given().when().get("/api/v1/orgs/fake/topics");
    Assert.assertEquals(200, post.statusCode());
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.readTree(post.getBody().asString());
    JsonNode content = jsonNode.get("content");
    List<Topic> topics = Arrays.asList(mapper.readValue(content.toString(), Topic[].class));
    Assert.assertEquals(10, topics.size());

    /* TEST PATCH + DELETE */

    Topic secondTest = topics.get(1);
    secondTest.setBody("Patched Test");
    post = header().given().body(secondTest).when().patch("/api/v1/orgs/fake/topics/" + secondTest.getNumber());
    Assert.assertEquals(200, post.statusCode());
    response = post.getBody().as(Topic.class);
    Assert.assertEquals(secondTest.getBody(), response.getBody());

    post = header().given().when().get("/api/v1/orgs/fake/topics/" + secondTest.getNumber());

    Assert.assertEquals(200, post.statusCode());
    response = post.getBody().as(Topic.class);
    Assert.assertEquals(secondTest.getBody(), response.getBody());

    post = header().given().when().delete("/api/v1/orgs/fake/topics/" + secondTest.getNumber());
    Assert.assertEquals(200, post.statusCode());

    post = header().given().when().get("/api/v1/orgs/fake/topics/" + topics.get(1).getNumber());
    Assert.assertEquals(404, post.statusCode());

    /* TEST POST COMMENT */

    Topic commentTest = topics.get(0);

    TopicComment comment = new TopicComment();
    comment.setBody("TestComment");

    post = header().given().body(comment).when().post("/api/v1/orgs/fake/topics/" + commentTest.getNumber() + "/comments");

    Assert.assertEquals(200, post.statusCode());

    TopicComment res = post.getBody().as(TopicComment.class);
    Assert.assertNotNull(res);
    Assert.assertNotNull(res.getUuid());
    Assert.assertNotNull(res.getUser());
    Assert.assertNotNull(res.getCreatedAt());
    Assert.assertEquals(comment.getBody(), res.getBody());

    post = header().given().when().get("/api/v1/orgs/fake/topics/" + commentTest.getNumber() + "/comments/" + res.getUuid());

    Assert.assertEquals(200, post.statusCode());

    res = post.getBody().as(TopicComment.class);
    Assert.assertNotNull(res);
    Assert.assertNotNull(res.getUuid());
    Assert.assertNotNull(res.getUser());
    Assert.assertNotNull(res.getCreatedAt());
    Assert.assertEquals(comment.getBody(), res.getBody());

    comment.setBody("PatchedComment");

    post = header().given().body(comment).when()
        .patch("/api/v1/orgs/fake/topics/" + commentTest.getNumber() + "/comments/" + res.getUuid());

    Assert.assertEquals(200, post.statusCode());
    res = post.getBody().as(TopicComment.class);
    Assert.assertNotNull(res);
    Assert.assertNotNull(res.getUuid());
    Assert.assertNotNull(res.getUser());
    Assert.assertNotNull(res.getCreatedAt());
    Assert.assertEquals(comment.getBody(), res.getBody());

    post = header().given().when().get("/api/v1/orgs/fake/topics/" + commentTest.getNumber() + "/comments");

    Assert.assertEquals(200, post.statusCode());
    List<TopicComment> comments = Arrays.asList(post.getBody().as(TopicComment[].class));

    Assert.assertEquals(1, comments.size());

    post = header().given().when().get("/api/v1/orgs/fake/topics/" + commentTest.getNumber() + "/comments/" + res.getUuid());

    Assert.assertEquals(200, post.statusCode());

    res = post.getBody().as(TopicComment.class);
    Assert.assertNotNull(res);
    Assert.assertNotNull(res.getUuid());
    Assert.assertNotNull(res.getUser());
    Assert.assertNotNull(res.getCreatedAt());
    Assert.assertEquals(comment.getBody(), res.getBody());

    post = header().given().when().delete("/api/v1/orgs/fake/topics/" + commentTest.getNumber() + "/comments/" + res.getUuid());

    Assert.assertEquals(200, post.statusCode());

    post = header().given().when().get("/api/v1/orgs/fake/topics/" + commentTest.getNumber() + "/comments/" + res.getUuid());
    Assert.assertEquals(404, post.statusCode());

    // /SEARCH

    post = header().given().param("q", "text:\"FirstTest\"").when().get("/api/v1/orgs/fake/topics");
    Assert.assertEquals(200, post.statusCode());

    jsonNode = mapper.readTree(post.getBody().asString());
    content = jsonNode.get("content");
    topics = Arrays.asList(mapper.readValue(content.toString(), Topic[].class));
    Assert.assertEquals(1, topics.size());

  }

  protected Tag testCreateTag() {

    Tag t = new Tag();
    t.setName("core");
    t.setColor("#ffffff");
    Response post = header().given().body(t).when().post("/api/v1/orgs/fake/tags");

    Assert.assertEquals(200, post.statusCode());

    Tag response = post.getBody().as(Tag.class);

    Assert.assertNotNull(response);
    Assert.assertNotNull(response.getUuid());
    Assert.assertNotNull(response.getName());
    Assert.assertNotNull(response.getColor());
    post = header().given().when().get("/api/v1/orgs/fake/tags");
    Assert.assertEquals(200, post.statusCode());
    List<Tag> tags = Arrays.asList(post.getBody().as(Tag[].class));
    Assert.assertEquals(1, tags.size());
    return response;
  }

  @Test
  public void testTags() {

    Tag t = testCreateTag();
    // TEST PATCH

    t.setName("core1");

    Response post = header().given().body(t).when().patch("/api/v1/orgs/fake/tags/" + t.getUuid());
    Assert.assertEquals(200, post.statusCode());
    Tag response = post.getBody().as(Tag.class);

    Assert.assertEquals(t.getName(), response.getName());

    testDeleteTag(t);

    post = header().given().when().get("/api/v1/orgs/fake/tags");
    Assert.assertEquals(200, post.statusCode());

    List<Tag> tags = Arrays.asList(post.getBody().as(Tag[].class));

    Assert.assertEquals(0, tags.size());

    // TEST DELETE
  }

  private void testDeleteTag(Tag t) {
    Response post;
    post = header().given().when().delete("/api/v1/orgs/fake/tags/" + t.getUuid());
    Assert.assertEquals(200, post.statusCode());
  }

  public static RequestSpecification header() {
    return given().header("X-AUTH-TOKEN", "testToken").given().contentType("application/json");
  }
}
