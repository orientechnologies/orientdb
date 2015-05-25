package com.orientechnologies.website.controller;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import com.jayway.restassured.specification.RequestSpecification;
import com.orientechnologies.website.Application;
import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.model.schema.dto.Client;
import com.orientechnologies.website.model.schema.dto.Contract;
import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.model.schema.dto.Organization;
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

  public static RequestSpecification header() {
    return given().header("X-AUTH-TOKEN", "testToken").given().contentType("application/json");
  }
}
