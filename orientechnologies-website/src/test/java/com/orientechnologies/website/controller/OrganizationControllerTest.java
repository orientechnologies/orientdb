package com.orientechnologies.website.controller;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.specification.RequestSpecification;
import com.orientechnologies.website.Application;
import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.model.schema.dto.Contract;
import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.model.schema.dto.Organization;
import com.orientechnologies.website.repository.OrganizationRepository;
import com.orientechnologies.website.repository.UserRepository;
import com.orientechnologies.website.services.OrganizationService;
import org.hamcrest.Matchers;
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

  public static boolean   dbInit = false;
  @Autowired
  OrganizationRepository  repository;

  @Autowired
  OrganizationService organizationService;

  @Autowired
  UserRepository          userRepository;

  @Autowired
  OrientDBFactory         dbFactory;
  @Value("${local.server.port}")
  int                     port;

  Organization            test;

  OUser                   user;

  @Before
  public void setUp() {

    if (!dbInit) {
      user = new OUser();

      user.setName("Enrico");
      user.setToken("testToken");

      test = new Organization();
      test.setName("Organization Test");

      test = repository.save(test);
      user = userRepository.save(user);

      organizationService.createMembership(test, user);
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
        .body("name", Matchers.is(test.getName())).body("id", Matchers.not(Matchers.isEmptyOrNullString()))
        .body("codename", Matchers.is(test.getId()));

  }

  @Test
  public void testAddContract() {

    Contract contract = new Contract();
    contract.setName("Developer Support");
    header().given().body(contract).when().post("/api/v1/orgs/fake/contracts").then().statusCode(HttpStatus.OK.value())
        .body("name", Matchers.is("Developer Support"));
    header().when().get("/api/v1/orgs/fake/contracts").then().statusCode(HttpStatus.OK.value());

  }

  public static RequestSpecification header() {
    return given().header("X-AUTH-TOKEN", "testToken");
  }
}
