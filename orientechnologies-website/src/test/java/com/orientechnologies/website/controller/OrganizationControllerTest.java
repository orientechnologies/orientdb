package com.orientechnologies.website.controller;

import com.jayway.restassured.RestAssured;
import com.orientechnologies.website.Application;
import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.model.schema.OSiteSchema;
import com.orientechnologies.website.model.schema.dto.Organization;
import com.orientechnologies.website.repository.OrganizationRepository;
import org.hamcrest.Matchers;
import org.junit.After;
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

import static com.jayway.restassured.RestAssured.when;

/**
 * Created by Enrico Risa on 17/10/14.
 */

@ActiveProfiles("test")
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = Application.class)
@WebAppConfiguration
@IntegrationTest("server.port:0")
public class OrganizationControllerTest {

  @Autowired
  OrganizationRepository repository;

  @Autowired
  OrientDBFactory        dbFactory;
  @Value("${local.server.port}")
  int                    port;

  Organization           test;

  @Before
  public void setUp() {

    OSiteSchema.fillSchema(dbFactory.getGraphtNoTx().getRawGraph());

    test = new Organization();
    test.setName("Organization Test");

    repository.save(test);
    dbFactory.getGraph().commit();
    RestAssured.port = port;
  }

  @After
  public void deInit() {
    dbFactory.getGraph().drop();
  }

  @Test
  public void testFetchOrganization() {

    when().get("/org/{name}", test.getName()).then().statusCode(HttpStatus.OK.value()).body("name", Matchers.is(test.getName()))
        .body("id", Matchers.not(Matchers.isEmptyOrNullString())).body("codename", Matchers.is(test.getId()));

  }

  @Test
  public void testAddMember() {
    when().get("/org/{name}", "fake").then().statusCode(HttpStatus.OK.value());
  }
}
