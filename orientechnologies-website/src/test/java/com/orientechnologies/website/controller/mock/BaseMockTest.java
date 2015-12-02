package com.orientechnologies.website.controller.mock;

import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import com.jayway.restassured.specification.RequestSpecification;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.configuration.GitHubConfiguration;
import com.orientechnologies.website.model.schema.HasPriority;
import com.orientechnologies.website.model.schema.dto.*;
import com.orientechnologies.website.model.schema.dto.web.hateoas.ScopeDTO;
import com.orientechnologies.website.repository.*;
import com.orientechnologies.website.services.IssueService;
import com.orientechnologies.website.services.OrganizationService;
import com.orientechnologies.website.services.RepositoryService;
import com.orientechnologies.website.services.impl.OrganizationServiceImpl;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.joda.time.DateTime;
import org.junit.Before;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;

import java.util.*;

import static com.jayway.restassured.RestAssured.given;

/**
 * Created by Enrico Risa on 26/06/15.
 */
public class BaseMockTest {

  @Value("${local.server.port}")
  int                        port;
  @Autowired
  GitHubConfiguration        gitHubConfiguration;

  public static final int    MILLIS = 3000;
  public static boolean      dbInit = false;

  public static Organization test;
  public static OUser        botUser;
  public static OUser        memberUser;
  public static OUser        supportUser;
  public static OUser        normalUser;
  public static OUser        clientUser;
  public Client              client;
  public Client              clientSupport;
  public static Scope        scope;
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
  LabelRepository            labelRepository;
  @Autowired
  OrganizationService        organizationService;
  @Autowired
  OrientDBFactory            dbFactory;

  @Autowired
  ContractRepository         contractRepository;

  @Autowired
  ClientRepository           clientRepository;

  @Before
  public void setUp() {

    if (!dbInit) {
      gitHubConfiguration.setPort(port);
      test = repository.save(test);
      initUsers();

      registerClients();

      registerPriority("Critical", 1);
      registerPriority("High", 2);
      registerPriority("Medium", 3);
      registerPriority("Low", 4);

      Repository repo = repositoryService.createRepo("gnome-shell-extension-aam", null);

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
      organizationService.registerBot("romeshell", botUser.getName());

      dbFactory.getGraph().commit();

      RestAssured.port = port;
      dbInit = true;
    }

  }

  protected Response labelIssue(OUser actor, Integer issue, final String label) {
    List<String> labels = new ArrayList<String>() {
      {
        add(label);
      }
    };

    Response c = header(actor.getToken()).body(labels).when()
        .post("/api/v1/repos/romeshell/gnome-shell-extension-aam/issues/" + issue + "/labels");

    try {
      Thread.sleep(MILLIS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Assert.assertEquals(c.statusCode(), 200);
    return c;
  }

  protected Response commentIssue(OUser actor, Integer issue, Comment comment) {
    Response c = header(actor.getToken()).body(comment).when()
        .post("/api/v1/repos/romeshell/gnome-shell-extension-aam/issues/" + issue + "/comments");

    try {
      Thread.sleep(MILLIS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Assert.assertEquals(c.statusCode(), 200);
    return c;
  }

  protected List<Map> events(OUser actor, Integer issue) {

    Response get = header(actor.getToken()).get("/api/v1/repos/romeshell/gnome-shell-extension-aam/issues/1/events");

    Assert.assertEquals(get.statusCode(), 200);
    List<Map> events = Arrays.asList(get.getBody().as(Map[].class));

    return events;
  }

  protected Issue getSigleIssue(OUser actor, Integer issue) {
    Response get = header(memberUser.getToken()).get("/api/v1/orgs/romeshell/issues/" + issue);

    Issue as = get.as(Issue.class);
    return as;
  }

  protected Issue assigneIssue(OUser actor, Integer issue, final String assignee) {
    Map<String, Object> params = new HashMap<String, Object>() {
      {
        put("assignee", assignee);
      }
    };
    return patchSingleIssue(actor, issue, params);
  }

  public Issue closeIssue(OUser actor, Integer isssue) {
    Map<String, Object> params = new HashMap<String, Object>() {
      {
        put("state", "closed");
      }
    };
    return patchSingleIssue(actor, isssue, params);
  }

  public Issue reopenIssue(OUser actor, Integer isssue) {
    Map<String, Object> params = new HashMap<String, Object>() {
      {
        put("state", "open");
      }
    };
    return patchSingleIssue(actor, isssue, params);
  }

  protected Issue patchSingleIssue(OUser actor, Integer issue, Map<String, Object> params) {

    Response patch = header(memberUser.getToken()).body(params).when()
        .patch("/api/v1/repos/romeshell/gnome-shell-extension-aam/issues/" + issue);

    Issue as = patch.as(Issue.class);

    Assert.assertEquals(patch.statusCode(), 200);
    try {
      Thread.sleep(MILLIS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return as;
  }

  private void registerPriority(String name, Integer number) {
    ODocument priority = new ODocument(Priority.class.getSimpleName());

    priority.field("name", name);
    priority.field("number", number);
    OrientGraph graph = dbFactory.getGraph();
    OrientVertex prioVertex = graph.getVertex(priority);
    OrientVertex orgVertex = graph.getVertex(new ORecordId(test.getId()));
    orgVertex.addEdge(HasPriority.class.getSimpleName(), prioVertex);

  }

  private void registerClients() {
    clientSupport = new Client();
    clientSupport.setClientId(1);
    clientSupport.setName("Test Support");
    clientSupport.setSupport(true);

    client = new Client();
    client.setClientId(2);
    client.setName("Test Client");

    client = clientRepository.save(client);
    configureClient(client);
    clientSupport = clientRepository.save(clientSupport);
    try {
      OrganizationServiceImpl impl = getTargetObject(organizationService, OrganizationServiceImpl.class);
      impl.createClientRelationship(test, clientSupport);
      impl.createClientRelationship(test, client);
      impl.createClientMembership(client, clientUser);
      impl.createClientMembership(clientSupport, supportUser);
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  private void configureClient(Client client) {
    Contract contract = new Contract();

    contract.getBusinessHours().add("10:00-19:00");
    contract.getBusinessHours().add("10:00-19:00");
    contract.getBusinessHours().add("10:00-19:00");
    contract.getBusinessHours().add("10:00-19:00");
    contract.getBusinessHours().add("10:00-19:00");

    contract.getSlas().put(1, 2);
    contract.getSlas().put(2, 4);
    contract.getSlas().put(3, 8);
    contract.getSlas().put(4, 24);

    contract = contractRepository.save(contract);

    try {
      OrganizationServiceImpl impl = getTargetObject(organizationService, OrganizationServiceImpl.class);
      impl.createContractRelationship(test, contract);
      DateTime today = new DateTime(new Date());
      impl.createContractClientRelationship(client, contract, today.minusDays(10).toDate(), today.plusDays(10).toDate());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void initUsers() {
    memberUser = new OUser();
    memberUser.setName("maggiolo00");
    memberUser.setToken("testToken");
    memberUser.setId(1l);
    memberUser = userRepository.save(memberUser);

    botUser = new OUser();
    botUser.setName("testBot");
    botUser.setToken("testBot");
    botUser.setId(2l);
    botUser = userRepository.save(botUser);

    normalUser = new OUser();
    normalUser.setName("testNormal");
    normalUser.setToken("testNormal");
    normalUser.setId(3l);
    normalUser = userRepository.save(normalUser);

    supportUser = new OUser();
    supportUser.setName("testSupport");
    supportUser.setToken("testSupport");
    supportUser.setId(4l);
    supportUser = userRepository.save(supportUser);

    clientUser = new OUser();
    clientUser.setName("testClient");
    clientUser.setToken("testClient");
    clientUser.setId(5l);
    clientUser = userRepository.save(clientUser);

    organizationService.createMembership(test, memberUser);

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
