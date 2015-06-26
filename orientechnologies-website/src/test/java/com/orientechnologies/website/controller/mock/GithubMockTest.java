package com.orientechnologies.website.controller.mock;

import com.jayway.restassured.response.Response;
import com.orientechnologies.website.Application;
import com.orientechnologies.website.model.schema.dto.Comment;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.Label;
import com.orientechnologies.website.model.schema.dto.Repository;
import com.orientechnologies.website.model.schema.dto.web.IssueDTO;
import com.orientechnologies.website.services.impl.IssueServiceImpl;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.testng.Assert;

import java.util.*;

/**
 * Created by Enrico Risa on 24/06/15.
 */

@ActiveProfiles("test")
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = Application.class)
@WebAppConfiguration
@IntegrationTest("server.port:0")
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class GithubMockTest extends BaseMockTest {

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
    Response post = header(normalUser.getToken()).body(issueDTO).when().post("/api/v1/orgs/romeshell/issues");

    Assert.assertEquals(post.statusCode(), 200);
    Issue as = post.as(Issue.class);
    Assert.assertNotNull(as.getIid());
    Assert.assertNotNull(as.getNumber());
    Assert.assertEquals(as.getUser().getName(), normalUser.getUsername());
    Assert.assertEquals(as.getTitle(), issueDTO.getTitle());
    Assert.assertEquals(as.getBody(), issueDTO.getBody());

    try {
      Thread.sleep(MILLIS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    Response get = header(memberUser.getToken()).get("/api/v1/orgs/romeshell/issues/" + as.getIid());

    as = get.as(Issue.class);
    Assert.assertNotNull(as.getIid());
    Assert.assertNotNull(as.getNumber());

    Assert.assertEquals(as.getUser().getName(), normalUser.getUsername());
    Assert.assertEquals(as.getTitle(), issueDTO.getTitle());
    Assert.assertEquals(as.getBody(), issueDTO.getBody());
    Assert.assertEquals(as.getLabels().size(), 1);
    Assert.assertEquals(as.getLabels().iterator().next().getName(), "bug");

    get = header(memberUser.getToken()).get("/api/v1/repos/romeshell/gnome-shell-extension-aam/issues/" + as.getIid() + "/events");

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

    Response patch = header(memberUser.getToken()).body(params).when()
        .patch("/api/v1/repos/romeshell/gnome-shell-extension-aam/issues/1");

    Issue as = patch.as(Issue.class);

    Assert.assertEquals(patch.statusCode(), 200);

    try {
      Thread.sleep(MILLIS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Response get = header(memberUser.getToken()).get("/api/v1/orgs/romeshell/issues/" + as.getIid());
    as = get.as(Issue.class);
    Assert.assertNotNull(as.getAssignee());
    Assert.assertEquals(as.getAssignee().getName(), "maggiolo00");

    get = header(memberUser.getToken()).get("/api/v1/repos/romeshell/gnome-shell-extension-aam/issues/" + as.getIid() + "/events");

    List<Map> events = Arrays.asList(get.getBody().as(Map[].class));

    // scoped + label + assigne
    Assert.assertEquals(events.size(), 3);
  }

  @Test
  public void test3Comment() {

    Comment comment = new Comment();
    comment.setBody("Test");

    Response c = header(memberUser.getToken()).body(comment).when()
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
    Response get = header(memberUser.getToken()).get("/api/v1/repos/romeshell/gnome-shell-extension-aam/issues/1/events");

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

    Response c = header(memberUser.getToken()).body(labels).when()
        .post("/api/v1/repos/romeshell/gnome-shell-extension-aam/issues/1/labels");

    try {
      Thread.sleep(MILLIS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Assert.assertEquals(c.statusCode(), 200);

    Response get = header(memberUser.getToken()).get("/api/v1/orgs/romeshell/issues/1");
    Issue as = get.as(Issue.class);

    Assert.assertEquals(as.getLabels().size(), 2);
    get = header(memberUser.getToken()).get("/api/v1/repos/romeshell/gnome-shell-extension-aam/issues/1/events");

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
    Response get = header(memberUser.getToken()).get("/api/v1/repos/romeshell/gnome-shell-extension-aam/issues/1/events");

    List<Map> events = Arrays.asList(get.getBody().as(Map[].class));

    // scoped + label + assigne + comment + label + comment + remove wait for reply
    Assert.assertEquals(events.size(), 7);

    get = header(memberUser.getToken()).get("/api/v1/orgs/romeshell/issues/1");
    Issue as = get.as(Issue.class);

    Assert.assertEquals(as.getLabels().size(), 1);
  }

  @Test
  public void test5close() {

    Map<String, Object> params = new HashMap<String, Object>() {
      {
        put("state", "closed");
      }
    };
    Response c = header(memberUser.getToken()).body(params).when()
        .patch("/api/v1/repos/romeshell/gnome-shell-extension-aam/issues/1");

    Assert.assertEquals(c.statusCode(), 200);

    try {
      Thread.sleep(MILLIS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Response get = header(memberUser.getToken()).get("/api/v1/orgs/romeshell/issues/1");
    Issue as = get.as(Issue.class);

    Assert.assertEquals(as.getState(), "CLOSED");

    get = header(memberUser.getToken()).get("/api/v1/repos/romeshell/gnome-shell-extension-aam/issues/1/events");

    List<Map> events = Arrays.asList(get.getBody().as(Map[].class));

    // scoped + label + assigne + comment + label + comment + remove wait for reply + close
    Assert.assertEquals(events.size(), 8);
  }

  @Test
  public void test6reopen() {

    Map<String, Object> params = new HashMap<String, Object>() {
      {
        put("state", "open");
      }
    };
    Response c = header(memberUser.getToken()).body(params).when()
        .patch("/api/v1/repos/romeshell/gnome-shell-extension-aam/issues/1");

    Assert.assertEquals(c.statusCode(), 200);

    try {
      Thread.sleep(MILLIS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    Response get = header(memberUser.getToken()).get("/api/v1/orgs/romeshell/issues/1");
    Issue as = get.as(Issue.class);

    Assert.assertEquals(as.getState(), "OPEN");

    get = header(memberUser.getToken()).get("/api/v1/repos/romeshell/gnome-shell-extension-aam/issues/1/events");

    List<Map> events = Arrays.asList(get.getBody().as(Map[].class));

    // scoped + label + assigne + comment + label + comment + remove wait for reply + close + reopen
    Assert.assertEquals(events.size(), 9);
  }

}
