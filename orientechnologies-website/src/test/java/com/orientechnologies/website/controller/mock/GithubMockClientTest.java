package com.orientechnologies.website.controller.mock;

import com.jayway.restassured.response.Response;
import com.orientechnologies.website.Application;
import com.orientechnologies.website.model.schema.dto.Comment;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.Label;
import com.orientechnologies.website.model.schema.dto.Repository;
import com.orientechnologies.website.model.schema.dto.web.MockIssueDTO;
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

import java.util.List;
import java.util.Map;

/**
 * Created by Enrico Risa on 24/06/15.
 */

@ActiveProfiles("test")
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = Application.class)
@WebAppConfiguration
@IntegrationTest("server.port:0")
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class GithubMockClientTest extends BaseMockTest {

  protected void createLabel(String name, Repository repo) {
    Label label = new Label();
    label.setName(name);

    label = labelRepository.save(label);
    repositoryService.addLabel(repo, label);

  }

  @Test
  public void test1OpenIssue() {

    MockIssueDTO issueDTO = new MockIssueDTO();
    issueDTO.setTitle("error");
    issueDTO.setBody("Error");
    issueDTO.setType("bug");
    issueDTO.setPriority(1);
    issueDTO.setClient(client.getClientId());
    issueDTO.setScope(scope.getNumber());
    Response post = header(clientUser.getToken()).body(issueDTO).when().post("/api/v1/orgs/romeshell/issues");

    Assert.assertEquals(post.statusCode(), 200);
    Issue as = post.as(Issue.class);
    Assert.assertNotNull(as.getIid());
    Assert.assertNotNull(as.getNumber());
    Assert.assertNotNull(as.getDueTime());
    Assert.assertEquals(as.getUser().getName(), clientUser.getName());
    Assert.assertEquals(as.getTitle(), issueDTO.getTitle());
    Assert.assertEquals(as.getBody(), issueDTO.getBody());

    try {
      Thread.sleep(MILLIS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    as = getSigleIssue(memberUser, as.getIid().intValue());

    Assert.assertNotNull(as.getIid());
    Assert.assertNotNull(as.getNumber());

    Assert.assertEquals(as.getUser().getName(), clientUser.getName());
    Assert.assertEquals(as.getTitle(), issueDTO.getTitle());
    Assert.assertEquals(as.getBody(), issueDTO.getBody());
    Assert.assertEquals(as.getLabels().size(), 1);
    Assert.assertEquals(as.getLabels().iterator().next().getName(), "bug");

    List<Map> events = events(memberUser, as.getIid().intValue());

    // scoped + label + priority + sla
    Assert.assertEquals(events.size(), 4);
  }

  @Test
  public void test2CommentIssue() {

    Comment comment = new Comment();
    comment.setBody("Test");

    // Comment client
    commentIssue(clientUser, 1, comment);

    Issue singleIssue = getSigleIssue(memberUser, 1);

    Assert.assertNotNull(singleIssue.getDueTime());

    // Comment member
    commentIssue(memberUser, 1, comment);

    // Label wait for reply member
    labelIssue(memberUser, 1, IssueServiceImpl.WAIT_FOR_REPLY);

    // Ops forgot something another comment
    commentIssue(memberUser, 1, comment);

    singleIssue = getSigleIssue(memberUser, 1);

    Assert.assertNull(singleIssue.getDueTime());

    // bug + wait for reply
    Assert.assertEquals(singleIssue.getLabels().size(), 2);

    // Comment normal user
    commentIssue(normalUser, 1, comment);

    singleIssue = getSigleIssue(memberUser, 1);

    // should not remove any label
    Assert.assertEquals(singleIssue.getLabels().size(), 2);

    Assert.assertNull(singleIssue.getDueTime());

    // Client responds

    commentIssue(clientUser, 1, comment);

    singleIssue = getSigleIssue(memberUser, 1);

    // should remove wait for reply
    Assert.assertEquals(singleIssue.getLabels().size(), 1);

    Assert.assertNotNull(singleIssue.getDueTime());

    List<Map> events = events(memberUser, 1);

    // scoped
    // label
    // priority
    // sla
    // 2 comment
    // label
    // stop sla
    // 2 x comment
    // removed label
    // comment client
    // sla restarted
    Assert.assertEquals(events.size(), 13);
  }

  @Test
  public void test3assignAndProgress() {

    assigneIssue(memberUser, 1, memberUser.getName());

    labelIssue(memberUser, 1, IssueServiceImpl.IN_PROGRESS);

    List<Map> events = events(memberUser, 1);

    // scoped
    // label
    // priority
    // sla
    // 2 comment
    // label
    // stop sla
    // 2 x comment
    // removed label
    // comment client
    // sla restarted
    // assign
    // label in progress
    // sla stopped

    Assert.assertEquals(events.size(), 16);

    Issue singleIssue = getSigleIssue(memberUser, 1);

    Assert.assertNull(singleIssue.getDueTime());
  }

  @Test
  public void test4CloseReopen() {

    closeIssue(memberUser, 1);

    Issue issue = getSigleIssue(memberUser, 1);

    Assert.assertNull(issue.getDueTime());

    Assert.assertEquals(issue.getState(), "CLOSED");

    reopenIssue(clientUser, 1);

    issue = getSigleIssue(memberUser, 1);

    Assert.assertEquals(issue.getState(), "OPEN");

    Assert.assertNotNull(issue.getDueTime());

    List<Map> events = events(memberUser, 1);

    Assert.assertEquals(events.size(), 19);

  }

  @Test
  public void test5CloseComment() {

    closeIssue(memberUser, 1);

    Issue issue = getSigleIssue(memberUser, 1);

    Assert.assertNull(issue.getDueTime());

    Comment comment = new Comment();
    comment.setBody("Test");

    // Comment client
    commentIssue(clientUser, 1, comment);

    issue = getSigleIssue(memberUser, 1);

    Assert.assertNull(issue.getDueTime());

    List<Map> events = events(memberUser, 1);

    Assert.assertEquals(events.size(), 22);
  }
}
