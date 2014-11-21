package com.orientechnologies.website.github;

import com.orientechnologies.orient.core.record.impl.ODocument;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Enrico Risa on 05/11/14.
 */
public class GRepo extends GEntity {

  protected GRepo(GitHub gitHub, String content) {
    super(gitHub, null, content);
  }

  @Override
  protected String getBaseUrl() {
    return "/repos/" + _local.field("full_name");
  }

  public String getName() {
    return get("name");
  }

  public String getDescription() {
    return get("description");
  }

  public String getFullName() {
    return _local.field("full_name");
  }

  public List<GIssue> getIssues(GIssueState state) throws IOException {
    String content = github.REQUEST.uri().path(getBaseUrl() + "/issues").back().method("GET")
        .header("Authorization", String.format("token %s", github.token)).fetch().body();
    JSONArray array = new JSONArray(content);

    List<GIssue> issues = new ArrayList<GIssue>();
    String tmp;
    for (int i = 0; i < array.length(); i++) {
      JSONObject obj = array.getJSONObject(i);
      tmp = obj.toString();
      ODocument document = new ODocument().fromJSON(tmp, "noMap");
      GIssue g = new GIssue(github, this, tmp);
      issues.add(g);
    }
    return issues;
  }

  public GIssue getIssue(Integer number) throws IOException {
    String content = github.REQUEST.uri().path(getBaseUrl() + "/issues/" + number).back().method("GET")
        .header("Authorization", String.format("token %s", github.token)).fetch().body();
    GIssue g = new GIssue(github, this, content);
    return g;
  }

  public GIssue openIssue(String content) throws IOException {
    String res = GitHub.REQUEST.uri().path(getBaseUrl() + "/issues").back().body().set(content).back().method("POST")
        .header("Authorization", String.format("token %s", github.token)).fetch().body();
    return new GIssue(github, this, res);

  }

  public List<GLabel> changeIssueLabels(Integer number, String content) throws IOException {
    String res = GitHub.REQUEST.uri().path(getBaseUrl() + "/issues/" + number + "/labels").back().body().set(content).back()
        .method("POST").header("Authorization", String.format("token %s", github.token)).fetch().body();
    JSONArray array = new JSONArray(res);
    List<GLabel> issues = new ArrayList<GLabel>();
    String tmp;
    for (int i = 0; i < array.length(); i++) {
      JSONObject obj = array.getJSONObject(i);
      tmp = obj.toString();
      GLabel g = new GLabel(github, this, tmp);
      issues.add(g);
    }
    return issues;

  }

  public void removeIssueLabel(Integer number, String label) throws IOException {
    String res = GitHub.REQUEST.uri().path(getBaseUrl() + "/issues/" + number + "/labels/" + label).back().method("DELETE")
        .header("Authorization", String.format("token %s", github.token)).fetch().body();

  }

  public GComment commentIssue(Integer number, String content) throws IOException {
    String res = GitHub.REQUEST.uri().path(getBaseUrl() + "/issues/" + number + "/comments").back().body().set(content).back()
        .method("POST").header("Authorization", String.format("token %s", github.token)).fetch().body();
    return new GComment(github, this, res);

  }

  public GIssue patchIssue(Integer number, String content) throws IOException {
    String res = GitHub.REQUEST.uri().path(getBaseUrl() + "/issues/" + number).back().body().set(content).back().method("PATCH")
        .header("Authorization", String.format("token %s", github.token)).fetch().body();
    return new GIssue(github, this, res);

  }

  public List<GLabel> getLabels() throws IOException {
    String content = github.REQUEST.uri().path(getBaseUrl() + "/labels").back().method("GET")
        .header("Authorization", String.format("token %s", github.token)).fetch().body();
    JSONArray array = new JSONArray(content);

    List<GLabel> issues = new ArrayList<GLabel>();
    String tmp;
    for (int i = 0; i < array.length(); i++) {
      JSONObject obj = array.getJSONObject(i);
      tmp = obj.toString();
      GLabel g = new GLabel(github, this, tmp);
      issues.add(g);
    }
    return issues;
  }

  public List<GMilestone> getMilestones() throws IOException {
    String content = github.REQUEST.uri().path(getBaseUrl() + "/milestones").back().method("GET")
        .header("Authorization", String.format("token %s", github.token)).fetch().body();
    JSONArray array = new JSONArray(content);

    List<GMilestone> issues = new ArrayList<GMilestone>();
    String tmp;
    for (int i = 0; i < array.length(); i++) {
      JSONObject obj = array.getJSONObject(i);
      tmp = obj.toString();
      ODocument document = new ODocument().fromJSON(tmp, "noMap");
      GMilestone g = new GMilestone(github, this, tmp);
      issues.add(g);
    }
    return issues;
  }
}
