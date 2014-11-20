package com.orientechnologies.website.github;

import com.orientechnologies.orient.core.record.impl.ODocument;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

/**
 * Created by Enrico Risa on 05/11/14.
 */
public class GIssue extends GEntity {
  protected GIssue(GitHub github, GRepo owner, String content) {
    super(github, owner, content);
  }

  public static GIssue fromDoc(ODocument doc) {
    return new GIssue(null, null, doc.toJSON());
  }

  public Integer getNumber() {
    return get("number");
  }

  public String getBody() {
    return get("body");
  }

  public String getTitle() {
    return get("title");
  }

  public GIssueState getState() {
    return GIssueState.valueOf(((String) get("state")).toUpperCase());
  }

  public List<GLabel> getLabels() {
    Collection<ODocument> docs = get("labels");
    List<GLabel> labels = new ArrayList<GLabel>();

    for (ODocument doc : docs) {
      labels.add(new GLabel(github, this, doc.toJSON()));
    }
    return labels;
  }

  public GMilestone getMilestone() {
    ODocument doc = get("milestone");

    return doc != null ? new GMilestone(github, this, doc.toJSON()) : null;
  }

  public Date getCreatedAt() {
    return toDate((String) get("created_at"));
  }

  public Date getClosedAt() {
    return toDate((String) get("closed_at"));

  }

  public GUser getAssignee() {
    ODocument doc = get("assignee");

    return doc != null ? new GUser(github, this, doc.toJSON()) : null;
  }

  public List<GComment> getComments() throws IOException {
    String content = github.REQUEST.uri().path(getBaseUrl() + "/comments").back().method("GET")
        .header("Authorization", String.format("token %s", github.token)).fetch().body();
    JSONArray array = new JSONArray(content);

    List<GComment> comments = new ArrayList<GComment>();
    String tmp;
    for (int i = 0; i < array.length(); i++) {
      JSONObject obj = array.getJSONObject(i);
      tmp = obj.toString();
      ODocument document = new ODocument().fromJSON(tmp, "noMap");
      GComment g = new GComment(github, this, tmp);
      comments.add(g);
    }
    return comments;
  }

  public GUser getUser() {
    ODocument doc = get("user");
    return doc != null ? new GUser(github, this, doc.toJSON()) : null;

  }

  @Override
  protected String getBaseUrl() {
    return owner.getBaseUrl() + "/issues/" + getNumber();
  }

  public List<GEvent> getEvents() throws IOException {
    String content = github.REQUEST.uri().path(getBaseUrl() + "/events").back().method("GET")
        .header("Authorization", String.format("token %s", github.token)).fetch().body();
    JSONArray array = new JSONArray(content);

    List<GEvent> events = new ArrayList<GEvent>();
    String tmp;
    for (int i = 0; i < array.length(); i++) {
      JSONObject obj = array.getJSONObject(i);
      tmp = obj.toString();
      ODocument document = new ODocument().fromJSON(tmp, "noMap");
      GEvent g = new GEvent(github, this, tmp);
      events.add(g);
    }
    return events;
  }
}
