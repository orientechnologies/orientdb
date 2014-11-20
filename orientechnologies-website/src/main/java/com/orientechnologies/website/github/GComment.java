package com.orientechnologies.website.github;

import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Date;

/**
 * Created by Enrico Risa on 05/11/14.
 */
public class GComment extends GEntity {

  protected GComment(GitHub github, GEntity owner, String content) {
    super(github, owner, content);
  }

  public Integer getId() {
    return get("id");
  }

  public String getBody() {
    return get("body");
  }

  public GUser getUser() {
    return toUser((ODocument) get("user"));
  }

  public Date getCreatedAt() {
    return toDate((String) get("created_at"));
  }

  @Override
  protected String getBaseUrl() {
    return null;
  }

  public Date getUpdatedAt() {
    return toDate((String) get("updated_at"));
  }

  public static GComment fromDoc(ODocument comment) {
    return new GComment(null, null, comment.toJSON());
  }
}
