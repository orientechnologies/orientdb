package com.orientechnologies.website.github;

import java.util.Date;

/**
 * Created by Enrico Risa on 06/11/14.
 */
public class GEvent extends GEntity {

  protected GEvent(GitHub github, GEntity owner, String content) {
    super(github, owner, content);
  }

  public Integer getId() {
    return get("id");
  }

  public Date getCreatedAt() {
    return toDate((String) get("created_at"));
  }

  @Override
  protected String getBaseUrl() {
    return null;
  }
}
