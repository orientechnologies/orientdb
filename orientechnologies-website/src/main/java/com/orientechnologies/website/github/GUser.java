package com.orientechnologies.website.github;

/**
 * Created by Enrico Risa on 05/11/14.
 */
public class GUser extends GEntity {

  protected GUser(GitHub github, GEntity owner, String content) {
    super(github, owner, content);
  }

  public String getLogin() {
    return get("login");
  }

  public String getEmail() {
    return get("email");
  }

  public Long getId() {
    return get("id");
  }

  @Override
  protected String getBaseUrl() {
    return null;
  }
}
