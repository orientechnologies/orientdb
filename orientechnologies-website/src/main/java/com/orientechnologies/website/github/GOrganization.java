package com.orientechnologies.website.github;

import java.io.IOException;

/**
 * Created by Enrico Risa on 05/11/14.
 */
public class GOrganization extends GEntity {

  protected GOrganization(GitHub owner, String content) {
    super(owner, null, content);
  }

  @Override
  protected String getBaseUrl() {
    return null;
  }

  public boolean hasMember(String username) throws IOException {
    return false;
  }

  public String getLogin() {
    return get("login");
  }

  public String getName() {
    return null;
  }

}
