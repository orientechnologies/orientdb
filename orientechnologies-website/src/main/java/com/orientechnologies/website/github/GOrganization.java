package com.orientechnologies.website.github;

import java.io.IOException;

/**
 * Created by Enrico Risa on 05/11/14.
 */
public class GOrganization extends GEntity {

  protected GOrganization(GitHub owner, String content) {
    super(owner, content);
  }

  public boolean hasMember(String username) throws IOException {
    return false;
  }

  public String getLogin() {
    return null;
  }

  public String getName() {
    return null;
  }

}
