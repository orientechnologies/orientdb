package com.orientechnologies.website.github;

import com.jcabi.http.Request;
import com.jcabi.http.request.ApacheRequest;

import java.io.IOException;

/**
 * Created by Enrico Risa on 05/11/14.
 */
public class GitHub {

  private static String  API     = "https://api.github.com";

  private static Request REQUEST = new ApacheRequest(API);

  private String         token;

  public GitHub(String token) {
    this.token = token;
  }

  public GOrganization organization(String org) throws IOException {

    String content = REQUEST.uri().path("/orgs/" + org).back().method("GET").header("token", token).fetch().body();
    return new GOrganization(this, content);
  }

  public GRepo repo(String repo) throws IOException {

    String content = REQUEST.uri().path("/repos/" + repo).back().method("GET").header("token", token).fetch().body();
    return new GRepo(this, content);
  }

  public GUser user() {
    return null;
  }
}
