package com.orientechnologies.website.github;

import com.jcabi.http.Request;
import com.jcabi.http.request.ApacheRequest;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Created by Enrico Risa on 05/11/14.
 */
public class GitHub {

  /**
   * Pattern to present day in ISO-8601.
   */
  public static final String   FORMAT_ISO = "yyyy-MM-dd'T'HH:mm:ss'Z'";
  /**
   * The time zone we're in.
   */
  public static final TimeZone TIMEZONE   = TimeZone.getTimeZone("UTC");
  protected static String      API        = "https://api.github.com";

  protected static Request     REQUEST    = new ApacheRequest(API);

  protected String             token;

  public GitHub(String token) {
    this.token = token;
  }

  public GOrganization organization(String org) throws IOException {

    String content = REQUEST.uri().path("/orgs/" + org).back().method("GET")
        .header("Authorization", String.format("token %s", token)).fetch().body();
    return new GOrganization(this, content);
  }

  public GRepo repo(String repo) throws IOException {

    String content = REQUEST.uri().path("/repos/" + repo).back().method("GET")
        .header("Authorization", String.format("token %s", token)).fetch().body();
    return new GRepo(this, content);
  }

  public GIssue openIssue(String repo, String content) throws IOException {
    REQUEST.uri().path("/repos" + repo + "/issues").back().body().set(content).back().method("POST")
        .header("Authorization", String.format("token %s", token)).fetch().body();
  }

  public GUser user() throws IOException {
    String content = REQUEST.uri().path("/user").back().method("GET").header("Authorization", String.format("token %s", token))
        .fetch().body();
    return new GUser(this, null, content);
  }

  /**
   * Make format.
   * 
   * @return Date format
   */
  protected static DateFormat format() {
    final DateFormat fmt = new SimpleDateFormat(FORMAT_ISO, Locale.ENGLISH);
    fmt.setTimeZone(TIMEZONE);
    return fmt;
  }
}
