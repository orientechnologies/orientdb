package com.orientechnologies.website.github;

import java.util.List;

/**
 * Created by Enrico Risa on 05/11/14.
 */
public class GRepo extends GEntity {

  protected GRepo(GitHub owner, String content) {
    super(owner, content);
  }

  public String getName() {
    return null;
  }

  public String getDescription() {
    return null;
  }

  public String getFullName() {
    return _local.field("full_name");
  }

  public List<GIssue> getIssues(GIssueState state) {
    return null;
  }

}
