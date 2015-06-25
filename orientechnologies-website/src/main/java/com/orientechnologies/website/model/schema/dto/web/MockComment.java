package com.orientechnologies.website.model.schema.dto.web;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.orientechnologies.website.github.GitHub;
import com.orientechnologies.website.model.schema.dto.Comment;

/**
 * Created by Enrico Risa on 25/06/15.
 */
public class MockComment extends Comment {

  @JsonIgnore
  @Override
  public String getId() {
    return super.getId();
  }

  @JsonProperty("created_at")
  public String getCreated() {
    return GitHub.format().format(getCreatedAt());
  }

  @JsonProperty("updated_at")
  public String getUpdated() {

    return GitHub.format().format(getUpdatedAt());

  }

  @JsonProperty("id")
  @Override
  public Integer getCommentId() {
    return super.getCommentId();
  }
}
