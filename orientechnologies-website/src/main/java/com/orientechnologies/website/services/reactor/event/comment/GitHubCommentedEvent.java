package com.orientechnologies.website.services.reactor.event.comment;

import com.orientechnologies.orient.core.record.impl.ODocument;
import org.springframework.stereotype.Component;

/**
 * Created by Enrico Risa on 14/11/14.
 */

@Component
public class GitHubCommentedEvent implements GithubCommentEvent {
  @Override
  public void handle(String evt, ODocument payload) {

  }

  @Override
  public String handleWhat() {
    return "commented";
  }
}
