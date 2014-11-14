package com.orientechnologies.website.services.reactor.event.issue;

import com.orientechnologies.orient.core.record.impl.ODocument;
import org.springframework.stereotype.Component;

/**
 * Created by Enrico Risa on 14/11/14.
 */

@Component
public class GithubUnLabelEvent implements GithubIssueEvent {

  @Override
  public void handle(String evt, ODocument payload) {

  }

  @Override
  public String handleWhat() {
    return "unlabeled";
  }
}
