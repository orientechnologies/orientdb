package com.orientechnologies.website.services.reactor.event.issue;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.repository.RepositoryRepository;
import org.springframework.stereotype.Component;

/**
 * Created by Enrico Risa on 14/11/14.
 */

@Component
public class GithubLabelEvent implements GithubIssueEvent {

  private RepositoryRepository repositoryRepository;

  @Override
  public void handle(String evt, ODocument payload) {

  }

  @Override
  public String handleWhat() {
    return "labeled";
  }
}
