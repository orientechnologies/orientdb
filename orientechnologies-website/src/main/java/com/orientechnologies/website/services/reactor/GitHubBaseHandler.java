package com.orientechnologies.website.services.reactor;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.services.reactor.event.GithubEvent;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by Enrico Risa on 27/10/14.
 */

public abstract class GitHubBaseHandler<T> implements GitHubHandler<T> {

  protected Map<String, GithubEvent> events = new HashMap<String, GithubEvent>();

  public Set<String> handleWhat() {
    return events.keySet();
  }

  @Transactional
  public void fireEvent(ODocument payload) {

    String action = payload.field("action");
    GithubEvent evt = events.get(action);
    if (evt != null) {
      evt.handle(action, payload);
    }
  }
}
