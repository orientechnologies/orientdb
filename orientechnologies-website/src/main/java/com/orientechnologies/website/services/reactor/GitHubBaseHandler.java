package com.orientechnologies.website.services.reactor;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.services.reactor.event.GithubEvent;

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

  public void fireEvent(ODocument payload) {

    String action = payload.field("action");
    GithubEvent evt = events.get(action);
    if (evt != null) {
      for (int r = 0; r < 10; ++r) {
        try {
          evt.handle(action, payload);
          break;
        } catch (ONeedRetryException retry) {
          OLogManager.instance().error(this, " Retried %s event ", action);
        } catch (Exception e) {
          OLogManager.instance().error(this, "Error handling %s event with payload %s", action, payload.toJSON());
        }
      }
    }
  }
}
