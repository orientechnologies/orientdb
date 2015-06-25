package com.orientechnologies.website.services.reactor;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.annotation.RetryingTransaction;
import com.orientechnologies.website.services.reactor.event.GithubEvent;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by Enrico Risa on 27/10/14.
 */

public abstract class GitHubBaseHandler<T> implements GitHubHandler<T> {

  @Autowired
  OrientDBFactory                    factory;
  protected Map<String, GithubEvent> events = new HashMap<String, GithubEvent>();

  public Set<String> handleWhat() {
    return events.keySet();
  }

  public void fireEvent(final ODocument payload) {

    new Thread(new Runnable() {
      @Override
      public void run() {
        OrientGraph graph = factory.getGraph();
        String action = payload.field("action");
        GithubEvent evt = events.get(action);

        try {
          if (evt != null) {
            for (int r = 0; r < 10; ++r) {
              try {
                test();
                evt.handle(action, payload);
                break;
              } catch (ONeedRetryException retry) {
                OLogManager.instance().error(this, " Retried %s event with payload : %s", action, evt.formantPayload(payload));
              } catch (Exception e) {
                OLogManager.instance()
                    .error(this, "Error handling %s event with payload : %s", action, evt.formantPayload(payload));
                e.printStackTrace();
              }
            }
          }
        } finally {
          if (graph != null)
            graph.shutdown();
        }
      }
    }).start();

  }

  @RetryingTransaction(exception = ONeedRetryException.class, retries = 5)
  protected void test() {

  }
}
