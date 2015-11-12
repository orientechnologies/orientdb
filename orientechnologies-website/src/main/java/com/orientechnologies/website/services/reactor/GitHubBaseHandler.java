package com.orientechnologies.website.services.reactor;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.events.EventManager;
import com.orientechnologies.website.services.reactor.event.GithubEvent;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Enrico Risa on 27/10/14.
 */

public abstract class GitHubBaseHandler<T> implements GitHubHandler<T> {

  @Autowired
  OrientDBFactory                    factory;
  protected Map<String, GithubEvent> events          = new HashMap<String, GithubEvent>();

  @Autowired
  protected EventManager             eventManager;
  ExecutorService                    executorService = Executors.newFixedThreadPool(10);

  public Set<String> handleWhat() {
    return events.keySet();
  }

  public void fireEvent(final ODocument payload) {

    try {
      executorService.execute(new Runnable() {
        @Override
        public void run() {
          OrientGraph graph = factory.getGraph();
          String action = payload.field("action");
          GithubEvent evt = events.get(action);

          try {
            if (evt != null) {
              for (int r = 0; r < 10; ++r) {
                try {
                  evt.handle(action, payload);
                  break;
                } catch (ONeedRetryException retry) {
                  OLogManager.instance().error(this, " Retried %s event with payload : %s", action, evt.formantPayload(payload));
                } catch (Exception e) {
                  OLogManager.instance().error(this, "Error handling %s event with payload : (%s)", e, action,
                      evt.formantPayload(payload));

                  logException(graph, e, action, payload);
                  break;
                }
              }
            }
          } finally {
            if (graph != null)
              graph.shutdown();
            factory.unsetDb();
            eventManager.fireEvents();
          }
        }
      });
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  private void logException(OrientGraph graph, Exception e, String action, ODocument payload) {
    ODatabaseDocumentTx rawGraph = graph.getRawGraph();

    ODocument doc = new ODocument("GitHubErrorLog");

    doc.field("action", action);
    doc.field("payload", payload.toJSON());
    doc.field("timestamp", new Date());
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    e.printStackTrace(pw);
    doc.field("stackTrace", sw.toString());
    rawGraph.save(doc);

  }

}
