package com.orientechnologies.website.services.reactor;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.services.reactor.event.issue.GithubIssueEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import reactor.event.Event;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * Created by Enrico Risa on 27/10/14.
 */
@Component
public class GithubIssueEventHandler extends GitHubBaseHandler<Event<ODocument>> {

  @Autowired
  protected List<GithubIssueEvent> eventLit;

  @Autowired
  private OrientDBFactory          factory;

  @PostConstruct
  protected void init() {
    for (GithubIssueEvent e : eventLit) {
      events.put(e.handleWhat(), e);
    }
  }

  @Override
  public void accept(Event<ODocument> oDocumentEvent) {

    fireEvent(oDocumentEvent.getData());
    factory.unsetDb();

  }

}
