package com.orientechnologies.website.services.reactor;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.services.reactor.event.comment.GithubCommentEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import reactor.event.Event;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * Created by Enrico Risa on 27/10/14.
 */
@Component
public class GithubIssueCommentEventHandler extends GitHubBaseHandler<Event<ODocument>> {

  @Autowired
  protected List<GithubCommentEvent> eventLit;

  @PostConstruct
  protected void init() {
    for (GithubCommentEvent e : eventLit) {
      events.put(e.handleWhat(), e);
    }
  }

  @Override
  public void accept(Event<ODocument> oDocumentEvent) {
    fireEvent(oDocumentEvent.getData());
  }

}
