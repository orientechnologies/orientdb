package com.orientechnologies.website.services.reactor;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import reactor.event.Event;

import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Created by Enrico Risa on 27/10/14.
 */
@Component
public class GithubIssueCommentEventHandler implements GitHubBaseHandler<Event<ODocument>> {

  public static List<String> events = new ArrayList<String>() {
                                      {
                                        add("created");
                                      }
                                    };

  @Override
  public void accept(Event<ODocument> oDocumentEvent) {

    ODocument msg = oDocumentEvent.getData();

    ODocument comment = msg.field("comment");
  }

  @Override
  public List<String> handleWhat() {
    return events;
  }
}
