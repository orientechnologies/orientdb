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
public class GithubIssueEventHandler implements GitHubBaseHandler<Event<ODocument>> {

  public static List<String> events = new ArrayList<String>() {
                                      {

                                        add("assigned");
                                        add("unassigned");
                                        add("labeled");
                                        add("unlabeled");
                                        add("opened");
                                        add("closed");
                                        add("reopened");
                                      }
                                    };

  @Override
  public void accept(Event<ODocument> oDocumentEvent) {

    ODocument msg = oDocumentEvent.getData();
  }

  @Override
  public List<String> handleWhat() {
    return events;
  }
}
