package com.orientechnologies.website.services.reactor.event;

import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Created by Enrico Risa on 14/11/14.
 */
public interface GithubEvent {

  public void handle(String evt, ODocument payload);

  public String handleWhat();
}
