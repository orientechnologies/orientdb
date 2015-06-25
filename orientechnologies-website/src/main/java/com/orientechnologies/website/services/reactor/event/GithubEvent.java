package com.orientechnologies.website.services.reactor.event;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.website.annotation.RetryingTransaction;
import org.springframework.transaction.annotation.Transactional;

import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Created by Enrico Risa on 14/11/14.
 */
public interface GithubEvent {

  @Transactional
  @RetryingTransaction(exception = ONeedRetryException.class, retries = 5)
  public void handle(String evt, ODocument payload);

  public String handleWhat();


  public String formantPayload(ODocument payload);
}
