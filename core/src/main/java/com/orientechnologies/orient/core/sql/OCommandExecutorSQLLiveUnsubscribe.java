/*
 *
 *  *  Copyright 2015 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.query.live.OLiveQueryHook;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Map;

/**
 * @author Luigi Dell'Aquila
 */
public class OCommandExecutorSQLLiveUnsubscribe extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest {
  public static final String KEYWORD_LIVE_UNSUBSCRIBE = "LIVE UNSUBSCRIBE";

  protected String           unsubscribeToken;

  public OCommandExecutorSQLLiveUnsubscribe() {
  }

  private Object executeUnsubscribe() {
    try {

      OLiveQueryHook.unsubscribe(Integer.parseInt(unsubscribeToken));
      ODocument result = new ODocument();
      result.field("unsubscribed", unsubscribeToken);

      return result;
    } catch (Exception e) {
      OLogManager.instance().warn(this,
          "error unsubscribing token " + unsubscribeToken + ": " + e.getClass().getName() + " - " + e.getMessage());
      ODocument result = new ODocument();
      result.field("error-unsubscribe", unsubscribeToken);
      result.field("error-description", e.getMessage());
      result.field("error-type", e.getClass().getName());

      return result;
    }
  }

  @Override
  public long getDistributedTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_COMMAND_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }

  public Object execute(final Map<Object, Object> iArgs) {
    if (this.unsubscribeToken != null) {
      return executeUnsubscribe();
    }
    ODocument result = new ODocument();
    result.field("error-unsubscribe", "no token");
    return result;
  }

  @Override
  public OCommandExecutorSQLLiveUnsubscribe parse(OCommandRequest iRequest) {
    OCommandRequestText requestText = (OCommandRequestText) iRequest;
    String originalText = requestText.getText();
    String remainingText = requestText.getText().trim().substring(5).trim();
    requestText.setText(remainingText);
    try {
      if (remainingText.toLowerCase().startsWith("unsubscribe")) {
        remainingText = remainingText.substring("unsubscribe".length()).trim();
        if (remainingText.contains(" ")) {
          throw new OQueryParsingException("invalid unsubscribe token for live query: " + remainingText);
        }
        this.unsubscribeToken = remainingText;
      }
    } finally {
      requestText.setText(originalText);
    }
    return this;
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.NONE;
  }
}
