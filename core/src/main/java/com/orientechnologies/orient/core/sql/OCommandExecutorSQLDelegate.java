/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandExecutorNotFoundException;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.parser.OStatement;
import com.orientechnologies.orient.core.sql.parser.OrientSql;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;

/**
 * SQL UPDATE command.
 * 
 * @author Luca Garulli
 * 
 */
public class OCommandExecutorSQLDelegate extends OCommandExecutorSQLAbstract implements OCommandDistributedReplicateRequest {
  protected OCommandExecutorSQLAbstract delegate;

  @SuppressWarnings("unchecked")
  public OCommandExecutorSQLDelegate parse(final OCommandRequest iCommand) {
    if (iCommand instanceof OCommandRequestText) {
      final OCommandRequestText textRequest = (OCommandRequestText) iCommand;
      final String text = textRequest.getText();
      if (text == null)
        throw new IllegalArgumentException("Command text is null");

      final String textUpperCase = upperCase(text);

      if (textUpperCase.startsWith("SELECT") && false) {
        InputStream is = new ByteArrayInputStream(text.getBytes());
        OrientSql osql = new OrientSql(is);
        try {
          // TODO create a cache of parsed statements
          OStatement stm = osql.parse();
          delegate = stm.buildExecutor(iCommand);
          delegate.setContext(context);
          delegate.setLimit(iCommand.getLimit());
          delegate.setProgressListener(progressListener);
          is.close();
        } catch (Exception e) {
          throwParsingException(e.getMessage());
        }
      } else {
        delegate = (OCommandExecutorSQLAbstract) OSQLEngine.getInstance().getCommand(textUpperCase);
        if (delegate == null)
          throw new OCommandExecutorNotFoundException("Cannot find a command executor for the command request: " + iCommand);

        delegate.setContext(context);
        delegate.setLimit(iCommand.getLimit());
        delegate.parse(iCommand);
        delegate.setProgressListener(progressListener);
      }
    } else
      throw new OCommandExecutionException("Cannot find a command executor for the command request: " + iCommand);
    return this;
  }

  @Override
  public long getDistributedTimeout() {
    return delegate.getDistributedTimeout();
  }

  public Object execute(final Map<Object, Object> iArgs) {
    return delegate.execute(iArgs);
  }

  @Override
  public OCommandContext getContext() {
    return delegate.getContext();
  }

  @Override
  public String toString() {
    return delegate.toString();
  }

  public String getSyntax() {
    return delegate.getSyntax();
  }

  @Override
  public String getFetchPlan() {
    return delegate.getFetchPlan();
  }

  public boolean isIdempotent() {
    return delegate.isIdempotent();
  }

  public OCommandExecutorSQLAbstract getDelegate() {
    return delegate;
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    if (delegate instanceof OCommandDistributedReplicateRequest)
      return ((OCommandDistributedReplicateRequest) delegate).getQuorumType();
    return QUORUM_TYPE.ALL;
  }

  @Override
  public Set<String> getInvolvedClusters() {
    return delegate.getInvolvedClusters();
  }
}
