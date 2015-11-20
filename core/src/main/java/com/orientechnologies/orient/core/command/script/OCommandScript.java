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
package com.orientechnologies.orient.core.command.script;

import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequestTextAbstract;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.serialization.OMemoryStream;
import com.orientechnologies.orient.core.serialization.OSerializableStream;

import javax.script.CompiledScript;

/**
 * Script command request implementation. It just stores the request and delegated the execution to the configured OCommandExecutor.
 * 
 * 
 * @see OCommandExecutorScript
 * @author Luca Garulli
 * 
 */
@SuppressWarnings("serial")
public class OCommandScript extends OCommandRequestTextAbstract {
  private String                                                         language;
  private CompiledScript                                                 compiledScript;

  private OCommandDistributedReplicateRequest.DISTRIBUTED_EXECUTION_MODE executionMode = OCommandDistributedReplicateRequest.DISTRIBUTED_EXECUTION_MODE.LOCAL;

  public OCommandScript() {
    useCache = true;
  }

  public OCommandScript(final String iLanguage, final String iText) {
    super(iText);
    setLanguage(iLanguage);
    useCache = true;
  }

  public OCommandScript(final String iText) {
    this("sql", iText);
  }

  public boolean isIdempotent() {
    return false;
  }

  public String getLanguage() {
    return language;
  }

  public OCommandScript setLanguage(String language) {
    if (language == null || language.isEmpty()) {
      throw new IllegalArgumentException("Not a valid script language specified: " + language);
    }
    this.language = language;
    return this;
  }

  public OSerializableStream fromStream(byte[] iStream) throws OSerializationException {
    final OMemoryStream buffer = new OMemoryStream(iStream);
    language = buffer.getAsString();

    // FIX TO HANDLE USAGE OF EXECUTION MODE STARTING FROM v2.1.3
    final int currPosition = buffer.getPosition();
    final String value = buffer.getAsString();
    try {
      executionMode = OCommandDistributedReplicateRequest.DISTRIBUTED_EXECUTION_MODE.valueOf(value);
    } catch (IllegalArgumentException e) {
      // OLD VERSION: RESET TO THE OLD POSITION
      buffer.setPosition(currPosition);
    }

    fromStream(buffer);
    return this;
  }

  public byte[] toStream() throws OSerializationException {
    final OMemoryStream buffer = new OMemoryStream();
    buffer.setUtf8(language);
    buffer.setUtf8(executionMode.name());
    return toStream(buffer);
  }

  public CompiledScript getCompiledScript() {
    return compiledScript;
  }

  public void setCompiledScript(CompiledScript script) {
    compiledScript = script;
  }

  @Override
  public String toString() {
    if (language != null)
      return language + "." + text;
    return "script." + text;
  }

  public OCommandDistributedReplicateRequest.DISTRIBUTED_EXECUTION_MODE getExecutionMode() {
    return executionMode;
  }

  public OCommandScript setExecutionMode(OCommandDistributedReplicateRequest.DISTRIBUTED_EXECUTION_MODE executionMode) {
    this.executionMode = executionMode;
    return this;
  }
}
