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

import com.orientechnologies.common.exception.OException;

public class OCommandScriptException extends OException {

  private String            text;
  private int               position;
  private static final long serialVersionUID = -7430575036316163711L;

  public OCommandScriptException(String iMessage) {
    super(iMessage, null);
  }

  public OCommandScriptException(String iMessage, Throwable cause) {
    super(iMessage, cause);
  }

  public OCommandScriptException(String iMessage, String iText, int iPosition, Throwable cause) {
    super(iMessage, cause);
    text = iText;
    position = iPosition < 0 ? 0 : iPosition;
  }

  public OCommandScriptException(String iMessage, String iText, int iPosition) {
    super(iMessage);
    text = iText;
    position = iPosition < 0 ? 0 : iPosition;
  }

  @Override
  public String getMessage() {
    if (text == null)
      return super.getMessage();

    final StringBuilder buffer = new StringBuilder();
    buffer.append("Error on parsing script at position #");
    buffer.append(position);
    buffer.append(": " + super.getMessage());
    buffer.append("\nScript: ");
    buffer.append(text);
    buffer.append("\n------");
    for (int i = 0; i < position - 1; ++i)
      buffer.append("-");

    buffer.append("^");
    return buffer.toString();
  }
}
