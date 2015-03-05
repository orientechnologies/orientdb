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
package com.orientechnologies.orient.core.processor.block;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.processor.OComposableProcessor;
import com.orientechnologies.orient.core.processor.OProcessException;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OTableBlock extends OAbstractBlock {
  public static final String NAME = "table";

  protected Object           header;
  protected Object           body;
  protected Object           footer;

  @Override
  public Object processBlock(OComposableProcessor iManager, final OCommandContext iContext, final ODocument iConfig,
      ODocument iOutput, final boolean iReadOnly) {
    if (!(iConfig instanceof ODocument))
      throw new OProcessException("Content in not a JSON");

    final ODocument table = new ODocument();

    // HEADER
    header = getRequiredField(iContext, iConfig, "header");
    if (isBlock(header))
      header = delegate("header", iManager, header, iContext, iOutput, iReadOnly);
    table.field("header", header);

    // BODY
    body = getRequiredField(iContext, iConfig, "body");
    if (isBlock(body))
      body = delegate("body", iManager, body, iContext, iOutput, iReadOnly);
    table.field("body", body);

    // FOOTER
    footer = getRequiredField(iContext, iConfig, "footer");
    if (isBlock(footer))
      footer = delegate("footer", iManager, footer, iContext, iOutput, iReadOnly);
    table.field("footer", footer);

    return table;
  }

  @Override
  public String getName() {
    return NAME;
  }

  public Object getHeader() {
    return header;
  }

  public void setHeader(Object header) {
    this.header = header;
  }

  public Object getBody() {
    return body;
  }

  public void setBody(Object body) {
    this.body = body;
  }

  public Object getFooter() {
    return footer;
  }

  public void setFooter(Object footer) {
    this.footer = footer;
  }
}
