/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.processor.block;

import java.util.List;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.processor.OComposableProcessor;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

public class OQueryBlock extends OAbstractBlock {
  @Override
  public Object processBlock(final OComposableProcessor iManager, final ODocument iConfig, final OCommandContext iContext,
      final boolean iReadOnly) {
    if (!(iConfig instanceof ODocument))
      throw new OTransactionException("QueryBlock: expected document as content");

    String command = parse((ODocument) iConfig, iContext);

    command = (String) resolveInContext(command, iContext);

    debug(iContext, "Executing: " + (iReadOnly ? "query" : "command") + ": " + command + "...");

    final OCommandRequestText cmd = new OSQLSynchQuery<OIdentifiable>(command.toString());

    // CREATE THE RIGHT COMMAND BASED ON IDEMPOTENCY
    // final OCommandRequestText cmd = iReadOnly ? new OSQLSynchQuery<OIdentifiable>(command.toString()) : new OCommandSQL(
    // command.toString());

    return cmd;
  }

  @Override
  public String getName() {
    return "query";
  }

  protected String parse(final ODocument iContent, final OCommandContext iContext) {
    final Object code = getField(iContent, "code");
    if (code != null)
      // CODE MODE
      return code.toString();

    // SINGLE FIELDS MODE
    final StringBuilder command = new StringBuilder();
    command.append("select ");

    generateProjections(iContent, command);
    generateTarget(iContent, command);
    generateLimit(iContent, command);
    generateFilter(iContent, command);
    generateGroupBy(iContent, command);
    generateOrderBy(iContent, command);
    generateLimit(iContent, command);

    return command.toString();
  }

  @SuppressWarnings("unchecked")
  private void generateProjections(ODocument iInput, final StringBuilder iCommandText) {
    final Object fields = getField(iInput, "fields");

    if (fields instanceof String)
      iCommandText.append(fields.toString());
    else {
      final List<ODocument> fieldList = (List<ODocument>) fields;
      if (fieldList != null) {
        boolean first = true;
        for (Object field : fieldList) {
          if (first)
            first = false;
          else
            iCommandText.append(", ");

          if (field instanceof ODocument) {
            final ODocument fieldDoc = (ODocument) field;
            for (String f : fieldDoc.fieldNames()) {
              iCommandText.append(f);
              iCommandText.append(" as ");
              iCommandText.append(fieldDoc.field(f));
            }
          } else
            iCommandText.append(field.toString());
        }
      }
    }
  }

  private void generateTarget(ODocument iInput, final StringBuilder iCommandText) {
    final String target = getField(iInput, "target");
    if (target != null) {
      iCommandText.append(" from ");
      iCommandText.append(target);
    }
  }

  private void generateFilter(ODocument iInput, final StringBuilder iCommandText) {
    final String filter = getField(iInput, "filter");
    if (filter != null) {
      iCommandText.append(" where ");
      iCommandText.append(filter);
    }
  }

  private void generateGroupBy(ODocument iInput, final StringBuilder iCommandText) {
    final String groupBy = getField(iInput, "groupBy");
    if (groupBy != null) {
      iCommandText.append(" group by ");
      iCommandText.append(groupBy);
    }
  }

  private void generateOrderBy(ODocument iInput, final StringBuilder iCommandText) {
    final String orderBy = getField(iInput, "orderBy");
    if (orderBy != null) {
      iCommandText.append(" order by ");
      iCommandText.append(orderBy);
    }
  }

  private void generateLimit(ODocument iInput, final StringBuilder iCommandText) {
    final Integer limit = getField(iInput, "limit");
    if (limit != null) {
      iCommandText.append(" limit ");
      iCommandText.append(limit);
    }
  }
}