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

import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.processor.OConfigurableProcessor;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

public class OQueryBlock extends OAbstractBlock {
  @Override
  public Object process(OConfigurableProcessor iManager, final Object iContent, final ODocument iContext, final boolean iReadOnly) {
    if (!(iContent instanceof ODocument))
      throw new OTransactionException("QueryBlock: expected document as content");

    final String command = parse((ODocument) iContent, iContext);

    // CREATE THE RIGHT COMMAND BASED ON IDEMPOTENCY
    final OCommandRequestText cmd = iReadOnly ? new OSQLSynchQuery<OIdentifiable>(command.toString()) : new OCommandSQL(
        command.toString());

    return cmd.execute();
  }

  protected String parse(final ODocument iContent, final ODocument iContext) {
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

  private void generateProjections(ODocument iInput, final StringBuilder iCommandText) {
    final List<ODocument> fields = iInput.field("fields");
    if (fields != null) {
      boolean first = true;
      for (Object field : fields) {
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

  private void generateTarget(ODocument iInput, final StringBuilder iCommandText) {
    final String target = iInput.field("target");
    if (target != null) {
      iCommandText.append(" from ");
      iCommandText.append(target);
    }
  }

  private void generateFilter(ODocument iInput, final StringBuilder iCommandText) {
    final String filter = iInput.field("filter");
    if (filter != null) {
      iCommandText.append(" where ");
      iCommandText.append(filter);
    }
  }

  private void generateGroupBy(ODocument iInput, final StringBuilder iCommandText) {
    final String groupBy = iInput.field("groupBy");
    if (groupBy != null) {
      iCommandText.append(" group by ");
      iCommandText.append(groupBy);
    }
  }

  private void generateOrderBy(ODocument iInput, final StringBuilder iCommandText) {
    final String orderBy = iInput.field("orderBy");
    if (orderBy != null) {
      iCommandText.append(" order by ");
      iCommandText.append(orderBy);
    }
  }

  private void generateLimit(ODocument iInput, final StringBuilder iCommandText) {
    final Integer limit = iInput.field("limit");
    if (limit != null) {
      iCommandText.append(" limit ");
      iCommandText.append(limit);
    }
  }
}