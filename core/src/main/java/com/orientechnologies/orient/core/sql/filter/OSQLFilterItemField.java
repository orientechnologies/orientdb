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
package com.orientechnologies.orient.core.sql.filter;

import com.orientechnologies.common.parser.OBaseParser;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodField;
import com.orientechnologies.orient.core.sql.methods.OSQLMethodRuntime;

import java.util.Set;

/**
 * Represent an object field as value in the query condition.
 * 
 * @author Luca Garulli
 * 
 */
public class OSQLFilterItemField extends OSQLFilterItemAbstract {
  protected Set<String> preLoadedFields;
  protected String[]    preLoadedFieldsArray;
  protected String      name;
  protected OCollate    collate;

  /**
   * Represents filter item as chain of fields. Provide interface to work with this chain like with sequence of field names.
   */
  public class FieldChain {
    private FieldChain() {
    }

    public String getItemName(int fieldIndex) {
      if (fieldIndex == 0) {
        return name;
      } else {
        return operationsChain.get(fieldIndex - 1).getValue()[0].toString();
      }
    }

    public int getItemCount() {
      if (operationsChain == null) {
        return 1;
      } else {
        return operationsChain.size() + 1;
      }
    }

    /**
     * Field chain is considered as long chain if it contains more than one item.
     * 
     * @return true if this chain is long and false in another case.
     */
    public boolean isLong() {
      return operationsChain != null && operationsChain.size() > 0;
    }

    public boolean belongsTo(OSQLFilterItemField filterItemField) {
      return OSQLFilterItemField.this == filterItemField;
    }
  }

  public OSQLFilterItemField(final String iName) {
    this.name = OStringSerializerHelper.getStringContent(iName);
  }

  public OSQLFilterItemField(final OBaseParser iQueryToParse, final String iName) {
    super(iQueryToParse, iName);
  }

  public Object getValue(final OIdentifiable iRecord, final Object iCurrentResult, final OCommandContext iContext) {
    if (iRecord == null)
      throw new OCommandExecutionException("expression item '" + name + "' cannot be resolved because current record is NULL");

    final ODocument doc = (ODocument) iRecord.getRecord();

    if (preLoadedFieldsArray == null && preLoadedFields != null && preLoadedFields.size() > 0 && preLoadedFields.size() < 5) {
      // TRANSFORM THE SET IN ARRAY ONLY THE FIRST TIME AND IF FIELDS ARE MORE THAN ONE, OTHERWISE GO WITH THE DEFAULT BEHAVIOR
      preLoadedFieldsArray = new String[preLoadedFields.size()];
      preLoadedFields.toArray(preLoadedFieldsArray);
    }

    // UNMARSHALL THE SINGLE FIELD
    if (doc.deserializeFields(preLoadedFieldsArray)) {
      final Object v = doc.rawField(name);

      collate = getCollateForField(doc, name);

      return transformValue(iRecord, iContext, v);
    }
    return null;
  }

  public String getRoot() {
    return name;
  }

  public void setRoot(final OBaseParser iQueryToParse, final String iRoot) {
    this.name = OStringSerializerHelper.getStringContent(iRoot);
  }

  /**
   * Check whether or not this filter item is chain of fields (e.g. "field1.field2.field3"). Return true if filter item contains
   * only field projections operators, if field item contains any other projection operator the method returns false. When filter
   * item does not contains any chain operator, it is also field chain consist of one field.
   * 
   * @return whether or not this filter item can be represented as chain of fields.
   */
  public boolean isFieldChain() {
    if (operationsChain == null) {
      return true;
    }

    for (OPair<OSQLMethodRuntime, Object[]> pair : operationsChain) {
      if (!pair.getKey().getMethod().getName().equals(OSQLMethodField.NAME)) {
        return false;
      }
    }

    return true;
  }

  /**
   * Creates {@code FieldChain} in case when filter item can have such representation.
   * 
   * @return {@code FieldChain} representation of this filter item.
   * @throws IllegalStateException
   *           if this filter item cannot be represented as {@code FieldChain}.
   */
  public FieldChain getFieldChain() {
    if (!isFieldChain()) {
      throw new IllegalStateException("Filter item field contains not only field operators");
    }

    return new FieldChain();
  }

  public void setPreLoadedFields(final Set<String> iPrefetchedFieldList) {
    this.preLoadedFields = iPrefetchedFieldList;
  }

  public OCollate getCollate() {
    return collate;
  }
}
