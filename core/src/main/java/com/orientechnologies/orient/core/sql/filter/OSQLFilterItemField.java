/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.sql.filter;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.parser.OBaseParser;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.security.OPropertyEncryption;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OBinaryField;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ODocumentSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinary;
import com.orientechnologies.orient.core.sql.method.OSQLMethodRuntime;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodField;
import java.util.Set;

/**
 * Represent an object field as value in the query condition.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLFilterItemField extends OSQLFilterItemAbstract {

  protected Set<String> preLoadedFields;
  protected String[] preLoadedFieldsArray;
  protected String name;
  protected OCollate collate;
  private boolean collatePreset = false;
  private String stringValue;

  /**
   * Represents filter item as chain of fields. Provide interface to work with this chain like with
   * sequence of field names.
   */
  public class FieldChain {
    private FieldChain() {}

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

  public OSQLFilterItemField(final String iName, final OClass iClass) {
    this.name = OIOUtils.getStringContent(iName);
    collate = getCollateForField(iClass, name);
    if (iClass != null) {
      collatePreset = true;
    }
  }

  public OSQLFilterItemField(
      final OBaseParser iQueryToParse, final String iName, final OClass iClass) {
    super(iQueryToParse, iName);
    collate = getCollateForField(iClass, iName);
    if (iClass != null) {
      collatePreset = true;
    }
  }

  public Object getValue(
      final OIdentifiable iRecord, final Object iCurrentResult, final OCommandContext iContext) {
    if (iRecord == null)
      throw new OCommandExecutionException(
          "expression item '" + name + "' cannot be resolved because current record is NULL");

    if (preLoadedFields != null && preLoadedFields.size() == 1) {
      if ("@rid".equalsIgnoreCase(preLoadedFields.iterator().next())) return iRecord.getIdentity();
    }

    final ODocument doc = (ODocument) iRecord.getRecord();

    if (preLoadedFieldsArray == null
        && preLoadedFields != null
        && preLoadedFields.size() > 0
        && preLoadedFields.size() < 5) {
      // TRANSFORM THE SET IN ARRAY ONLY THE FIRST TIME AND IF FIELDS ARE MORE THAN ONE, OTHERWISE
      // GO WITH THE DEFAULT BEHAVIOR
      preLoadedFieldsArray = new String[preLoadedFields.size()];
      preLoadedFields.toArray(preLoadedFieldsArray);
    }

    // UNMARSHALL THE SINGLE FIELD
    if (preLoadedFieldsArray != null && !doc.deserializeFields(preLoadedFieldsArray)) return null;

    final Object v = stringValue == null ? doc.rawField(name) : stringValue;

    if (!collatePreset && doc != null) {
      OClass schemaClass = ODocumentInternal.getImmutableSchemaClass(doc);
      if (schemaClass != null) {
        collate = getCollateForField(schemaClass, name);
      }
    }

    return transformValue(iRecord, iContext, v);
  }

  public OBinaryField getBinaryField(final OIdentifiable iRecord) {
    if (iRecord == null)
      throw new OCommandExecutionException(
          "expression item '" + name + "' cannot be resolved because current record is NULL");

    if (operationsChain != null && operationsChain.size() > 0)
      // CANNOT USE BINARY FIELDS
      return null;

    final ODocument rec = iRecord.getRecord();
    OPropertyEncryption encryption = ODocumentInternal.getPropertyEncryption(rec);
    BytesContainer serialized = new BytesContainer(rec.toStream());
    byte version = serialized.bytes[serialized.offset++];
    ODocumentSerializer serializer = ORecordSerializerBinary.INSTANCE.getSerializer(version);
    ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().get();

    // check for embedded objects, they have invalid ID and they are serialized with class name
    return serializer.deserializeField(
        serialized,
        rec instanceof ODocument
            ? ODocumentInternal.getImmutableSchemaClass(((ODocument) rec))
            : null,
        name,
        rec.isEmbedded(),
        db.getMetadata().getImmutableSchemaSnapshot(),
        encryption);
  }

  public String getRoot() {
    return name;
  }

  public void setRoot(final OBaseParser iQueryToParse, final String iRoot) {
    if (isStringLiteral(iRoot)) {
      this.stringValue = OIOUtils.getStringContent(iRoot);
    }
    // TODO support all the basic types
    this.name = OIOUtils.getStringContent(iRoot);
  }

  private boolean isStringLiteral(String iRoot) {
    if (iRoot.startsWith("'") && iRoot.endsWith("'")) {
      return true;
    }
    if (iRoot.startsWith("\"") && iRoot.endsWith("\"")) {
      return true;
    }
    return false;
  }

  /**
   * Check whether or not this filter item is chain of fields (e.g. "field1.field2.field3"). Return
   * true if filter item contains only field projections operators, if field item contains any other
   * projection operator the method returns false. When filter item does not contains any chain
   * operator, it is also field chain consist of one field.
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
   * @throws IllegalStateException if this filter item cannot be represented as {@code FieldChain}.
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

  /**
   * get the collate of this expression, based on the fully evaluated field chain starting from the
   * passed object.
   *
   * @param doc the root element (document?) of this field chain
   * @return the collate, null if no collate is defined
   */
  public OCollate getCollate(Object doc) {
    if (collate != null || operationsChain == null || !isFieldChain()) {
      return collate;
    }
    if (!(doc instanceof OIdentifiable)) {
      return null;
    }
    FieldChain chain = getFieldChain();
    ODocument lastDoc = ((OIdentifiable) doc).getRecord();
    for (int i = 0; i < chain.getItemCount() - 1; i++) {
      if (lastDoc == null) {
        return null;
      }
      Object nextDoc = lastDoc.field(chain.getItemName(i));
      if (nextDoc == null || !(nextDoc instanceof OIdentifiable)) {
        return null;
      }
      lastDoc = ((OIdentifiable) nextDoc).getRecord();
    }
    if (lastDoc == null) {
      return null;
    }
    OClass schemaClass = ODocumentInternal.getImmutableSchemaClass(lastDoc);
    if (schemaClass == null) {
      return null;
    }
    OProperty property = schemaClass.getProperty(chain.getItemName(chain.getItemCount() - 1));
    if (property == null) {
      return null;
    }
    return property.getCollate();
  }
}
