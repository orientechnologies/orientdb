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
package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OAutoConvertToRecord;
import com.orientechnologies.orient.core.db.record.ODetachable;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeListener;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeTimeLine;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazyMap;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedMap;
import com.orientechnologies.orient.core.db.record.OTrackedMultiValue;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.delta.ODeltaDocumentFieldType;
import com.orientechnologies.orient.core.delta.ODocumentDelta;
import com.orientechnologies.orient.core.delta.UpdateTypeValueType;
import com.orientechnologies.orient.core.delta.ValueType;
import com.orientechnologies.orient.core.exception.*;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.iterator.OEmptyMapEntryIterator;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OGlobalProperty;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.metadata.schema.OImmutableProperty;
import com.orientechnologies.orient.core.metadata.schema.OImmutableSchema;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.schema.OTypeInterface;
import com.orientechnologies.orient.core.metadata.security.*;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.serialization.ODocumentSerializable;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetwork;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.filter.OSQLPredicate;
import com.orientechnologies.orient.core.storage.OBasicTransaction;

import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.UnsupportedEncodingException;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Document representation to handle values dynamically. Can be used in schema-less, schema-mixed and schema-full modes. Fields can
 * be added at run-time. Instances can be reused across calls by using the reset() before to re-use.
 */
@SuppressWarnings({ "unchecked" })
public class ODocument extends ORecordAbstract
    implements Iterable<Entry<String, Object>>, ORecordSchemaAware, ODetachable, Externalizable, OElement {

  public static final    byte     RECORD_TYPE      = 'd';
  protected static final String[] EMPTY_STRINGS    = new String[] {};
  private static final   long     serialVersionUID = 1L;

  protected int fieldSize;

  protected Map<String, ODocumentEntry> fields;

  protected           boolean                             trackingChanges        = true;
  protected           boolean                             ordered                = true;
  protected           boolean                             lazyLoad               = true;
  protected           boolean                             allowChainedAccess     = true;
  protected transient List<WeakReference<ORecordElement>> owners                 = null;
  protected           OImmutableSchema                    schema;
  private             String                              className;
  private             OImmutableClass                     immutableClazz;
  private             int                                 immutableSchemaVersion = 1;
  protected           OPropertyAccess                     propertyAccess;
  protected           OPropertyEncryption                 propertyEncryption;

  /**
   * Internal constructor used on unmarshalling.
   */
  public ODocument() {
    setup(ODatabaseRecordThreadLocal.instance().getIfDefined());
  }

  /**
   * Internal constructor used on unmarshalling.
   */
  public ODocument(ODatabaseSession database) {
    setup((ODatabaseDocumentInternal) database);
  }

  /**
   * Creates a new instance by the raw stream usually read from the database. New instances are not persistent until {@link #save()}
   * is called.
   *
   * @param iSource Raw stream
   */
  @Deprecated
  public ODocument(final byte[] iSource) {
    source = iSource;
    setup(ODatabaseRecordThreadLocal.instance().getIfDefined());
  }

  /**
   * Creates a new instance by the raw stream usually read from the database. New instances are not persistent until {@link #save()}
   * is called.
   *
   * @param iSource Raw stream as InputStream
   */
  public ODocument(final InputStream iSource) throws IOException {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    OIOUtils.copyStream(iSource, out, -1);
    source = out.toByteArray();
    setup(ODatabaseRecordThreadLocal.instance().getIfDefined());
  }

  /**
   * Creates a new instance in memory linked by the Record Id to the persistent one. New instances are not persistent until {@link
   * #save()} is called.
   *
   * @param iRID Record Id
   */
  public ODocument(final ORID iRID) {
    setup(ODatabaseRecordThreadLocal.instance().getIfDefined());
    recordId = (ORecordId) iRID;
    status = STATUS.NOT_LOADED;
    dirty = false;
    contentChanged = false;
  }

  /**
   * Creates a new instance in memory of the specified class, linked by the Record Id to the persistent one. New instances are not
   * persistent until {@link #save()} is called.
   *
   * @param iClassName Class name
   * @param iRID       Record Id
   */
  public ODocument(final String iClassName, final ORID iRID) {
    this(iClassName);
    recordId = (ORecordId) iRID;

    final ODatabaseDocumentInternal database = getDatabaseInternal();
    if (recordId.getClusterId() > -1) {
      final OSchema schema = database.getMetadata().getImmutableSchemaSnapshot();
      final OClass cls = schema.getClassByClusterId(recordId.getClusterId());
      if (cls != null && !cls.getName().equals(iClassName))
        throw new IllegalArgumentException(
            "Cluster id does not correspond class name should be " + iClassName + " but found " + cls.getName());
    }

    dirty = false;
    contentChanged = false;
    status = STATUS.NOT_LOADED;
  }

  /**
   * Creates a new instance in memory of the specified class. New instances are not persistent until {@link #save()} is called.
   *
   * @param iClassName Class name
   */
  public ODocument(final String iClassName) {
    setup(ODatabaseRecordThreadLocal.instance().getIfDefined());
    setClassName(iClassName);
  }

  /**
   * Creates a new instance in memory of the specified class. New instances are not persistent until {@link #save()} is called.
   *
   * @param session    the session the instance will be attached to
   * @param iClassName Class name
   */
  public ODocument(ODatabaseSession session, final String iClassName) {
    setup((ODatabaseDocumentInternal) session);
    setClassName(iClassName);
  }

  /**
   * Creates a new instance in memory of the specified schema class. New instances are not persistent until {@link #save()} is
   * called. The database reference is taken from the thread local.
   *
   * @param iClass OClass instance
   */
  public ODocument(final OClass iClass) {
    this(iClass != null ? iClass.getName() : null);
  }

  /**
   * Fills a document passing the field array in form of pairs of field name and value.
   *
   * @param iFields Array of field pairs
   */
  public ODocument(final Object[] iFields) {
    setup(ODatabaseRecordThreadLocal.instance().getIfDefined());
    if (iFields != null && iFields.length > 0)
      for (int i = 0; i < iFields.length; i += 2) {
        field(iFields[i].toString(), iFields[i + 1]);
      }
  }

  /**
   * Fills a document passing a map of key/values where the key is the field name and the value the field's value.
   *
   * @param iFieldMap Map of Object/Object
   */
  public ODocument(final Map<?, Object> iFieldMap) {
    setup(ODatabaseRecordThreadLocal.instance().getIfDefined());
    if (iFieldMap != null && !iFieldMap.isEmpty())
      for (Entry<?, Object> entry : iFieldMap.entrySet()) {
        field(entry.getKey().toString(), entry.getValue());
      }
  }

  /**
   * Fills a document passing the field names/values pair, where the first pair is mandatory.
   */
  public ODocument(final String iFieldName, final Object iFieldValue, final Object... iFields) {
    this(iFields);
    field(iFieldName, iFieldValue);
  }

  @Override
  public Optional<OVertex> asVertex() {
    if (this instanceof OVertex)
      return Optional.of((OVertex) this);
    OClass type = this.getSchemaClass();
    if (type == null) {
      return Optional.empty();
    }
    if (type.isVertexType()) {
      return Optional.of(new OVertexDelegate(this));
    }
    return Optional.empty();
  }

  @Override
  public Optional<OEdge> asEdge() {
    if (this instanceof OEdge)
      return Optional.of((OEdge) this);
    OClass type = this.getSchemaClass();
    if (type == null) {
      return Optional.empty();
    }
    if (type.isEdgeType()) {
      return Optional.of(new OEdgeDelegate(this));
    }
    return Optional.empty();
  }

  @Override
  public boolean isVertex() {
    if (this instanceof OVertex)
      return true;
    OClass type = this.getSchemaClass();
    if (type == null) {
      return false;
    }
    return type.isVertexType();
  }

  @Override
  public boolean isEdge() {
    if (this instanceof OEdge)
      return true;
    OClass type = this.getSchemaClass();
    if (type == null) {
      return false;
    }
    return type.isEdgeType();
  }

  @Override
  public Optional<OClass> getSchemaType() {
    return Optional.ofNullable(getSchemaClass());
  }

  private Stream<String> calculatePropertyNames() {
    checkForLoading();

    if (status == ORecordElement.STATUS.LOADED && source != null && ODatabaseRecordThreadLocal.instance().isDefined()
        && !ODatabaseRecordThreadLocal.instance().get().isClosed()) {
      // DESERIALIZE FIELD NAMES ONLY (SUPPORTED ONLY BY BINARY SERIALIZER)
      final String[] fieldNames = recordFormat.getFieldNamesRoot(this, source);
      if (fieldNames != null) {
        if (propertyAccess != null) {
          return Arrays.stream(fieldNames).filter((e) -> propertyAccess.isReadable(e));
        } else {
          return Arrays.stream(fieldNames);
        }
      }
    }

    checkForFields();

    if (fields == null || fields.size() == 0)
      return Stream.empty();

    return fields.entrySet().stream()
        .filter(s -> s.getValue().exist() && (propertyAccess == null || propertyAccess.isReadable(s.getKey())))
        .map(Entry::getKey);
  }

  @Override
  public Set<String> getPropertyNames() {
    return calculatePropertyNames().collect(Collectors.toSet());
  }

  /**
   * retrieves a property value from the current document
   *
   * @param iFieldName The field name, it can contain any character (it's not evaluated as an expression, as in #eval()
   * @param <RET>
   *
   * @return the field value. Null if the field does not exist.
   */
  public <RET> RET getProperty(final String iFieldName) {
    if (iFieldName == null)
      return null;

    checkForLoading();
    RET value = (RET) ODocumentHelper.getIdentifiableValue(this, iFieldName);

    if (!iFieldName.startsWith("@") && lazyLoad && value instanceof ORID && (((ORID) value).isPersistent() || ((ORID) value)
        .isNew()) && ODatabaseRecordThreadLocal.instance().isDefined()) {
      // CREATE THE DOCUMENT OBJECT IN LAZY WAY
      RET newValue = getDatabase().load((ORID) value);
      if (newValue != null) {
        unTrack((ORID) value);
        track((OIdentifiable) newValue);
        value = newValue;
        if (isTrackingChanges()) {
          ORecordInternal.setDirtyManager((ORecord) value, this.getDirtyManager());
        }
        ODocumentEntry entry = fields.get(iFieldName);
        removeCollectionChangeListener(entry, entry.value);
        entry.value = value;
        addCollectionChangeListener(entry);
      }
    }

    if (value instanceof OElement) {
      if (((OElement) value).isVertex()) {
        value = (RET) ((OElement) value).asVertex().get();
      } else if (((OElement) value).isEdge()) {
        value = (RET) ((OElement) value).asEdge().get();
      }
    }

    return value;
  }

  /**
   * retrieves a property value from the current document, without evaluating it (eg. no conversion from RID to document)
   *
   * @param iFieldName The field name, it can contain any character (it's not evaluated as an expression, as in #eval()
   * @param <RET>
   *
   * @return the field value. Null if the field does not exist.
   */
  protected <RET> RET getRawProperty(final String iFieldName) {
    if (iFieldName == null)
      return null;

    checkForLoading();
    return (RET) ODocumentHelper.getIdentifiableValue(this, iFieldName);
  }

  /**
   * sets a property value on current document
   *
   * @param iFieldName     The property name
   * @param iPropertyValue The property value
   */
  public void setProperty(final String iFieldName, Object iPropertyValue) {
    if (iPropertyValue instanceof OElement && !((OElement) iPropertyValue).getSchemaType().isPresent()
        && !((OElement) iPropertyValue).getIdentity().isValid()) {
      setProperty(iFieldName, iPropertyValue, OType.EMBEDDED);
    } else {
      setProperty(iFieldName, iPropertyValue, OCommonConst.EMPTY_TYPES_ARRAY);
    }
  }

  /**
   * Sets
   *
   * @param iPropetyName   The property name
   * @param iPropertyValue The property value
   * @param iFieldType     Forced type (not auto-determined)
   */
  public void setProperty(String iPropetyName, Object iPropertyValue, OType... iFieldType) {
    if (iPropetyName == null)
      throw new IllegalArgumentException("Field is null");

    if (iPropetyName.isEmpty())
      throw new IllegalArgumentException("Field name is empty");

    if (ODocumentHelper.ATTRIBUTE_CLASS.equals(iPropetyName)) {
      setClassName(iPropertyValue.toString());
      return;
    } else if (ODocumentHelper.ATTRIBUTE_RID.equals(iPropetyName)) {
      recordId.fromString(iPropertyValue.toString());
      return;
    } else if (ODocumentHelper.ATTRIBUTE_VERSION.equals(iPropetyName)) {
      if (iPropertyValue != null) {
        int v;

        if (iPropertyValue instanceof Number)
          v = ((Number) iPropertyValue).intValue();
        else
          v = Integer.parseInt(iPropertyValue.toString());

        recordVersion = v;
      }
      return;
    }

    checkForLoading();
    checkForFields();

    ODocumentEntry entry = fields.get(iPropetyName);
    final boolean knownProperty;
    final Object oldValue;
    final OType oldType;
    if (entry == null) {
      entry = new ODocumentEntry();
      fieldSize++;
      fields.put(iPropetyName, entry);
      entry.setCreated(true);
      knownProperty = false;
      oldValue = null;
      oldType = null;
    } else {
      knownProperty = entry.exist();
      oldValue = entry.value;
      oldType = entry.type;
    }
    OType fieldType = deriveFieldType(iPropetyName, entry, iFieldType);
    if (iPropertyValue != null && fieldType != null) {
      iPropertyValue = ODocumentHelper.convertField(this, iPropetyName, fieldType, null, iPropertyValue);
    } else if (iPropertyValue instanceof Enum)
      iPropertyValue = iPropertyValue.toString();

    if (knownProperty)
      // CHECK IF IS REALLY CHANGED
      if (iPropertyValue == null) {
        if (oldValue == null)
          // BOTH NULL: UNCHANGED
          return;
      } else {

        try {
          if (iPropertyValue.equals(oldValue)) {
            if (fieldType == oldType) {
              if (!(iPropertyValue instanceof ORecordElement))
                // SAME BUT NOT TRACKABLE: SET THE RECORD AS DIRTY TO BE SURE IT'S SAVED
                setDirty();

              // SAVE VALUE: UNCHANGED
              return;
            }
          }
        } catch (Exception e) {
          OLogManager.instance()
              .warn(this, "Error on checking the value of property %s against the record %s", e, iPropetyName, getIdentity());
        }
      }

    if (oldValue instanceof ORidBag) {
      final ORidBag ridBag = (ORidBag) oldValue;
      ridBag.setOwner(null);
    } else if (oldValue instanceof ODocument) {
      ((ODocument) oldValue).removeOwner(this);
    }

    if (oldValue instanceof OIdentifiable) {
      unTrack((OIdentifiable) oldValue);
    }

    if (iPropertyValue != null) {
      if (iPropertyValue instanceof ODocument) {
        if (OType.EMBEDDED.equals(fieldType)) {
          final ODocument embeddedDocument = (ODocument) iPropertyValue;
          ODocumentInternal.addOwner(embeddedDocument, this);
        }
      }
      if (iPropertyValue instanceof OIdentifiable) {
        track((OIdentifiable) iPropertyValue);
      }

      if (iPropertyValue instanceof ORidBag) {
        final ORidBag ridBag = (ORidBag) iPropertyValue;
        ridBag.setOwner(null); // in order to avoid IllegalStateException when ridBag changes the owner (ODocument.merge)
        ridBag.setOwner(this);
      }
    }

    if (oldType != fieldType && oldType != null) {
      // can be made in a better way, but "keeping type" issue should be solved before
      if (iPropertyValue == null || fieldType != null || oldType != OType.getTypeByValue(iPropertyValue))
        entry.type = fieldType;
    }
    removeCollectionChangeListener(entry, oldValue);
    entry.value = iPropertyValue;
    if (!entry.exist()) {
      entry.setExist(true);
      fieldSize++;
    }
    addCollectionChangeListener(entry);

    if (status != STATUS.UNMARSHALLING) {
      setDirty();
      if (!entry.isChanged()) {
        entry.original = oldValue;
        entry.setChanged(true);
      }
    }
  }

  public <RET> RET removeProperty(final String iFieldName) {
    checkForLoading();
    checkForFields();

    if (ODocumentHelper.ATTRIBUTE_CLASS.equalsIgnoreCase(iFieldName)) {
      setClassName(null);
    } else if (ODocumentHelper.ATTRIBUTE_RID.equalsIgnoreCase(iFieldName)) {
      recordId = new ORecordId();
    }

    final ODocumentEntry entry = fields.get(iFieldName);
    if (entry == null)
      return null;
    Object oldValue = entry.value;
    if (entry.exist() && trackingChanges) {
      // SAVE THE OLD VALUE IN A SEPARATE MAP
      if (entry.original == null)
        entry.original = entry.value;
      entry.value = null;
      entry.setExist(false);
      entry.setChanged(true);
    } else {
      fields.remove(iFieldName);
    }
    fieldSize--;

    removeCollectionChangeListener(entry, oldValue);
    if (oldValue instanceof OIdentifiable)
      unTrack((OIdentifiable) oldValue);
    if (oldValue instanceof ORidBag)
      ((ORidBag) oldValue).setOwner(null);
    setDirty();
    return (RET) oldValue;
  }

  protected static void validateFieldSecurity(ODatabaseDocumentInternal internal, ODocument iRecord, OImmutableProperty p)
      throws OValidationException {
    if (internal == null) {
      return;
    }
    ODocumentEntry entry = iRecord.fields.get(p.getName());
    if (entry != null && (entry.isChanged() || entry.timeLine != null)) {
      OSecurityInternal security = internal.getSharedContext().getSecurity();
      if (!security.isAllowedWrite(iRecord, p.getName())) {
        throw new OSecurityException(
            String.format("Change of field '%s' is not allowed for user '%s'", p.getFullName(), internal.getUser().getName()));
      }
    }
  }

  protected static void validateField(ODatabaseDocumentInternal internal, ODocument iRecord, OImmutableProperty p)
      throws OValidationException {
    validateFieldSecurity(internal, iRecord, p);
    final Object fieldValue;
    ODocumentEntry entry = iRecord.fields.get(p.getName());
    if (entry != null && entry.exist()) {
      // AVOID CONVERSIONS: FASTER!
      fieldValue = entry.value;

      if (p.isNotNull() && fieldValue == null)
        // NULLITY
        throw new OValidationException("The field '" + p.getFullName() + "' cannot be null, record: " + iRecord);

      if (fieldValue != null && p.getRegexp() != null && p.getType().equals(OType.STRING)) {
        // REGEXP
        if (!((String) fieldValue).matches(p.getRegexp()))
          throw new OValidationException(
              "The field '" + p.getFullName() + "' does not match the regular expression '" + p.getRegexp() + "'. Field value is: "
                  + fieldValue + ", record: " + iRecord);
      }

    } else {
      if (p.isMandatory()) {
        throw new OValidationException("The field '" + p.getFullName() + "' is mandatory, but not found on record: " + iRecord);
      }
      fieldValue = null;
    }

    final OType type = p.getType();

    if (fieldValue != null && type != null) {
      // CHECK TYPE
      switch (type) {
      case LINK:
        validateLink(p, fieldValue, false);
        break;
      case LINKLIST:
        if (!(fieldValue instanceof List))
          throw new OValidationException(
              "The field '" + p.getFullName() + "' has been declared as LINKLIST but an incompatible type is used. Value: "
                  + fieldValue);
        validateLinkCollection(p, (Collection<Object>) fieldValue, entry);
        break;
      case LINKSET:
        if (!(fieldValue instanceof Set))
          throw new OValidationException(
              "The field '" + p.getFullName() + "' has been declared as LINKSET but an incompatible type is used. Value: "
                  + fieldValue);
        validateLinkCollection(p, (Collection<Object>) fieldValue, entry);
        break;
      case LINKMAP:
        if (!(fieldValue instanceof Map))
          throw new OValidationException(
              "The field '" + p.getFullName() + "' has been declared as LINKMAP but an incompatible type is used. Value: "
                  + fieldValue);
        validateLinkCollection(p, ((Map<?, Object>) fieldValue).values(), entry);
        break;

      case LINKBAG:
        if (!(fieldValue instanceof ORidBag))
          throw new OValidationException(
              "The field '" + p.getFullName() + "' has been declared as LINKBAG but an incompatible type is used. Value: "
                  + fieldValue);
        validateLinkCollection(p, (Iterable<Object>) fieldValue, entry);
        break;
      case EMBEDDED:
        validateEmbedded(p, fieldValue);
        break;
      case EMBEDDEDLIST:
        if (!(fieldValue instanceof List))
          throw new OValidationException(
              "The field '" + p.getFullName() + "' has been declared as EMBEDDEDLIST but an incompatible type is used. Value: "
                  + fieldValue);
        if (p.getLinkedClass() != null) {
          for (Object item : ((List<?>) fieldValue))
            validateEmbedded(p, item);
        } else if (p.getLinkedType() != null) {
          for (Object item : ((List<?>) fieldValue))
            validateType(p, item);
        }
        break;
      case EMBEDDEDSET:
        if (!(fieldValue instanceof Set))
          throw new OValidationException(
              "The field '" + p.getFullName() + "' has been declared as EMBEDDEDSET but an incompatible type is used. Value: "
                  + fieldValue);
        if (p.getLinkedClass() != null) {
          for (Object item : ((Set<?>) fieldValue))
            validateEmbedded(p, item);
        } else if (p.getLinkedType() != null) {
          for (Object item : ((Set<?>) fieldValue))
            validateType(p, item);
        }
        break;
      case EMBEDDEDMAP:
        if (!(fieldValue instanceof Map))
          throw new OValidationException(
              "The field '" + p.getFullName() + "' has been declared as EMBEDDEDMAP but an incompatible type is used. Value: "
                  + fieldValue);
        if (p.getLinkedClass() != null) {
          for (Entry<?, ?> colleEntry : ((Map<?, ?>) fieldValue).entrySet())
            validateEmbedded(p, colleEntry.getValue());
        } else if (p.getLinkedType() != null) {
          for (Entry<?, ?> collEntry : ((Map<?, ?>) fieldValue).entrySet())
            validateType(p, collEntry.getValue());
        }
        break;
      }
    }

    if (p.getMin() != null && fieldValue != null) {
      // MIN
      final String min = p.getMin();
      if (p.getMinComparable().compareTo(fieldValue) > 0) {
        switch (p.getType()) {
        case STRING:
          throw new OValidationException(
              "The field '" + p.getFullName() + "' contains fewer characters than " + min + " requested");
        case DATE:
        case DATETIME:
          throw new OValidationException(
              "The field '" + p.getFullName() + "' contains the date " + fieldValue + " which precedes the first acceptable date ("
                  + min + ")");
        case BINARY:
          throw new OValidationException("The field '" + p.getFullName() + "' contains fewer bytes than " + min + " requested");
        case EMBEDDEDLIST:
        case EMBEDDEDSET:
        case LINKLIST:
        case LINKSET:
        case EMBEDDEDMAP:
        case LINKMAP:
          throw new OValidationException("The field '" + p.getFullName() + "' contains fewer items than " + min + " requested");
        default:
          throw new OValidationException("The field '" + p.getFullName() + "' is less than " + min);
        }
      }
    }

    if (p.getMaxComparable() != null && fieldValue != null) {
      final String max = p.getMax();
      if (p.getMaxComparable().compareTo(fieldValue) < 0) {
        switch (p.getType()) {
        case STRING:
          throw new OValidationException("The field '" + p.getFullName() + "' contains more characters than " + max + " requested");
        case DATE:
        case DATETIME:
          throw new OValidationException(
              "The field '" + p.getFullName() + "' contains the date " + fieldValue + " which is after the last acceptable date ("
                  + max + ")");
        case BINARY:
          throw new OValidationException("The field '" + p.getFullName() + "' contains more bytes than " + max + " requested");
        case EMBEDDEDLIST:
        case EMBEDDEDSET:
        case LINKLIST:
        case LINKSET:
        case EMBEDDEDMAP:
        case LINKMAP:
          throw new OValidationException("The field '" + p.getFullName() + "' contains more items than " + max + " requested");
        default:
          throw new OValidationException("The field '" + p.getFullName() + "' is greater than " + max);
        }
      }
    }

    if (p.isReadonly() && !ORecordVersionHelper.isTombstone(iRecord.getVersion())) {
      if (entry != null && (entry.changed || entry.timeLine != null) && !entry.created) {
        // check if the field is actually changed by equal.
        // this is due to a limitation in the merge algorithm used server side marking all non simple fields as dirty
        Object orgVal = entry.original;
        boolean simple = fieldValue != null ? OType.isSimpleType(fieldValue) : OType.isSimpleType(orgVal);
        if ((simple) || (fieldValue != null && orgVal == null) || (fieldValue == null && orgVal != null) || (fieldValue != null
            && !fieldValue.equals(orgVal)))
          throw new OValidationException(
              "The field '" + p.getFullName() + "' is immutable and cannot be altered. Field value is: " + entry.value);
      }
    }
  }

  protected static void validateLinkCollection(final OProperty property, Iterable<Object> values, ODocumentEntry value) {
    if (property.getLinkedClass() != null) {
      if (value.timeLine != null) {
        List<OMultiValueChangeEvent<Object, Object>> event = value.timeLine.getMultiValueChangeEvents();
        for (OMultiValueChangeEvent object : event) {
          if (object.getChangeType() == OMultiValueChangeEvent.OChangeType.ADD
              || object.getChangeType() == OMultiValueChangeEvent.OChangeType.UPDATE && object.getValue() != null)
            validateLink(property, object.getValue(), OSecurityShared.ALLOW_FIELDS.contains(property.getName()));
        }
      } else {
        boolean autoconvert = false;
        if (values instanceof ORecordLazyMultiValue) {
          autoconvert = ((ORecordLazyMultiValue) values).isAutoConvertToRecord();
          ((ORecordLazyMultiValue) values).setAutoConvertToRecord(false);
        }
        for (Object object : values) {
          validateLink(property, object, OSecurityShared.ALLOW_FIELDS.contains(property.getName()));
        }
        if (values instanceof ORecordLazyMultiValue)
          ((ORecordLazyMultiValue) values).setAutoConvertToRecord(autoconvert);
      }
    }
  }

  protected static void validateType(final OProperty p, final Object value) {
    if (value != null)
      if (OType.convert(value, p.getLinkedType().getDefaultJavaType()) == null)
        throw new OValidationException(
            "The field '" + p.getFullName() + "' has been declared as " + p.getType() + " of type '" + p.getLinkedType()
                + "' but the value is " + value);
  }

  protected static void validateLink(final OProperty p, final Object fieldValue, boolean allowNull) {
    if (fieldValue == null) {
      if (allowNull)
        return;
      else
        throw new OValidationException("The field '" + p.getFullName() + "' has been declared as " + p.getType()
            + " but contains a null record (probably a deleted record?)");
    }

    final ORecord linkedRecord;
    if (!(fieldValue instanceof OIdentifiable))
      throw new OValidationException("The field '" + p.getFullName() + "' has been declared as " + p.getType()
          + " but the value is not a record or a record-id");
    final OClass schemaClass = p.getLinkedClass();
    if (schemaClass != null && !schemaClass.isSubClassOf(OIdentity.CLASS_NAME)) {
      // DON'T VALIDATE OUSER AND OROLE FOR SECURITY RESTRICTIONS

      final ORID rid = ((OIdentifiable) fieldValue).getIdentity();

      if (!schemaClass.hasPolymorphicClusterId(rid.getClusterId())) {
        linkedRecord = ((OIdentifiable) fieldValue).getRecord();
        if (linkedRecord != null) {
          if (!(linkedRecord instanceof ODocument))
            throw new OValidationException(
                "The field '" + p.getFullName() + "' has been declared as " + p.getType() + " of type '" + schemaClass
                    + "' but the value is the record " + linkedRecord.getIdentity() + " that is not a document");

          final ODocument doc = (ODocument) linkedRecord;

          // AT THIS POINT CHECK THE CLASS ONLY IF != NULL BECAUSE IN CASE OF GRAPHS THE RECORD COULD BE PARTIAL
          if (doc.getImmutableSchemaClass() != null && !schemaClass.isSuperClassOf(doc.getImmutableSchemaClass()))
            throw new OValidationException(
                "The field '" + p.getFullName() + "' has been declared as " + p.getType() + " of type '" + schemaClass.getName()
                    + "' but the value is the document " + linkedRecord.getIdentity() + " of class '" + doc
                    .getImmutableSchemaClass() + "'");
        }
      }
    }
  }

  protected static void validateEmbedded(final OProperty p, final Object fieldValue) {
    if (fieldValue == null)
      return;
    if (fieldValue instanceof ORecordId)
      throw new OValidationException(
          "The field '" + p.getFullName() + "' has been declared as " + p.getType() + " but the value is the RecordID "
              + fieldValue);
    else if (fieldValue instanceof OIdentifiable) {
      final OIdentifiable embedded = (OIdentifiable) fieldValue;
      if (embedded.getIdentity().isValid())
        throw new OValidationException("The field '" + p.getFullName() + "' has been declared as " + p.getType()
            + " but the value is a document with the valid RecordID " + fieldValue);

      final OClass embeddedClass = p.getLinkedClass();
      if (embeddedClass != null) {
        final ORecord embeddedRecord = embedded.getRecord();
        if (!(embeddedRecord instanceof ODocument))
          throw new OValidationException(
              "The field '" + p.getFullName() + "' has been declared as " + p.getType() + " with linked class '" + embeddedClass
                  + "' but the record was not a document");

        final ODocument doc = (ODocument) embeddedRecord;
        if (doc.getImmutableSchemaClass() == null)
          throw new OValidationException(
              "The field '" + p.getFullName() + "' has been declared as " + p.getType() + " with linked class '" + embeddedClass
                  + "' but the record has no class");

        if (!(doc.getImmutableSchemaClass().isSubClassOf(embeddedClass)))
          throw new OValidationException(
              "The field '" + p.getFullName() + "' has been declared as " + p.getType() + " with linked class '" + embeddedClass
                  + "' but the record is of class '" + doc.getImmutableSchemaClass().getName()
                  + "' that is not a subclass of that");

        doc.validate();
      }

    } else
      throw new OValidationException(
          "The field '" + p.getFullName() + "' has been declared as " + p.getType() + " but an incompatible type is used. Value: "
              + fieldValue);
  }

  /**
   * Copies the current instance to a new one. Hasn't been choose the clone() to let ODocument return type. Once copied the new
   * instance has the same identity and values but all the internal structure are totally independent by the source.
   */
  public ODocument copy() {
    return (ODocument) copyTo(new ODocument());
  }

  /**
   * Copies all the fields into iDestination document.
   */
  @Override
  public ORecordAbstract copyTo(final ORecordAbstract iDestination) {
    // TODO: REMOVE THIS
    checkForFields();

    ODocument destination = (ODocument) iDestination;

    super.copyTo(iDestination);

    destination.ordered = ordered;

    destination.className = className;
    destination.immutableSchemaVersion = -1;
    destination.immutableClazz = null;

    destination.trackingChanges = trackingChanges;
    if (owners != null)
      destination.owners = new ArrayList<>(owners);
    else
      destination.owners = null;

    if (fields != null) {
      destination.fields = fields instanceof LinkedHashMap ? new LinkedHashMap<>() : new HashMap<>();
      for (Entry<String, ODocumentEntry> entry : fields.entrySet()) {
        ODocumentEntry docEntry = entry.getValue().clone();
        destination.fields.put(entry.getKey(), docEntry);
        docEntry.value = ODocumentHelper.cloneValue(destination, entry.getValue().value);
      }
    } else
      destination.fields = null;
    destination.fieldSize = fieldSize;
    destination.addAllMultiValueChangeListeners();

    destination.dirty = dirty; // LEAVE IT AS LAST TO AVOID SOMETHING SET THE FLAG TO TRUE
    destination.contentChanged = contentChanged;

    return destination;
  }

  /**
   * Returns an empty record as place-holder of the current. Used when a record is requested, but only the identity is needed.
   *
   * @return placeholder of this document
   */
  public ORecord placeholder() {
    final ODocument cloned = new ODocument();
    cloned.source = null;
    cloned.recordId = recordId;
    cloned.status = STATUS.NOT_LOADED;
    cloned.dirty = false;
    cloned.contentChanged = false;
    return cloned;
  }

  /**
   * Detaches all the connected records. If new records are linked to the document the detaching cannot be completed and false will
   * be returned. RidBag types cannot be fully detached when the database is connected using "remote" protocol.
   *
   * @return true if the record has been detached, otherwise false
   */
  public boolean detach() {
    deserializeFields();
    boolean fullyDetached = true;

    if (fields != null) {
      Object fieldValue;
      for (Map.Entry<String, ODocumentEntry> entry : fields.entrySet()) {
        fieldValue = entry.getValue().value;

        if (fieldValue instanceof ORecord)
          if (((ORecord) fieldValue).getIdentity().isNew())
            fullyDetached = false;
          else
            entry.getValue().value = ((ORecord) fieldValue).getIdentity();

        if (fieldValue instanceof ODetachable) {
          if (!((ODetachable) fieldValue).detach())
            fullyDetached = false;
        }
      }
    }

    return fullyDetached;
  }

  /**
   * Loads the record using a fetch plan. Example: <p> <code>doc.load( "*:3" ); // LOAD THE DOCUMENT BY EARLY FETCHING UP TO 3rd
   * LEVEL OF CONNECTIONS</code> </p>
   *
   * @param iFetchPlan Fetch plan to use
   */
  public ODocument load(final String iFetchPlan) {
    return load(iFetchPlan, false);
  }

  /**
   * Loads the record using a fetch plan. Example: <p> <code>doc.load( "*:3", true ); // LOAD THE DOCUMENT BY EARLY FETCHING UP TO
   * 3rd LEVEL OF CONNECTIONS IGNORING THE CACHE</code> </p>
   *
   * @param iIgnoreCache Ignore the cache or use it
   */
  public ODocument load(final String iFetchPlan, boolean iIgnoreCache) {
    Object result;
    try {
      result = getDatabase().load(this, iFetchPlan, iIgnoreCache);
    } catch (Exception e) {
      throw OException.wrapException(new ORecordNotFoundException(getIdentity()), e);
    }

    if (result == null)
      throw new ORecordNotFoundException(getIdentity());

    return (ODocument) result;
  }

  @Override
  public ODocument reload(final String fetchPlan, final boolean ignoreCache) {
    super.reload(fetchPlan, ignoreCache);
    if (!lazyLoad) {
      checkForLoading();
      checkForFields();
    }
    return this;
  }

  public boolean hasSameContentOf(final ODocument iOther) {
    final ODatabaseDocumentInternal currentDb = ODatabaseRecordThreadLocal.instance().getIfDefined();
    return ODocumentHelper.hasSameContentOf(this, currentDb, iOther, currentDb, null);
  }

  @Override
  public byte[] toStream() {
    if (recordFormat == null)
      setup(ODatabaseRecordThreadLocal.instance().getIfDefined());
    return toStream(false);
  }

  /**
   * Returns the document as Map String,Object . If the document has identity, then the @rid entry is valued. If the document has a
   * class, then the @class entry is valued.
   *
   * @since 2.0
   */
  public Map<String, Object> toMap() {
    final Map<String, Object> map = new HashMap<>();
    for (String field : fieldNames())
      map.put(field, field(field));

    final ORID id = getIdentity();
    if (id.isValid())
      map.put(ODocumentHelper.ATTRIBUTE_RID, id);

    final String className = getClassName();
    if (className != null)
      map.put(ODocumentHelper.ATTRIBUTE_CLASS, className);

    return map;
  }

  /**
   * Dumps the instance as string.
   */
  @Override
  public String toString() {
    return toString(new HashSet<>());
  }

  /**
   * Fills the ODocument directly with the string representation of the document itself. Use it for faster insertion but pay
   * attention to respect the OrientDB record format. <p> <code> record.reset();<br> record.setClassName("Account");<br>
   * record.fromString(new String("Account@id:" + data.getCyclesDone() + ",name:'Luca',surname:'Garulli',birthDate:" +
   * date.getTime()<br> + ",salary:" + 3000f + i));<br> record.save();<br> </code> </p>
   *
   * @param iValue String representation of the record.
   */
  @Deprecated
  public void fromString(final String iValue) {
    dirty = true;
    contentChanged = true;
    try {
      source = iValue.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw OException.wrapException(new OSerializationException("Error reading content from string"), e);
    }

    removeAllCollectionChangeListeners();

    fields = null;
    fieldSize = 0;
  }

  /**
   * Returns the set of field names.
   */
  public String[] fieldNames() {
    return calculatePropertyNames().collect(Collectors.toList()).toArray(new String[] {});
  }

  /**
   * Returns the array of field values.
   */
  public Object[] fieldValues() {
    checkForLoading();
    checkForFields();
    final List<Object> res = new ArrayList<>(fields.size());
    for (Map.Entry<String, ODocumentEntry> entry : fields.entrySet()) {
      if (entry.getValue().exist() && (propertyAccess == null || propertyAccess.isReadable(entry.getKey())))
        res.add(entry.getValue().value);
    }
    return res.toArray();
  }

  public <RET> RET rawField(final String iFieldName) {
    if (iFieldName == null || iFieldName.length() == 0)
      return null;

    checkForLoading();
    if (!checkForFields(iFieldName))
      // NO FIELDS
      return null;

    // OPTIMIZATION
    if (!allowChainedAccess || (iFieldName.charAt(0) != '@' && OStringSerializerHelper.indexOf(iFieldName, 0, '.', '[') == -1)) {
      return (RET) accessProperty(iFieldName);
    }

    // NOT FOUND, PARSE THE FIELD NAME
    return (RET) ODocumentHelper.getFieldValue(this, iFieldName);
  }

  /**
   * Evaluates a SQL expression against current document. Example: <code>long amountPlusVat = doc.eval("amount * 120 /
   * 100");</code>
   *
   * @param iExpression SQL expression to evaluate.
   *
   * @return The result of expression
   *
   * @throws OQueryParsingException in case the expression is not valid
   */
  public Object eval(final String iExpression) {
    return eval(iExpression, null);
  }

  /**
   * Evaluates a SQL expression against current document by passing a context. The expression can refer to the variables contained
   * in the context. Example: <code> OCommandContext context = new OBasicCommandContext().setVariable("vat", 20); long amountPlusVat
   * = doc.eval("amount * (100+$vat) / 100", context); </code>
   *
   * @param iExpression SQL expression to evaluate.
   *
   * @return The result of expression
   *
   * @throws OQueryParsingException in case the expression is not valid
   */
  public Object eval(final String iExpression, final OCommandContext iContext) {
    return new OSQLPredicate(iExpression).evaluate(this, null, iContext);
  }

  /**
   * Reads the field value.
   *
   * @param iFieldName field name
   *
   * @return field value if defined, otherwise null
   */
  @Override
  public <RET> RET field(final String iFieldName) {
    RET value = this.rawField(iFieldName);

    if (!iFieldName.startsWith("@") && lazyLoad && value instanceof ORID && (((ORID) value).isPersistent() || ((ORID) value)
        .isNew()) && ODatabaseRecordThreadLocal.instance().isDefined()) {
      // CREATE THE DOCUMENT OBJECT IN LAZY WAY
      RET newValue = getDatabase().load((ORID) value);
      if (newValue != null) {
        unTrack((ORID) value);
        track((OIdentifiable) newValue);
        value = newValue;
        if (this.isTrackingChanges()) {
          ORecordInternal.setDirtyManager((ORecord) value, this.getDirtyManager());
        }
        if (!iFieldName.contains(".")) {
          ODocumentEntry entry = fields.get(iFieldName);
          removeCollectionChangeListener(entry, entry.value);
          entry.value = value;
          addCollectionChangeListener(entry);
        }
      }
    }

    return value;
  }

  /**
   * Reads the field value forcing the return type. Use this method to force return of ORID instead of the entire document by
   * passing ORID.class as iFieldType.
   *
   * @param iFieldName field name
   * @param iFieldType Forced type.
   *
   * @return field value if defined, otherwise null
   */
  public <RET> RET field(final String iFieldName, final Class<?> iFieldType) {
    RET value = this.rawField(iFieldName);

    if (value != null)
      value = ODocumentHelper.convertField(this, iFieldName, OType.getTypeByClass(iFieldType), iFieldType, value);

    return value;
  }

  /**
   * Reads the field value forcing the return type. Use this method to force return of binary data.
   *
   * @param iFieldName field name
   * @param iFieldType Forced type.
   *
   * @return field value if defined, otherwise null
   */
  public <RET> RET field(final String iFieldName, final OType iFieldType) {
    RET value = field(iFieldName);
    OType original;
    if (iFieldType != null && iFieldType != (original = fieldType(iFieldName))) {
      // this is needed for the csv serializer that don't give back values
      if (original == null) {
        original = OType.getTypeByValue(value);
        if (iFieldType == original)
          return value;
      }

      final Object newValue;

      if (iFieldType == OType.BINARY && value instanceof String)
        newValue = OStringSerializerHelper.getBinaryContent(value);
      else if (iFieldType == OType.DATE && value instanceof Long)
        newValue = new Date((Long) value);
      else if ((iFieldType == OType.EMBEDDEDSET || iFieldType == OType.LINKSET) && value instanceof List)
        newValue = Collections.unmodifiableSet((Set<?>) ODocumentHelper.convertField(this, iFieldName, iFieldType, null, value));
      else if ((iFieldType == OType.EMBEDDEDLIST || iFieldType == OType.LINKLIST) && value instanceof Set)
        newValue = Collections.unmodifiableList((List<?>) ODocumentHelper.convertField(this, iFieldName, iFieldType, null, value));
      else if ((iFieldType == OType.EMBEDDEDMAP || iFieldType == OType.LINKMAP) && value instanceof Map)
        newValue = Collections.unmodifiableMap((Map<?, ?>) ODocumentHelper.convertField(this, iFieldName, iFieldType, null, value));
      else
        newValue = OType.convert(value, iFieldType.getDefaultJavaType());

      if (newValue != null)
        value = (RET) newValue;

    }
    return value;
  }

  /**
   * Writes the field value. This method sets the current document as dirty.
   *
   * @param iFieldName     field name. If contains dots (.) the change is applied to the nested documents in chain. To disable this
   *                       feature call {@link #setAllowChainedAccess(boolean)} to false.
   * @param iPropertyValue field value
   *
   * @return The Record instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  public ODocument field(final String iFieldName, Object iPropertyValue) {
    return field(iFieldName, iPropertyValue, OCommonConst.EMPTY_TYPES_ARRAY);
  }

  /**
   * Fills a document passing the field names/values.
   */
  public ODocument fields(final String iFieldName, final Object iFieldValue, final Object... iFields) {
    if (iFields != null && iFields.length % 2 != 0)
      throw new IllegalArgumentException("Fields must be passed in pairs as name and value");

    field(iFieldName, iFieldValue);
    if (iFields != null && iFields.length > 0)
      for (int i = 0; i < iFields.length; i += 2) {
        field(iFields[i].toString(), iFields[i + 1]);
      }
    return this;
  }

  /**
   * Deprecated. Use fromMap(Map) instead.<br> Fills a document passing the field names/values as a Map String,Object where the keys
   * are the field names and the values are the field values.
   *
   * @see #fromMap(Map)
   */
  @Deprecated
  public ODocument fields(final Map<String, Object> iMap) {
    return fromMap(iMap);
  }

  /**
   * Fills a document passing the field names/values as a Map String,Object where the keys are the field names and the values are
   * the field values. It accepts also @rid for record id and @class for class name.
   *
   * @since 2.0
   */
  public ODocument fromMap(final Map<String, ?> iMap) {
    if (iMap != null) {
      for (Entry<String, ?> entry : iMap.entrySet())
        field(entry.getKey(), entry.getValue());
    }
    return this;
  }

  /**
   * Writes the field value forcing the type. This method sets the current document as dirty.
   * <p>
   * if there's a schema definition for the specified field, the value will be converted to respect the schema definition if needed.
   * if the type defined in the schema support less precision than the iPropertyValue provided, the iPropertyValue will be converted
   * following the java casting rules with possible precision loss.
   *
   * @param iFieldName     field name. If contains dots (.) the change is applied to the nested documents in chain. To disable this
   *                       feature call {@link #setAllowChainedAccess(boolean)} to false.
   * @param iPropertyValue field value.
   * @param iFieldType     Forced type (not auto-determined)
   *
   * @return The Record instance itself giving a "fluent interface". Useful to call multiple methods in chain. If the updated
   * document is another document (using the dot (.) notation) then the document returned is the changed one or NULL if no document
   * has been found in chain
   */
  public ODocument field(String iFieldName, Object iPropertyValue, OType... iFieldType) {
    if (iFieldName == null)
      throw new IllegalArgumentException("Field is null");

    if (iFieldName.isEmpty())
      throw new IllegalArgumentException("Field name is empty");

    if (ODocumentHelper.ATTRIBUTE_CLASS.equals(iFieldName)) {
      setClassName(iPropertyValue.toString());
      return this;
    } else if (ODocumentHelper.ATTRIBUTE_RID.equals(iFieldName)) {
      recordId.fromString(iPropertyValue.toString());
      return this;
    } else if (ODocumentHelper.ATTRIBUTE_VERSION.equals(iFieldName)) {
      if (iPropertyValue != null) {
        int v;

        if (iPropertyValue instanceof Number)
          v = ((Number) iPropertyValue).intValue();
        else
          v = Integer.parseInt(iPropertyValue.toString());

        recordVersion = v;
      }
      return this;
    }

    final int lastDotSep = allowChainedAccess ? iFieldName.lastIndexOf('.') : -1;
    final int lastArraySep = allowChainedAccess ? iFieldName.lastIndexOf('[') : -1;

    final int lastSep = Math.max(lastArraySep, lastDotSep);
    final boolean lastIsArray = lastArraySep > lastDotSep;

    if (lastSep > -1) {
      // SUB PROPERTY GET 1 LEVEL BEFORE LAST
      final Object subObject = field(iFieldName.substring(0, lastSep));
      if (subObject != null) {
        final String subFieldName = lastIsArray ? iFieldName.substring(lastSep) : iFieldName.substring(lastSep + 1);
        if (subObject instanceof ODocument) {
          // SUB-DOCUMENT
          ((ODocument) subObject).field(subFieldName, iPropertyValue);
          return (ODocument) (((ODocument) subObject).isEmbedded() ? this : subObject);
        } else if (subObject instanceof Map<?, ?>) {
          // KEY/VALUE
          ((Map<String, Object>) subObject).put(subFieldName, iPropertyValue);
        } else if (OMultiValue.isMultiValue(subObject)) {
          if ((subObject instanceof List<?> || subObject.getClass().isArray()) && lastIsArray) {
            // List // Array Type with a index subscript.
            final int subFieldNameLen = subFieldName.length();

            if (subFieldName.charAt(subFieldNameLen - 1) != ']') {
              throw new IllegalArgumentException("Missed closing ']'");
            }

            final String indexPart = subFieldName.substring(1, subFieldNameLen - 1);
            final Object indexPartObject = ODocumentHelper.getIndexPart(null, indexPart);
            final String indexAsString = indexPartObject == null ? null : indexPartObject.toString();

            try {
              final int index = Integer.parseInt(indexAsString);
              OMultiValue.setValue(subObject, iPropertyValue, index);
            } catch (NumberFormatException e) {
              throw new IllegalArgumentException("List / array subscripts must resolve to integer values.", e);
            }
          } else {
            // APPLY CHANGE TO ALL THE ITEM IN SUB-COLLECTION
            for (Object subObjectItem : OMultiValue.getMultiValueIterable(subObject)) {
              if (subObjectItem instanceof ODocument) {
                // SUB-DOCUMENT, CHECK IF IT'S NOT LINKED
                if (!((ODocument) subObjectItem).isEmbedded())
                  throw new IllegalArgumentException("Property '" + iFieldName
                      + "' points to linked collection of items. You can only change embedded documents in this way");
                ((ODocument) subObjectItem).field(subFieldName, iPropertyValue);
              } else if (subObjectItem instanceof Map<?, ?>) {
                // KEY/VALUE
                ((Map<String, Object>) subObjectItem).put(subFieldName, iPropertyValue);
              }
            }
          }
          return this;
        }
      } else
        throw new IllegalArgumentException("Property '" + iFieldName.substring(0, lastSep)
            + "' is null, is possible to set a value with dotted notation only on not null property");
      return null;
    }

    iFieldName = checkFieldName(iFieldName);

    checkForLoading();
    checkForFields();

    ODocumentEntry entry = fields.get(iFieldName);
    final boolean knownProperty;
    final Object oldValue;
    final OType oldType;
    if (entry == null) {
      entry = new ODocumentEntry();
      fieldSize++;
      fields.put(iFieldName, entry);
      entry.setCreated(true);
      knownProperty = false;
      oldValue = null;
      oldType = null;
    } else {
      knownProperty = entry.exist();
      oldValue = entry.value;
      oldType = entry.type;
    }
    OType fieldType = deriveFieldType(iFieldName, entry, iFieldType);
    if (iPropertyValue != null && fieldType != null) {
      iPropertyValue = ODocumentHelper.convertField(this, iFieldName, fieldType, null, iPropertyValue);
    } else if (iPropertyValue instanceof Enum)
      iPropertyValue = iPropertyValue.toString();

    if (knownProperty)
      // CHECK IF IS REALLY CHANGED
      if (iPropertyValue == null) {
        if (oldValue == null)
          // BOTH NULL: UNCHANGED
          return this;
      } else {

        try {
          if (iPropertyValue.equals(oldValue)) {
            if (fieldType == oldType) {
              if (!(iPropertyValue instanceof ORecordElement))
                // SAME BUT NOT TRACKABLE: SET THE RECORD AS DIRTY TO BE SURE IT'S SAVED
                setDirty();

              // SAVE VALUE: UNCHANGED
              return this;
            }
          } else if (iPropertyValue instanceof byte[] && Arrays.equals((byte[]) iPropertyValue, (byte[]) oldValue)) {
            // SAVE VALUE: UNCHANGED
            return this;
          }
        } catch (Exception e) {
          OLogManager.instance()
              .warn(this, "Error on checking the value of property %s against the record %s", e, iFieldName, getIdentity());
        }
      }

    if (oldValue instanceof ORidBag) {
      final ORidBag ridBag = (ORidBag) oldValue;
      ridBag.setOwner(null);
    } else if (oldValue instanceof ODocument) {
      ((ODocument) oldValue).removeOwner(this);
    }

    if (oldValue instanceof OIdentifiable) {
      unTrack((OIdentifiable) oldValue);
    }

    if (iPropertyValue != null) {
      if (iPropertyValue instanceof ODocument) {
        if (OType.EMBEDDED.equals(fieldType)) {
          final ODocument embeddedDocument = (ODocument) iPropertyValue;
          ODocumentInternal.addOwner(embeddedDocument, this);
        } else if (OType.LINK.equals(fieldType)) {
          final ODocument embeddedDocument = (ODocument) iPropertyValue;
          ODocumentInternal.removeOwner(embeddedDocument, this);
        }
      }
      if (iPropertyValue instanceof OIdentifiable) {
        track((OIdentifiable) iPropertyValue);
      }

      if (iPropertyValue instanceof ORidBag) {
        final ORidBag ridBag = (ORidBag) iPropertyValue;
        ridBag.setOwner(null); // in order to avoid IllegalStateException when ridBag changes the owner (ODocument.merge)
        ridBag.setOwner(this);
      }
    }

    if (oldType != fieldType && oldType != null) {
      // can be made in a better way, but "keeping type" issue should be solved before
      if (iPropertyValue == null || fieldType != null || oldType != OType.getTypeByValue(iPropertyValue))
        entry.type = fieldType;
    }
    removeCollectionChangeListener(entry, oldValue);
    entry.value = iPropertyValue;
    if (!entry.exist()) {
      entry.setExist(true);
      fieldSize++;
    }
    addCollectionChangeListener(entry);

    if (status != STATUS.UNMARSHALLING) {
      setDirty();
      if (!entry.isChanged()) {
        entry.original = oldValue;
        entry.setChanged(true);
      }
    }

    return this;
  }

  /**
   * Removes a field.
   */
  @Override
  public Object removeField(final String iFieldName) {
    checkForLoading();
    checkForFields();

    if (ODocumentHelper.ATTRIBUTE_CLASS.equalsIgnoreCase(iFieldName)) {
      setClassName(null);
    } else if (ODocumentHelper.ATTRIBUTE_RID.equalsIgnoreCase(iFieldName)) {
      recordId = new ORecordId();
    }

    final ODocumentEntry entry = fields.get(iFieldName);
    if (entry == null)
      return null;
    Object oldValue = entry.value;
    if (entry.exist() && trackingChanges) {
      // SAVE THE OLD VALUE IN A SEPARATE MAP
      if (entry.original == null)
        entry.original = entry.value;
      entry.value = null;
      entry.setExist(false);
      entry.setChanged(true);
    } else {
      fields.remove(iFieldName);
    }
    fieldSize--;

    removeCollectionChangeListener(entry, oldValue);
    if (oldValue instanceof OIdentifiable)
      unTrack((OIdentifiable) oldValue);
    if (oldValue instanceof ORidBag)
      ((ORidBag) oldValue).setOwner(null);
    setDirty();
    return oldValue;
  }

  /**
   * Merge current document with the document passed as parameter. If the field already exists then the conflicts are managed based
   * on the value of the parameter 'iUpdateOnlyMode'.
   *
   * @param iOther                              Other ODocument instance to merge
   * @param iUpdateOnlyMode                     if true, the other document properties will always be added or overwritten. If
   *                                            false, the missed properties in the "other" document will be removed by original
   *                                            document
   * @param iMergeSingleItemsOfMultiValueFields If true, merges single items of multi field fields (collections, maps, arrays, etc)
   *
   * @return
   */
  public ODocument merge(final ODocument iOther, boolean iUpdateOnlyMode, boolean iMergeSingleItemsOfMultiValueFields) {
    iOther.checkForLoading();
    iOther.checkForFields();

    if (className == null && iOther.getImmutableSchemaClass() != null)
      className = iOther.getImmutableSchemaClass().getName();

    return mergeMap(iOther.fields, iUpdateOnlyMode, iMergeSingleItemsOfMultiValueFields);
  }

  private static boolean valuesDiffersInClass(Object val1, Object val2) {
    if (val1.getClass().equals(val2.getClass())) {
      return false;
    }
    if ((val1 instanceof ODocument && val2 instanceof ODocumentDelta) || (val1 instanceof ODocumentDelta
        && val2 instanceof ODocument))
      return false;

    return true;
  }

  private static void mergeDocumentToDocument(final ODocument to, final ODocumentDelta from) {
    for (Map.Entry<String, ValueType> field : from.fields()) {
      final String fieldName = field.getKey();
      final ODocumentDelta updateDoc = from.field(fieldName).getValue();
      Object deltaVal = updateDoc.field("v").getValue();
      UpdateDeltaValueType deltaType = UpdateDeltaValueType.fromOrd(updateDoc.field("t").getValue());

      if (deltaType == UpdateDeltaValueType.UPDATE || deltaType == UpdateDeltaValueType.CHANGE) {
        if (!(deltaVal instanceof ODocumentDelta) || !to.fields.keySet().contains(fieldName) || valuesDiffersInClass(
            to.fields.get(fieldName).value, deltaVal)) {
          to.field(fieldName, deltaVal);
        } else {
          final ODocumentDelta fromDocField = (ODocumentDelta) deltaVal;
          final ODocument toDocField = (ODocument) to.field(fieldName);
          ValueType toNestedValueType = new ValueType(toDocField, OType.EMBEDDED);
          ValueType fromNestedValueType = new ValueType(fromDocField, ODeltaDocumentFieldType.DELTA_RECORD);
          mergeUpdateTree(toNestedValueType, fromNestedValueType);
        }
      } else if (deltaType == UpdateDeltaValueType.LIST_UPDATE) {
        List deltaList = (List) deltaVal;
        List originalList = to.field(fieldName);
        OType toLinkedType = HelperClasses.getLinkedType(to, to.fieldType(fieldName), fieldName);
        //we want to treat ANY as null, because elements remain of unknown type
        if (toLinkedType == OType.ANY) {
          toLinkedType = null;
        }
        int removed = 0;
        for (int i = 0; i < deltaList.size(); i++) {
          ODocumentDelta deltaUpdateListElement = (ODocumentDelta) deltaList.get(i);
          UpdateDeltaValueType listElementUpdateType = UpdateDeltaValueType.fromOrd(deltaUpdateListElement.field("t").getValue());
          Object val;
          int index;
          switch (listElementUpdateType) {
          case LIST_ELEMENT_ADD:
            val = deltaUpdateListElement.field("v").getValue();
            originalList.add(val);
            break;
          case LIST_ELEMENT_CHANGE:
            val = deltaUpdateListElement.field("v").getValue();
            index = deltaUpdateListElement.field("i").getValue();
            originalList.set(index, val);
            break;
          case LIST_ELEMENT_REMOVE:
            index = deltaUpdateListElement.field("i").getValue();
            originalList.remove(index - removed++);
            break;
          case LIST_ELEMENT_UPDATE:
            ValueType deltaValueType = deltaUpdateListElement.field("v");
            index = deltaUpdateListElement.field("i").getValue();
            Object originalVal = originalList.get(index);
            //here deltaDeltaVal can be null, then we want to use valDoc. This is case with list of documents
            ODocumentDelta deltaObj = deltaValueType.getValue();
            if (deltaObj.field("t") == null) {
              //in this case some document field should be updated, and in that case update type is not set
              deltaType = UpdateDeltaValueType.UNKNOWN;
            } else {
              deltaType = UpdateDeltaValueType.fromOrd(deltaObj.field("t").getValue());
            }
            ValueType deltaPassObject = deltaValueType;
            if (deltaType == UpdateDeltaValueType.LIST_UPDATE || deltaType == UpdateDeltaValueType.UPDATE) {
              deltaPassObject = deltaObj.field("v");
            }
            ValueType originalValueType = new ValueType(originalVal,
                toLinkedType != null ? toLinkedType : ODeltaDocumentFieldType.getTypeByValue(originalVal));
            Object merged = mergeUpdateTree(originalValueType, deltaPassObject);
            originalList.set(index, merged);
            break;
          }
        }
      } else if (deltaType == UpdateDeltaValueType.RIDBAG_UPDATE) {
        List deltaList = (List) deltaVal;
        ORidBag originalRidbag = to.field(fieldName);
        for (int i = 0; i < deltaList.size(); i++) {
          ODocumentDelta deltaUpdateListElement = (ODocumentDelta) deltaList.get(i);
          UpdateDeltaValueType listElementUpdateType = UpdateDeltaValueType.fromOrd(deltaUpdateListElement.field("t").getValue());
          OIdentifiable val;
          int index;
          switch (listElementUpdateType) {
          case LIST_ELEMENT_ADD:
            val = deltaUpdateListElement.field("v").getValue();
            originalRidbag.add(val);
            break;
          case LIST_ELEMENT_CHANGE:
            val = deltaUpdateListElement.field("v").getValue();
            index = deltaUpdateListElement.field("i").getValue();
            originalRidbag.changeValue(index, val);
            break;
          case LIST_ELEMENT_REMOVE:
            val = deltaUpdateListElement.field("v").getValue();
            originalRidbag.remove(val);
            break;
          }
        }
      }
    }
  }

  private static void mergeListToList(List toList, List fromList) {
    int removed = 0;
    for (int i = 0; i < fromList.size(); i++) {
      Object fromElement = fromList.get(i);
      if (fromElement instanceof ODocumentDelta) {
        ODocumentDelta fromDoc = (ODocumentDelta) fromElement;
        UpdateDeltaValueType type = UpdateDeltaValueType.fromOrd(fromDoc.field("t").getValue());
        if (type == UpdateDeltaValueType.LIST_ELEMENT_ADD) {
          Object addVal = fromDoc.field("v").getValue();
          toList.add(addVal);
          continue;
        }
        int index = fromDoc.field("i").getValue();
        if (type == UpdateDeltaValueType.LIST_ELEMENT_CHANGE) {
          Object val = fromDoc.field("v").getValue();
          toList.set(index, val);
          continue;
        }
        if (type == UpdateDeltaValueType.LIST_ELEMENT_REMOVE) {
          toList.remove(index - removed++);
          continue;
        }
        Object toElement = toList.get(index);

        ValueType fromValueTypeNested = new ValueType(fromDoc, ODeltaDocumentFieldType.DELTA_RECORD);
        if (type == UpdateDeltaValueType.LIST_ELEMENT_UPDATE) {
          fromValueTypeNested = fromDoc.field("v");
        }
        fromDoc = fromValueTypeNested.getValue();
        if (fromDoc.field("t") == null) {
          //this is the case when updating field, in this case update type is not set
          type = UpdateDeltaValueType.UNKNOWN;
        } else {
          type = UpdateDeltaValueType.fromOrd(fromDoc.field("t").getValue());
        }
        if (type == UpdateDeltaValueType.CHANGE) {
          toElement = fromDoc.field("v").getValue();
        } else {
          if (type == UpdateDeltaValueType.LIST_UPDATE || type == UpdateDeltaValueType.UPDATE) {
            fromValueTypeNested = fromDoc.field("v");
          }
          ValueType toValueTypeNested = new ValueType(toElement, ODeltaDocumentFieldType.getTypeByValue(toElement));
          toElement = mergeUpdateTree(toValueTypeNested, fromValueTypeNested);
        }
        toList.set(index, toElement);
      } else {
        //this should never happend, this is invalid delta
        throw new IllegalStateException("Inavlid delta");
      }
    }
  }

  //process in depth first
  private static Object mergeUpdateTree(ValueType toValueType, final ValueType fromValueType) {
    OTypeInterface toType = toValueType.getType();
    OTypeInterface fromType = fromValueType.getType();
    Object toObj = toValueType.getValue();
    Object fromObj = fromValueType.getValue();
    if (toObj instanceof ODocument && fromObj instanceof ODocumentDelta) {
      ODocument to = (ODocument) toObj;
      ODocumentDelta from = (ODocumentDelta) fromObj;
      mergeDocumentToDocument(to, from);
    }
    //here processing nested lists
    else if (fromType.isList() && toType.isList()) {
      //this case should only happen with list of documents
      List fromList = (List) fromObj;
      List toList = (List) toObj;
      mergeListToList(toList, fromList);
    } else if (fromObj instanceof ODocumentDelta) {
      ODocumentDelta from = (ODocumentDelta) fromObj;
      toObj = from.field("v").getValue();
    } else {
      toObj = fromObj;
    }

    return toObj;
  }

  protected ODocument mergeUpdateDelta(final ODocumentDelta iOther) {
    if (iOther == null) {
      return this;
    }

    checkForLoading();
    checkForFields();

    ValueType mergeTo = new ValueType(this, OType.EMBEDDED);
    ValueType mergeFrom = new ValueType(iOther, ODeltaDocumentFieldType.DELTA_RECORD);

    mergeUpdateTree(mergeTo, mergeFrom);

    return this;
  }

  private static void mergeDeleteTree(final ODocument to, final ODocumentDelta from) {
    for (Map.Entry<String, ValueType> field : from.fields()) {
      final String fieldName = field.getKey();
      final ValueType fieldValueType = field.getValue();
      Object fieldVal = fieldValueType.getValue();
      if ((!(fieldVal instanceof ODocumentDelta) && !(fieldVal instanceof List)) || (fieldVal instanceof ODocumentDelta
          && ((ODocumentDelta) fieldVal).isLeaf())) {
        to.removeField(fieldName);
      } else if (fieldVal instanceof List) {
        List<ODocumentDelta> fieldList = (List<ODocumentDelta>) fieldVal;
        List toFieldList = to.field(fieldName);
        for (ODocumentDelta deltaElement : fieldList) {
          int index = deltaElement.field("i").getValue();
          if (toFieldList.size() > index) {
            Object toElement = toFieldList.get(index);
            if (toElement instanceof ODocument) {
              ODocumentDelta fromElement = deltaElement.field("v").getValue();
              mergeDeleteTree((ODocument) toElement, fromElement);
            }
          }
        }
      } else {
        //then it is ODocumentDelta
        if (to.fields.keySet().contains(fieldName)) {
          ODocumentDelta fromDoc = (ODocumentDelta) fieldVal;
          Object toVal = to.field(fieldName);
          if (toVal instanceof ODocument) {
            ODocument toDoc = (ODocument) toVal;
            mergeDeleteTree(toDoc, fromDoc);
          }
        }
      }
    }
  }

  protected ODocument mergeDeleteDelta(final ODocumentDelta iOther) {
    if (iOther == null) {
      return this;
    }

    checkForLoading();
    checkForFields();

    mergeDeleteTree(this, iOther);

    return this;
  }

  public ODocument mergeDelta(final ODocumentDelta delta) {
    ODocumentDelta updateDoc = delta.field("u").getValue();
    ODocumentDelta deleteDoc = delta.field("d").getValue();
    mergeUpdateDelta(updateDoc);
    mergeDeleteDelta(deleteDoc);
    return this;
  }

  /**
   * Merge current document with the document passed as parameter. If the field already exists then the conflicts are managed based
   * on the value of the parameter 'iUpdateOnlyMode'.
   *
   * @param iOther                              Other ODocument instance to merge
   * @param iUpdateOnlyMode                     if true, the other document properties will always be added or overwritten. If
   *                                            false, the missed properties in the "other" document will be removed by original
   *                                            document
   * @param iMergeSingleItemsOfMultiValueFields If true, merges single items of multi field fields (collections, maps, arrays, etc)
   *
   * @return
   */
  public ODocument merge(final Map<String, Object> iOther, final boolean iUpdateOnlyMode,
      boolean iMergeSingleItemsOfMultiValueFields) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns list of changed fields. There are two types of changes: <ol> <li>Value of field itself was changed by calling of {@link
   * #field(String, Object)} method for example.</li> <li>Internal state of field was changed but was not saved. This case currently
   * is applicable for for collections only.</li> </ol>
   *
   * @return List of fields, values of which were changed.
   */
  public String[] getDirtyFields() {
    if (fields == null || fields.isEmpty())
      return EMPTY_STRINGS;

    final Set<String> dirtyFields = new HashSet<>();
    for (Entry<String, ODocumentEntry> entry : fields.entrySet()) {
      if (entry.getValue().isChanged() || entry.getValue().timeLine != null)
        dirtyFields.add(entry.getKey());
    }
    return dirtyFields.toArray(new String[dirtyFields.size()]);
  }

  /**
   * Returns the original value of a field before it has been changed.
   *
   * @param iFieldName Property name to retrieve the original value
   */
  public Object getOriginalValue(final String iFieldName) {
    if (fields != null) {
      ODocumentEntry entry = fields.get(iFieldName);
      if (entry != null)
        return entry.original;
    }
    return null;
  }

  public OMultiValueChangeTimeLine<Object, Object> getCollectionTimeLine(final String iFieldName) {
    ODocumentEntry entry = fields != null ? fields.get(iFieldName) : null;
    return entry != null ? entry.timeLine : null;
  }

  /**
   * Returns the iterator fields
   */
  @Override
  public Iterator<Entry<String, Object>> iterator() {
    checkForLoading();
    checkForFields();

    if (fields == null)
      return OEmptyMapEntryIterator.INSTANCE;

    final Iterator<Entry<String, ODocumentEntry>> iterator = fields.entrySet().iterator();
    return new Iterator<Entry<String, Object>>() {
      private Entry<String, ODocumentEntry> current;
      private boolean read = true;

      @Override
      public boolean hasNext() {
        while (iterator.hasNext()) {
          current = iterator.next();
          if (current.getValue().exist() && (propertyAccess == null || propertyAccess
              .isReadable(current.getKey()))) {
            read = false;
            return true;
          }
        }
        return false;
      }

      @Override
      public Entry<String, Object> next() {
        if (read)
          if (!hasNext()) {
            // Look wrong but is correct, it need to fail if there isn't next.
            iterator.next();
          }
        final Entry<String, Object> toRet = new Entry<String, Object>() {
          private Entry<String, ODocumentEntry> intern = current;

          @Override
          public Object setValue(Object value) {
            throw new UnsupportedOperationException();
          }

          @Override
          public Object getValue() {
            return intern.getValue().value;
          }

          @Override
          public String getKey() {
            return intern.getKey();
          }
        };
        read = true;
        return toRet;
      }

      @Override
      public void remove() {

        if (trackingChanges) {
          if (current.getValue().isChanged())
            current.getValue().original = current.getValue().value;
          current.getValue().value = null;
          current.getValue().setExist(false);
          current.getValue().setChanged(true);
        } else
          iterator.remove();
        fieldSize--;
        removeCollectionChangeListener(current.getValue(), current.getValue().value);
      }
    };
  }

  /**
   * Checks if a field exists.
   *
   * @return True if exists, otherwise false.
   */
  @Override
  public boolean containsField(final String iFieldName) {
    return hasProperty(iFieldName);
  }

  /**
   * Checks if a property exists.
   *
   * @return True if exists, otherwise false.
   */
  @Override
  public boolean hasProperty(final String propertyName) {
    if (propertyName == null)
      return false;

    checkForLoading();
    if (checkForFields(propertyName) && (propertyAccess == null || propertyAccess.isReadable(propertyName))) {
      ODocumentEntry entry = fields.get(propertyName);
      return entry != null && entry.exist();
    } else {
      return false;
    }
  }

  /**
   * Returns true if the record has some owner.
   */
  public boolean hasOwners() {
    return owners != null && !owners.isEmpty();
  }

  @Override
  public ORecordElement getOwner() {
    if (owners == null)
      return null;

    for (WeakReference<ORecordElement> owner : owners) {
      final ORecordElement e = owner.get();
      if (e != null)
        return e;
    }

    return null;
  }

  public Iterable<ORecordElement> getOwners() {
    if (owners == null)
      return Collections.emptyList();

    final List<ORecordElement> result = new ArrayList<>();
    for (WeakReference<ORecordElement> o : owners) {
      if (o.get() != null)
        result.add(o.get());
    }

    return result;
  }

  /**
   * Propagates the dirty status to the owner, if any. This happens when the object is embedded in another one.
   */
  @Override
  public ORecordAbstract setDirty() {
    if (owners != null) {
      // PROPAGATES TO THE OWNER
      ORecordElement e;
      for (WeakReference<ORecordElement> o : owners) {
        e = o.get();
        if (e != null)
          e.setDirty();
      }
    } else if (!isDirty())
      getDirtyManager().setDirty(this);

    // THIS IS IMPORTANT TO BE SURE THAT FIELDS ARE LOADED BEFORE IT'S TOO LATE AND THE RECORD _SOURCE IS NULL
    checkForFields();

    super.setDirty();

    boolean addToChangedList = false;

    ORecordElement owner;
    if (!isEmbedded())
      owner = this;
    else {
      owner = getOwner();
      while (owner != null && owner.getOwner() != null) {
        owner = owner.getOwner();
      }
    }

    if (owner instanceof ODocument && ((ODocument) owner).isTrackingChanges() && ((ODocument) owner).getIdentity().isPersistent())
      addToChangedList = true;

    if (addToChangedList) {
      final ODatabaseDocumentInternal database = getDatabaseIfDefined();

      if (database != null) {
        final OBasicTransaction transaction = database.getMicroOrRegularTransaction();
        transaction.addChangedDocument(this);
      }
    }

    return this;
  }

  @Override
  public void setDirtyNoChanged() {
    if (owners != null) {
      // PROPAGATES TO THE OWNER
      ORecordElement e;
      for (WeakReference<ORecordElement> o : owners) {
        e = o.get();
        if (e != null)
          e.setDirtyNoChanged();
      }
    }

    getDirtyManager().setDirty(this);

    // THIS IS IMPORTANT TO BE SURE THAT FIELDS ARE LOADED BEFORE IT'S TOO LATE AND THE RECORD _SOURCE IS NULL
    checkForFields();

    super.setDirtyNoChanged();
  }

  @Override
  public ODocument fromStream(final byte[] iRecordBuffer) {
    removeAllCollectionChangeListeners();

    fields = null;
    fieldSize = 0;
    contentChanged = false;
    schema = null;
    fetchSchemaIfCan();
    super.fromStream(iRecordBuffer);

    if (!lazyLoad) {
      checkForLoading();
      checkForFields();
    }

    return this;
  }

  /**
   * Returns the forced field type if any.
   *
   * @param iFieldName name of field to check
   */
  public OType fieldType(final String iFieldName) {
    checkForLoading();
    checkForFields(iFieldName);

    ODocumentEntry entry = fields.get(iFieldName);
    if (entry != null) {
      if (propertyAccess == null || propertyAccess.isReadable(iFieldName)) {
        return entry.type;
      } else {
        return null;
      }
    }

    return null;
  }

  @Override
  public ODocument unload() {
    super.unload();
    internalReset();
    return this;
  }

  /**
   * <p> Clears all the field values and types. Clears only record content, but saves its identity. </p>
   * <p/>
   * <p> The following code will clear all data from specified document. </p> <code> doc.clear(); doc.save(); </code>
   *
   * @return this
   *
   * @see #reset()
   */
  @Override
  public ODocument clear() {
    super.clear();
    internalReset();
    owners = null;
    return this;
  }

  /**
   * <p> Resets the record values and class type to being reused. It's like you create a ODocument from scratch. This method is
   * handy when you want to insert a bunch of documents and don't want to strain GC. </p>
   * <p/>
   * <p> The following code will create a new document in database. </p> <code> doc.clear(); doc.save(); </code>
   * <p/>
   * <p> IMPORTANT! This can be used only if no transactions are begun. </p>
   *
   * @return this
   *
   * @throws IllegalStateException if transaction is begun.
   * @see #clear()
   */
  @Override
  public ODocument reset() {
    ODatabaseDocument db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (db != null && db.getTransaction().isActive())
      throw new IllegalStateException("Cannot reset documents during a transaction. Create a new one each time");

    super.reset();

    className = null;
    immutableClazz = null;
    immutableSchemaVersion = -1;

    internalReset();

    owners = null;
    return this;
  }

  /**
   * Rollbacks changes to the loaded version without reloading the document. Works only if tracking changes is enabled @see {@link
   * #isTrackingChanges()} and {@link #setTrackingChanges(boolean)} methods.
   */
  public ODocument undo() {
    if (!trackingChanges)
      throw new OConfigurationException("Cannot undo the document because tracking of changes is disabled");

    if (fields != null) {
      Iterator<Entry<String, ODocumentEntry>> vals = fields.entrySet().iterator();
      while (vals.hasNext()) {
        Entry<String, ODocumentEntry> next = vals.next();
        ODocumentEntry val = next.getValue();
        if (val.created) {
          vals.remove();
        } else if (val.changed) {
          val.value = val.original;
          val.changed = false;
          val.original = null;
          val.exist = true;
        }
      }
      fieldSize = fields.size();
    }

    return this;
  }

  public ODocument undo(final String field) {
    if (!trackingChanges)
      throw new OConfigurationException("Cannot undo the document because tracking of changes is disabled");

    if (fields != null) {
      final ODocumentEntry value = fields.get(field);
      if (value != null) {
        if (value.created) {
          fields.remove(field);
        }
        if (value.changed) {
          value.value = value.original;
          value.original = null;
          value.changed = false;
          value.exist = true;
        }
      }
    }
    return this;
  }

  public boolean isLazyLoad() {
    return lazyLoad;
  }

  public void setLazyLoad(final boolean iLazyLoad) {
    this.lazyLoad = iLazyLoad;
    checkForFields();

    if (fields != null) {
      // PROPAGATE LAZINESS TO THE FIELDS
      for (Entry<String, ODocumentEntry> field : fields.entrySet()) {
        if (field.getValue().value instanceof ORecordLazyMultiValue)
          ((ORecordLazyMultiValue) field.getValue().value).setAutoConvertToRecord(false);
      }
    }
  }

  public boolean isTrackingChanges() {
    return trackingChanges;
  }

  /**
   * Enabled or disabled the tracking of changes in the document. This is needed by some triggers like {@link
   * com.orientechnologies.orient.core.index.OClassIndexManager} to determine what fields are changed to update indexes.
   *
   * @param iTrackingChanges True to enable it, otherwise false
   *
   * @return this
   */
  public ODocument setTrackingChanges(final boolean iTrackingChanges) {
    this.trackingChanges = iTrackingChanges;
    if (!iTrackingChanges && fields != null) {
      // FREE RESOURCES
      Iterator<Entry<String, ODocumentEntry>> iter = fields.entrySet().iterator();
      while (iter.hasNext()) {
        Entry<String, ODocumentEntry> cur = iter.next();
        if (!cur.getValue().exist())
          iter.remove();
        else {
          cur.getValue().setCreated(false);
          cur.getValue().setChanged(false);
          cur.getValue().original = null;
          cur.getValue().timeLine = null;
        }
      }
      removeAllCollectionChangeListeners();
    } else {
      addAllMultiValueChangeListeners();
    }
    return this;
  }

  protected void clearTrackData() {
    if (fields != null) {
      // FREE RESOURCES
      Iterator<Entry<String, ODocumentEntry>> iter = fields.entrySet().iterator();
      while (iter.hasNext()) {
        Entry<String, ODocumentEntry> cur = iter.next();
        if (!cur.getValue().exist())
          iter.remove();
        else {
          cur.getValue().setCreated(false);
          cur.getValue().setChanged(false);
          cur.getValue().original = null;
          cur.getValue().timeLine = null;

          if (cur.getValue().changeListener == null && cur.getValue().value instanceof OTrackedMultiValue<?, ?>) {
            addCollectionChangeListener(cur.getValue());
          }
        }
      }
    }
  }

  public boolean isOrdered() {
    return ordered;
  }

  public ODocument setOrdered(final boolean iOrdered) {
    this.ordered = iOrdered;
    return this;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj))
      return false;

    return this == obj || recordId.isValid();
  }

  @Override
  public int hashCode() {
    if (recordId.isValid())
      return super.hashCode();

    return System.identityHashCode(this);
  }

  /**
   * Returns the number of fields in memory.
   */
  @Override
  public int fields() {
    checkForLoading();
    checkForFields();
    return fieldSize;
  }

  public boolean isEmpty() {
    checkForLoading();
    checkForFields();
    return fields == null || fields.isEmpty();
  }

  @Override
  public ODocument fromJSON(final String iSource, final String iOptions) {
    return (ODocument) super.fromJSON(iSource, iOptions);
  }

  @Override
  public ODocument fromJSON(final String iSource) {
    return (ODocument) super.fromJSON(iSource);
  }

  @Override
  public ODocument fromJSON(final InputStream iContentResult) throws IOException {
    return (ODocument) super.fromJSON(iContentResult);
  }

  @Override
  public ODocument fromJSON(final String iSource, final boolean needReload) {
    return (ODocument) super.fromJSON(iSource, needReload);
  }

  public boolean isEmbedded() {
    return owners != null && !owners.isEmpty();
  }

  /**
   * Sets the field type. This overrides the schema property settings if any.
   *
   * @param iFieldName Field name
   * @param iFieldType Type to set between OType enumeration values
   */
  public ODocument setFieldType(final String iFieldName, final OType iFieldType) {
    checkForLoading();
    checkForFields(iFieldName);
    if (iFieldType != null) {
      if (fields == null)
        fields = ordered ? new LinkedHashMap<>() : new HashMap<>();
      // SET THE FORCED TYPE
      ODocumentEntry entry = getOrCreate(iFieldName);
      if (entry.type != iFieldType)
        field(iFieldName, field(iFieldName), iFieldType);
    } else if (fields != null) {
      // REMOVE THE FIELD TYPE
      ODocumentEntry entry = fields.get(iFieldName);
      if (entry != null)
        // EMPTY: OPTIMIZE IT BY REMOVING THE ENTIRE MAP
        entry.type = null;
    }
    return this;
  }

  @Override
  public ODocument save() {
    return (ODocument) save(null, false);
  }

  @Override
  public ODocument save(final String iClusterName) {
    return (ODocument) save(iClusterName, false);
  }

  @Override
  public ORecordAbstract save(final String iClusterName, final boolean forceCreate) {
    return getDatabase().save(this, iClusterName, ODatabase.OPERATION_MODE.SYNCHRONOUS, forceCreate, null, null);
  }

  /*
   * Initializes the object if has been unserialized
   */
  public boolean deserializeFields(String... iFields) {
    List<String> additional = null;
    if (source == null)
      // ALREADY UNMARSHALLED OR JUST EMPTY
      return true;

    if (iFields != null && iFields.length > 0) {
      // EXTRACT REAL FIELD NAMES
      for (int i = 0; i < iFields.length; ++i) {
        final String f = iFields[i];
        if (f != null && !f.startsWith("@")) {
          int pos1 = f.indexOf('[');
          int pos2 = f.indexOf('.');
          if (pos1 > -1 || pos2 > -1) {
            int pos = pos1 > -1 ? pos1 : pos2;
            if (pos2 > -1 && pos2 < pos)
              pos = pos2;

            // REPLACE THE FIELD NAME
            if (additional == null) {
              additional = new ArrayList<>();
            }
            additional.add(f.substring(0, pos));
          }
        }
      }

      if (additional != null && iFields != null) {
        String[] copy = new String[iFields.length + additional.size()];
        System.arraycopy(iFields, 0, copy, 0, iFields.length);
        int next = iFields.length;
        for (String s : additional) {
          copy[next++] = s;
        }
        iFields = copy;
      }

      // CHECK IF HAS BEEN ALREADY UNMARSHALLED
      if (fields != null && !fields.isEmpty()) {
        boolean allFound = true;
        for (String f : iFields)
          if (f != null && !f.startsWith("@") && !fields.containsKey(f)) {
            allFound = false;
            break;
          }

        if (allFound)
          // ALL THE REQUESTED FIELDS HAVE BEEN LOADED BEFORE AND AVAILABLE, AVOID UNMARSHALLIGN
          return true;
      }
    }

    if (recordFormat == null)
      setup(ODatabaseRecordThreadLocal.instance().getIfDefined());

    status = ORecordElement.STATUS.UNMARSHALLING;
    try {
      recordFormat.fromStream(source, this, iFields);
    } finally {
      status = ORecordElement.STATUS.LOADED;
    }

    if (iFields != null && iFields.length > 0) {
      for (String field : iFields) {
        if (field != null && field.startsWith("@"))
          // ATTRIBUTE
          return true;
      }

      // PARTIAL UNMARSHALLING
      if (fields != null && !fields.isEmpty())
        for (String f : iFields)
          if (f != null && fields.containsKey(f))
            return true;

      // NO FIELDS FOUND
      return false;
    } else if (source != null)
      // FULL UNMARSHALLING
      source = null;

    return true;
  }

  @Override
  public void writeExternal(ObjectOutput stream) throws IOException {
    ORecordSerializer serializer = ORecordSerializerFactory.instance().getFormat(ORecordSerializerNetwork.NAME);
    final byte[] idBuffer = recordId.toStream();
    stream.writeInt(-1);
    stream.writeInt(idBuffer.length);
    stream.write(idBuffer);
    stream.writeInt(recordVersion);

    final byte[] content = serializer.toStream(this, false);
    stream.writeInt(content.length);
    stream.write(content);

    stream.writeBoolean(dirty);
    stream.writeObject(serializer.toString());
  }

  @Override
  public void readExternal(ObjectInput stream) throws IOException, ClassNotFoundException {
    int i = stream.readInt();
    int size;
    if (i < 0)
      size = stream.readInt();
    else
      size = i;
    final byte[] idBuffer = new byte[size];
    stream.readFully(idBuffer);
    recordId.fromStream(idBuffer);

    recordVersion = stream.readInt();

    final int len = stream.readInt();
    final byte[] content = new byte[len];
    stream.readFully(content);

    dirty = stream.readBoolean();

    ORecordSerializer serializer = recordFormat;
    if (i < 0) {
      final String str = (String) stream.readObject();
      // TODO: WHEN TO USE THE SERIALIZER?
      serializer = ORecordSerializerFactory.instance().getFormat(str);
    }

    status = ORecordElement.STATUS.UNMARSHALLING;
    try {
      serializer.fromStream(content, this, null);
    } finally {
      status = ORecordElement.STATUS.LOADED;
    }
  }

  /**
   * Returns the behavior of field() methods allowing access to the sub documents with dot notation ('.'). Default is true. Set it
   * to false if you allow to store properties with the dot.
   */
  public boolean isAllowChainedAccess() {
    return allowChainedAccess;
  }

  /**
   * Change the behavior of field() methods allowing access to the sub documents with dot notation ('.'). Default is true. Set it to
   * false if you allow to store properties with the dot.
   */
  public ODocument setAllowChainedAccess(final boolean allowChainedAccess) {
    this.allowChainedAccess = allowChainedAccess;
    return this;
  }

  @Override
  public void setClassNameIfExists(final String iClassName) {
    immutableClazz = null;
    immutableSchemaVersion = -1;

    className = iClassName;

    if (iClassName == null) {
      return;
    }

    final ODatabaseDocument db = getDatabaseIfDefined();
    if (db != null) {
      final OClass _clazz = ((OMetadataInternal) db.getMetadata()).getImmutableSchemaSnapshot().getClass(iClassName);
      if (_clazz != null) {
        className = _clazz.getName();
        convertFieldsToClass(_clazz);
      }
    }
  }

  @Override
  public OClass getSchemaClass() {
    if (className == null)
      fetchClassName();

    if (className == null)
      return null;

    final ODatabaseDocument databaseRecord = getDatabaseIfDefined();
    if (databaseRecord != null)
      return databaseRecord.getMetadata().getSchema().getClass(className);

    return null;
  }

  @Override
  public String getClassName() {
    if (className == null)
      fetchClassName();

    return className;
  }

  @Override
  public void setClassName(final String className) {
    immutableClazz = null;
    immutableSchemaVersion = -1;

    this.className = className;

    if (className == null) {
      return;
    }

    final ODatabaseDocument db = getDatabaseIfDefined();
    if (db != null) {
      OMetadataInternal metadata = (OMetadataInternal) db.getMetadata();
      this.immutableClazz = (OImmutableClass) metadata.getImmutableSchemaSnapshot().getClass(className);
      OClass clazz;
      if (this.immutableClazz != null) {
        clazz = this.immutableClazz;
      } else {
        clazz = metadata.getSchema().getOrCreateClass(className);
      }
      if (clazz != null) {
        this.className = clazz.getName();
        convertFieldsToClass(clazz);
      }
    }
  }

  /**
   * Validates the record following the declared constraints defined in schema such as mandatory, notNull, min, max, regexp, etc. If
   * the schema is not defined for the current class or there are not constraints then the validation is ignored.
   *
   * @throws OValidationException if the document breaks some validation constraints defined in the schema
   * @see OProperty
   */
  @Override
  public void validate() throws OValidationException {
    checkForLoading();
    checkForFields();

    autoConvertValues();

    ODatabaseDocumentInternal internal = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (internal != null && !internal.isValidationEnabled()) {
      final OImmutableClass immutableSchemaClass = getImmutableSchemaClass();
      if (immutableSchemaClass != null) {
        for (OProperty p : immutableSchemaClass.properties()) {
          validateFieldSecurity(internal, this, (OImmutableProperty) p);
        }
      }
      return;
    }

    final OImmutableClass immutableSchemaClass = getImmutableSchemaClass();
    if (immutableSchemaClass != null) {
      if (immutableSchemaClass.isStrictMode()) {
        // CHECK IF ALL FIELDS ARE DEFINED
        for (String f : fieldNames()) {
          if (immutableSchemaClass.getProperty(f) == null)
            throw new OValidationException(
                "Found additional field '" + f + "'. It cannot be added because the schema class '" + immutableSchemaClass.getName()
                    + "' is defined as STRICT");
        }
      }

      for (OProperty p : immutableSchemaClass.properties()) {
        validateField(internal, this, (OImmutableProperty) p);
      }
    }
  }

  protected String toString(Set<ORecord> inspected) {
    if (inspected.contains(this))
      return "<recursion:rid=" + (recordId != null ? recordId : "null") + ">";
    else
      inspected.add(this);

    final boolean saveDirtyStatus = dirty;
    final boolean oldUpdateContent = contentChanged;

    try {
      final StringBuilder buffer = new StringBuilder(128);

      checkForFields();

      final ODatabaseDocument db = getDatabaseIfDefined();
      if (db != null && !db.isClosed()) {
        final String clsName = getClassName();
        if (clsName != null)
          buffer.append(clsName);
      }

      if (recordId != null) {
        if (recordId.isValid())
          buffer.append(recordId);
      }

      boolean first = true;
      for (Entry<String, ODocumentEntry> f : fields.entrySet()) {
        if (propertyAccess != null && !propertyAccess.isReadable(f.getKey())) {
          continue;
        }
        buffer.append(first ? '{' : ',');
        buffer.append(f.getKey());
        buffer.append(':');
        if (f.getValue().value == null)
          buffer.append("null");
        else if (f.getValue().value instanceof Collection<?> || f.getValue().value instanceof Map<?, ?> || f.getValue().value
            .getClass().isArray()) {
          buffer.append('[');
          buffer.append(OMultiValue.getSize(f.getValue().value));
          buffer.append(']');
        } else if (f.getValue().value instanceof ORecord) {
          final ORecord record = (ORecord) f.getValue().value;

          if (record.getIdentity().isValid())
            record.getIdentity().toString(buffer);
          else if (record instanceof ODocument)
            buffer.append(((ODocument) record).toString(inspected));
          else
            buffer.append(record.toString());
        } else
          buffer.append(f.getValue().value);

        if (first)
          first = false;
      }
      if (!first)
        buffer.append('}');

      if (recordId != null && recordId.isValid()) {
        buffer.append(" v");
        buffer.append(recordVersion);
      }

      return buffer.toString();
    } finally {
      dirty = saveDirtyStatus;
      contentChanged = oldUpdateContent;
    }
  }

  protected ODocument mergeMap(final Map<String, ODocumentEntry> iOther, final boolean iUpdateOnlyMode,
      boolean iMergeSingleItemsOfMultiValueFields) {
    checkForLoading();
    checkForFields();

    source = null;

    for (String f : iOther.keySet()) {
      ODocumentEntry docEntry = iOther.get(f);
      if (!docEntry.exist()) {
        continue;
      }
      final Object otherValue = docEntry.value;

      ODocumentEntry curValue = fields.get(f);

      if (curValue != null && curValue.exist()) {
        final Object value = curValue.value;
        if (iMergeSingleItemsOfMultiValueFields) {
          if (value instanceof Map<?, ?>) {
            final Map<String, Object> map = (Map<String, Object>) value;
            final Map<String, Object> otherMap = (Map<String, Object>) otherValue;

            for (Entry<String, Object> entry : otherMap.entrySet()) {
              map.put(entry.getKey(), entry.getValue());
            }
            continue;
          } else if (OMultiValue.isMultiValue(value) && !(value instanceof ORidBag)) {
            for (Object item : OMultiValue.getMultiValueIterable(otherValue)) {
              if (!OMultiValue.contains(value, item))
                OMultiValue.add(value, item);
            }

            // JUMP RAW REPLACE
            continue;
          }
        }
        boolean bagsMerged = false;
        if (value instanceof ORidBag && otherValue instanceof ORidBag)
          bagsMerged = ((ORidBag) value).tryMerge((ORidBag) otherValue, iMergeSingleItemsOfMultiValueFields);

        if (!bagsMerged && (value != null && !value.equals(otherValue)) || (value == null && otherValue != null)) {
          setProperty(f, otherValue);
        }
      } else {
        setProperty(f, otherValue);
      }
    }

    if (!iUpdateOnlyMode) {
      // REMOVE PROPERTIES NOT FOUND IN OTHER DOC
      for (String f : fieldNames())
        if (!iOther.containsKey(f) || !iOther.get(f).exist())
          removeField(f);
    }

    return this;
  }

  @Override
  protected ORecordAbstract fill(final ORID iRid, final int iVersion, final byte[] iBuffer, final boolean iDirty) {
    schema = null;
    fetchSchemaIfCan();
    return super.fill(iRid, iVersion, iBuffer, iDirty);
  }

  @Override
  protected ORecordAbstract fill(final ORID iRid, final int iVersion, final byte[] iBuffer, final boolean iDirty,
      ODatabaseDocumentInternal db) {
    schema = null;
    fetchSchemaIfCan(db);
    return super.fill(iRid, iVersion, iBuffer, iDirty, db);
  }

  @Override
  protected void clearSource() {
    super.clearSource();
    schema = null;
  }

  protected OGlobalProperty getGlobalPropertyById(int id) {
    if (schema == null) {
      OMetadataInternal metadata = getDatabase().getMetadata();
      schema = metadata.getImmutableSchemaSnapshot();
    }
    OGlobalProperty prop = schema.getGlobalPropertyById(id);
    if (prop == null) {
      ODatabaseDocument db = getDatabase();
      if (db == null || db.isClosed())
        throw new ODatabaseException(
            "Cannot unmarshall the document because no database is active, use detach for use the document outside the database session scope");
      OMetadataInternal metadata = (OMetadataInternal) db.getMetadata();
      if (metadata.getImmutableSchemaSnapshot() != null)
        metadata.clearThreadLocalSchemaSnapshot();
      metadata.reload();
      metadata.makeThreadLocalSchemaSnapshot();
      schema = metadata.getImmutableSchemaSnapshot();
      prop = schema.getGlobalPropertyById(id);
    }
    return prop;
  }

  protected void fillClassIfNeed(final String iClassName) {
    if (this.className == null) {
      immutableClazz = null;
      immutableSchemaVersion = -1;
      className = iClassName;
    }

  }

  protected OImmutableClass getImmutableSchemaClass() {
    return getImmutableSchemaClass(null);
  }

  protected OImmutableClass getImmutableSchemaClass(ODatabaseDocumentInternal database) {
    if (immutableClazz == null) {
      if (className == null)
        fetchClassName();
      if (className != null) {
        if (database == null) {
          database = getDatabaseIfDefined();
        }

        if (database != null && !database.isClosed()) {
          final OSchema immutableSchema = database.getMetadata().getImmutableSchemaSnapshot();
          if (immutableSchema == null)
            return null;
          immutableSchemaVersion = immutableSchema.getVersion();
          immutableClazz = (OImmutableClass) immutableSchema.getClass(className);
        }
      }
    }

    return immutableClazz;
  }

  protected void rawField(final String iFieldName, final Object iFieldValue, final OType iFieldType) {
    if (fields == null)
      fields = ordered ? new LinkedHashMap<>() : new HashMap<>();

    ODocumentEntry entry = getOrCreate(iFieldName);
    removeCollectionChangeListener(entry, entry.value);
    entry.value = iFieldValue;
    entry.type = iFieldType;
    addCollectionChangeListener(entry);
    if (iFieldValue instanceof OIdentifiable && !((OIdentifiable) iFieldValue).getIdentity().isPersistent())
      track((OIdentifiable) iFieldValue);
  }

  protected ODocumentEntry getOrCreate(String key) {
    ODocumentEntry entry = fields.get(key);
    if (entry == null) {
      entry = new ODocumentEntry();
      fieldSize++;
      fields.put(key, entry);
    }
    return entry;
  }

  protected boolean rawContainsField(final String iFiledName) {
    return fields != null && fields.containsKey(iFiledName);
  }

  protected void autoConvertValues() {
    OClass clazz = getImmutableSchemaClass();
    if (clazz != null) {
      for (OProperty prop : clazz.properties()) {
        OType type = prop.getType();
        OType linkedType = prop.getLinkedType();
        OClass linkedClass = prop.getLinkedClass();
        if (type == OType.EMBEDDED && linkedClass != null) {
          convertToEmbeddedType(prop);
          continue;
        }
        final ODocumentEntry entry = fields.get(prop.getName());
        if (entry == null) {
          continue;
        }
        if (!entry.created && !entry.changed) {
          continue;
        }
        Object value = entry.value;
        if (value == null) {
          continue;
        }
        try {
          if (type == OType.LINKMAP) {
            if (entry.value instanceof Map) {
              Map<String, Object> map = (Map) entry.value;
              Map newMap = new ORecordLazyMap(this);
              boolean changed = false;
              for (Entry<String, Object> stringObjectEntry : map.entrySet()) {
                Object val = stringObjectEntry.getValue();
                if (OMultiValue.isMultiValue(val) && OMultiValue.getSize(val) == 1) {
                  val = OMultiValue.getFirstValue(val);
                  if (val instanceof OResult) {
                    val = ((OResult) val).getIdentity().orElse(null);
                  }
                  changed = true;
                }
                newMap.put(stringObjectEntry.getKey(), val);
              }
              if (changed) {
                entry.value = newMap;
              }
            }
          }

          if (linkedType == null)
            continue;

          if (type == OType.EMBEDDEDLIST) {
            OTrackedList<Object> list = new OTrackedList<>(this);
            Collection<Object> values = (Collection<Object>) value;
            for (Object object : values) {
              list.add(OType.convert(object, linkedType.getDefaultJavaType()));
            }
            entry.value = list;
            replaceListenerOnAutoconvert(entry, value);
          } else if (type == OType.EMBEDDEDMAP) {
            Map<Object, Object> map = new OTrackedMap<>(this);
            Map<Object, Object> values = (Map<Object, Object>) value;
            for (Entry<Object, Object> object : values.entrySet()) {
              map.put(object.getKey(), OType.convert(object.getValue(), linkedType.getDefaultJavaType()));
            }
            entry.value = map;
            replaceListenerOnAutoconvert(entry, value);
          } else if (type == OType.EMBEDDEDSET) {
            Set<Object> set = new OTrackedSet<>(this);
            Collection<Object> values = (Collection<Object>) value;
            for (Object object : values) {
              set.add(OType.convert(object, linkedType.getDefaultJavaType()));
            }
            entry.value = set;
            replaceListenerOnAutoconvert(entry, value);
          }
        } catch (Exception e) {
          throw OException
              .wrapException(new OValidationException("impossible to convert value of field \"" + prop.getName() + "\""), e);
        }
      }
    }
  }

  private void convertToEmbeddedType(OProperty prop) {
    final ODocumentEntry entry = fields.get(prop.getName());
    OClass linkedClass = prop.getLinkedClass();
    if (entry == null || linkedClass == null) {
      return;
    }
    if (!entry.created && !entry.changed) {
      return;
    }
    Object value = entry.value;
    if (value == null) {
      return;
    }
    try {
      if (value instanceof ODocument) {
        OClass docClass = ((ODocument) value).getSchemaClass();
        if (docClass == null) {
          ((ODocument) value).setClass(linkedClass);
        } else if (!docClass.isSubClassOf(linkedClass)) {
          throw new OValidationException(
              "impossible to convert value of field \"" + prop.getName() + "\", incompatible with " + linkedClass);
        }
      } else if (value instanceof Map) {
        removeCollectionChangeListener(entry, value);
        ODocument newValue = new ODocument(linkedClass);
        newValue.fromMap((Map) value);
        entry.value = newValue;
        newValue.addOwner(this);
      } else {
        throw new OValidationException("impossible to convert value of field \"" + prop.getName() + "\"");
      }

    } catch (Exception e) {
      throw OException
          .wrapException(new OValidationException("impossible to convert value of field \"" + prop.getName() + "\""), e);
    }
  }

  private void replaceListenerOnAutoconvert(final ODocumentEntry entry, Object oldValue) {
    if (entry.changeListener != null) {
      // A listener was there, remove on the old value add it to the new value.
      final OTrackedMultiValue<Object, Object> oldMultiValue = (OTrackedMultiValue<Object, Object>) oldValue;
      oldMultiValue.removeRecordChangeListener(entry.changeListener);
      ((OTrackedMultiValue<Object, Object>) entry.value).addChangeListener(entry.changeListener);
    } else {
      // no listener was there add it only to the new value
      final OSimpleMultiValueChangeListener<Object, Object> listener = new OSimpleMultiValueChangeListener<>(this, entry);
      ((OTrackedMultiValue<Object, Object>) entry.value).addChangeListener(listener);
      entry.changeListener = listener;
    }
  }

  protected byte[] toStream(final boolean iOnlyDelta) {
    STATUS prev = status;
    status = STATUS.MARSHALLING;
    try {
      if (source == null)
        source = recordFormat.toStream(this, iOnlyDelta);
    } finally {
      status = prev;
    }

    return source;
  }

  /**
   * Internal.
   */
  @Override
  protected byte getRecordType() {
    return RECORD_TYPE;
  }

  /**
   * Internal.
   */
  protected void addOwner(final ORecordElement iOwner) {
    if (iOwner == null)
      return;
    if (owners == null) {
      owners = new ArrayList<>();
      if (dirtyManager != null && this.getIdentity().isNew())
        dirtyManager.removeNew(this);
    }

    boolean found = false;
    Iterator<WeakReference<ORecordElement>> ref = owners.iterator();
    while (ref.hasNext()) {
      final ORecordElement e = ref.next().get();
      if (e == iOwner) {
        found = true;
        break;
      } else if (e == null)
        ref.remove();
    }

    if (!found)
      this.owners.add(new WeakReference<>(iOwner));

  }

  protected void removeOwner(final ORecordElement iRecordElement) {
    if (owners != null) {
      // PROPAGATES TO THE OWNER
      ORecordElement e;
      for (int i = 0; i < owners.size(); ++i) {
        e = owners.get(i).get();
        if (e == iRecordElement) {
          owners.remove(i);
          break;
        }
      }
    }
  }

  protected void convertAllMultiValuesToTrackedVersions() {
    if (fields == null)
      return;
    for (Map.Entry<String, ODocumentEntry> fieldEntry : fields.entrySet()) {
      final Object fieldValue = fieldEntry.getValue().value;
      if (fieldValue instanceof ORidBag) {
        ((ORidBag) fieldValue).checkAndConvert();
      }
      if (!(fieldValue instanceof Collection<?>) && !(fieldValue instanceof Map<?, ?>) && !(fieldValue instanceof ODocument))
        continue;
      if (addCollectionChangeListener(fieldEntry.getValue())) {
        if (fieldEntry.getValue().timeLine != null && !fieldEntry.getValue().timeLine.getMultiValueChangeEvents().isEmpty()) {
          checkTimelineTrackable(fieldEntry.getValue().timeLine, (OTrackedMultiValue) fieldEntry.getValue().value);
        }
        continue;
      }

      if (fieldValue instanceof ODocument && ((ODocument) fieldValue).isEmbedded()) {
        ((ODocument) fieldValue).convertAllMultiValuesToTrackedVersions();
        continue;
      }

      OType fieldType = fieldEntry.getValue().type;
      if (fieldType == null) {
        OClass clazz = getImmutableSchemaClass();
        if (clazz != null) {
          final OProperty prop = clazz.getProperty(fieldEntry.getKey());
          fieldType = prop != null ? prop.getType() : null;
        }
      }
      if (fieldType == null)
        fieldType = OType.getTypeByValue(fieldValue);

      Object newValue = null;
      switch (fieldType) {
      case EMBEDDEDLIST:
        if (fieldValue instanceof List<?>) {
          newValue = new OTrackedList<>(this);
          fillTrackedCollection((Collection<Object>) newValue, (Collection<Object>) fieldValue);
        }
        break;
      case EMBEDDEDSET:
        if (fieldValue instanceof Set<?>) {
          newValue = new OTrackedSet<>(this);
          fillTrackedCollection((Collection<Object>) newValue, (Collection<Object>) fieldValue);
        }
        break;
      case EMBEDDEDMAP:
        if (fieldValue instanceof Map<?, ?>) {
          newValue = new OTrackedMap<>(this);
          fillTrackedMap((Map<Object, Object>) newValue, (Map<Object, Object>) fieldValue);
        }
        break;
      case LINKLIST:
        if (fieldValue instanceof List<?>)
          newValue = new ORecordLazyList(this, (Collection<OIdentifiable>) fieldValue);
        break;
      case LINKSET:
        if (fieldValue instanceof Set<?>)
          newValue = new ORecordLazySet(this, (Collection<OIdentifiable>) fieldValue);
        break;
      case LINKMAP:
        if (fieldValue instanceof Map<?, ?>)
          newValue = new ORecordLazyMap(this, (Map<Object, OIdentifiable>) fieldValue);
        break;
      case LINKBAG:
        if (fieldValue instanceof Collection<?>) {
          ORidBag bag = new ORidBag();
          bag.setOwner(this);
          bag.addAll((Collection<OIdentifiable>) fieldValue);
          newValue = bag;
        }
        break;
      default:
        break;
      }

      if (newValue != null) {
        addCollectionChangeListener(fieldEntry.getValue());
        fieldEntry.getValue().value = newValue;
        if (fieldType == OType.LINKSET || fieldType == OType.LINKLIST) {
          boolean pre = ((OAutoConvertToRecord) newValue).isAutoConvertToRecord();
          ((OAutoConvertToRecord) newValue).setAutoConvertToRecord(false);
          for (OIdentifiable rec : (Collection<OIdentifiable>) newValue) {
            if (rec instanceof ODocument)
              ((ODocument) rec).convertAllMultiValuesToTrackedVersions();
          }
          ((OAutoConvertToRecord) newValue).setAutoConvertToRecord(pre);
        } else if (fieldType == OType.LINKMAP) {
          boolean pre = ((OAutoConvertToRecord) newValue).isAutoConvertToRecord();
          ((OAutoConvertToRecord) newValue).setAutoConvertToRecord(false);
          for (OIdentifiable rec : (Collection<OIdentifiable>) ((Map<?, ?>) newValue).values()) {
            if (rec instanceof ODocument)
              ((ODocument) rec).convertAllMultiValuesToTrackedVersions();
          }
          ((OAutoConvertToRecord) newValue).setAutoConvertToRecord(pre);
        }
      }
    }

  }

  private void checkTimelineTrackable(OMultiValueChangeTimeLine<Object, Object> timeLine, OTrackedMultiValue origin) {
    List<OMultiValueChangeEvent<Object, Object>> events = timeLine.getMultiValueChangeEvents();
    for (OMultiValueChangeEvent<Object, Object> event : events) {
      Object value = event.getValue();
      if (event.getChangeType() == OMultiValueChangeEvent.OChangeType.ADD && !(value instanceof OTrackedMultiValue)) {
        if (value instanceof Collection) {
          Collection<Object> newCollection = value instanceof List ? new OTrackedList<>(this) : new OTrackedSet<>(this);
          fillTrackedCollection(newCollection, (Collection<Object>) value);
          origin.replace(event, newCollection);
        } else if (value instanceof Map) {
          Map<Object, Object> newMap = new OTrackedMap<>(this);
          fillTrackedMap(newMap, (Map<Object, Object>) value);
          origin.replace(event, newMap);
        }
      } else if (event.getChangeType() == OMultiValueChangeEvent.OChangeType.NESTED) {
        OMultiValueChangeTimeLine nestedTimeline = ((ONestedMultiValueChangeEvent) event).getTimeLine();
        if (nestedTimeline != null)
          checkTimelineTrackable(nestedTimeline, (OTrackedMultiValue) value);
      }
    }
  }

  private void fillTrackedCollection(Collection<Object> dest, Collection<Object> source) {
    for (Object cur : source) {
      if (cur instanceof ODocument) {
        ((ODocument) cur).convertAllMultiValuesToTrackedVersions();
        ((ODocument) cur).clearTrackData();
      } else if (cur instanceof List) {
        List newList = new OTrackedList<>(this);
        fillTrackedCollection((Collection) newList, (Collection<Object>) cur);
        cur = newList;
      } else if (cur instanceof Set) {
        Set<Object> newSet = new OTrackedSet<>(this);
        fillTrackedCollection(newSet, (Collection<Object>) cur);
        cur = newSet;
      } else if (cur instanceof Map) {
        Map<Object, Object> newMap = new OTrackedMap<>(this);
        fillTrackedMap(newMap, (Map<Object, Object>) cur);
        cur = newMap;
      }
      dest.add(cur);
    }
  }

  private void fillTrackedMap(Map<Object, Object> dest, Map<Object, Object> source) {
    for (Entry<Object, Object> cur : source.entrySet()) {
      Object value = cur.getValue();
      if (value instanceof ODocument) {
        ((ODocument) value).convertAllMultiValuesToTrackedVersions();
        ((ODocument) value).clearTrackData();
      } else if (cur.getValue() instanceof List) {
        List<Object> newList = new OTrackedList<>(this);
        fillTrackedCollection(newList, (Collection<Object>) value);
        value = newList;
      } else if (value instanceof Set) {
        Set<Object> newSet = new OTrackedSet<>(this);
        fillTrackedCollection(newSet, (Collection<Object>) value);
        value = newSet;
      } else if (value instanceof Map) {
        Map<Object, Object> newMap = new OTrackedMap<>(this);
        fillTrackedMap(newMap, (Map<Object, Object>) value);
        value = newMap;
      }
      dest.put(cur.getKey(), value);
    }
  }

  protected void internalReset() {
    removeAllCollectionChangeListeners();

    if (fields != null)
      fields.clear();
    fieldSize = 0;

  }

  protected boolean checkForFields(final String... iFields) {
    if (fields == null)
      fields = ordered ? new LinkedHashMap<>() : new HashMap<>();

    if (status == ORecordElement.STATUS.LOADED && source != null)
      // POPULATE FIELDS LAZY
      return deserializeFields(iFields);

    return true;
  }

  protected Object accessProperty(final String property) {
    if (checkForFields(property)) {
      if (propertyAccess == null || propertyAccess.isReadable(property)) {
        ODocumentEntry entry = fields.get(property);
        return entry != null ? entry.value : null;
      } else {
        return null;
      }
    } else {
      return null;
    }
  }

  /**
   * Internal.
   *
   * @param db
   */
  @Override
  protected void setup(ODatabaseDocumentInternal db) {
    super.setup(db);

    if (db != null)
      recordFormat = db.getSerializer();

    if (recordFormat == null)
      // GET THE DEFAULT ONE
      recordFormat = ODatabaseDocumentTx.getDefaultSerializer();
  }

  protected String checkFieldName(final String iFieldName) {
    final Character c = OSchemaShared.checkFieldNameIfValid(iFieldName);
    if (c != null)
      throw new IllegalArgumentException("Invalid field name '" + iFieldName + "'. Character '" + c + "' is invalid");

    return iFieldName;
  }

  protected void setClass(final OClass iClass) {
    if (iClass != null && iClass.isAbstract())
      throw new OSchemaException("Cannot create a document of the abstract class '" + iClass + "'");

    if (iClass == null)
      className = null;
    else
      className = iClass.getName();

    immutableClazz = null;
    immutableSchemaVersion = -1;
    if (iClass != null)
      convertFieldsToClass(iClass);
  }

  protected Set<Entry<String, ODocumentEntry>> getRawEntries() {
    checkForFields();
    return fields == null ? new HashSet<>() : fields.entrySet();
  }

  private void fetchSchemaIfCan() {
    if (schema == null) {
      ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
      if (db != null && !db.isClosed()) {
        OMetadataInternal metadata = db.getMetadata();
        schema = metadata.getImmutableSchemaSnapshot();
      }
    }
  }

  private void fetchSchemaIfCan(ODatabaseDocumentInternal db) {
    if (schema == null) {
      if (db != null && !db.isClosed()) {
        OMetadataInternal metadata = db.getMetadata();
        schema = metadata.getImmutableSchemaSnapshot();
      }
    }
  }

  private void fetchClassName() {
    final ODatabaseDocumentInternal database = getDatabaseIfDefinedInternal();
    if (database != null && database.getStorageVersions() != null) {
      if (recordId.getClusterId() < 0) {
        checkForLoading();
        checkForFields(ODocumentHelper.ATTRIBUTE_CLASS);
      } else {
        final OSchema schema = database.getMetadata().getImmutableSchemaSnapshot();
        if (schema != null) {
          OClass clazz = schema.getClassByClusterId(recordId.getClusterId());
          if (clazz != null)
            className = clazz.getName();
        }
      }
    } else {
      // CLASS NOT FOUND: CHECK IF NEED LOADING AND UNMARSHALLING
      checkForLoading();
      checkForFields(ODocumentHelper.ATTRIBUTE_CLASS);
    }
  }

  protected void autoConvertFieldsToClass(final ODatabaseDocumentInternal database) {
    if (className != null) {
      OClass klazz = database.getMetadata().getImmutableSchemaSnapshot().getClass(className);
      if (klazz != null)
        convertFieldsToClass(klazz);
    }
  }

  /**
   * Checks and convert the field of the document matching the types specified by the class.
   **/
  private void convertFieldsToClass(final OClass clazz) {
    for (OProperty prop : clazz.properties()) {
      ODocumentEntry entry = fields != null ? fields.get(prop.getName()) : null;
      if (entry != null && entry.exist()) {
        if (entry.type == null || entry.type != prop.getType()) {
          boolean preChanged = entry.changed;
          boolean preCreated = entry.created;
          ;
          field(prop.getName(), entry.value, prop.getType());
          if (recordId.isNew()) {
            entry.changed = preChanged;
            entry.created = preCreated;
          }
        }
      } else {
        String defValue = prop.getDefaultValue();
        if (defValue != null && /*defValue.length() > 0 && */!containsField(prop.getName())) {
          Object curFieldValue = OSQLHelper.parseDefaultValue(this, defValue);
          Object fieldValue = ODocumentHelper.convertField(this, prop.getName(), prop.getType(), null, curFieldValue);
          rawField(prop.getName(), fieldValue, prop.getType());
        }
      }
    }
  }

  private OType deriveFieldType(String iFieldName, ODocumentEntry entry, OType[] iFieldType) {
    OType fieldType;

    if (iFieldType != null && iFieldType.length == 1) {
      entry.type = iFieldType[0];
      fieldType = iFieldType[0];
    } else
      fieldType = null;

    OClass clazz = getImmutableSchemaClass();
    if (clazz != null) {
      // SCHEMA-FULL?
      final OProperty prop = clazz.getProperty(iFieldName);
      if (prop != null) {
        entry.property = prop;
        fieldType = prop.getType();
        if (fieldType != OType.ANY)
          entry.type = fieldType;
      }
    }
    return fieldType;
  }

  private boolean addCollectionChangeListener(final ODocumentEntry entry) {
    if (!(entry.value instanceof OTrackedMultiValue))
      return false;
    if (entry.changeListener == null) {
      final OSimpleMultiValueChangeListener<Object, Object> listener = new OSimpleMultiValueChangeListener<>(this, entry);
      ((OTrackedMultiValue<Object, Object>) entry.value).addChangeListener(listener);
      entry.changeListener = listener;
    }
    return true;
  }

  private void removeAllCollectionChangeListeners() {
    if (fields == null)
      return;

    for (final Map.Entry<String, ODocumentEntry> field : fields.entrySet()) {
      removeCollectionChangeListener(field.getValue(), field.getValue().value);
    }
  }

  private void addAllMultiValueChangeListeners() {
    if (fields == null)
      return;

    for (final Map.Entry<String, ODocumentEntry> field : fields.entrySet()) {
      addCollectionChangeListener(field.getValue());
    }
  }

  private void removeCollectionChangeListener(ODocumentEntry entry, Object fieldValue) {
    if (entry != null && entry.changeListener != null) {
      final OMultiValueChangeListener<Object, Object> changeListener = entry.changeListener;
      entry.changeListener = null;
      entry.timeLine = null;
      if (!(fieldValue instanceof OTrackedMultiValue))
        return;

      final OTrackedMultiValue<Object, Object> multiValue = (OTrackedMultiValue<Object, Object>) fieldValue;
      multiValue.removeRecordChangeListener(changeListener);
    }
  }

  protected void checkClass(ODatabaseDocumentInternal database) {
    if (className == null)
      fetchClassName();

    final OSchema immutableSchema = database.getMetadata().getImmutableSchemaSnapshot();
    if (immutableSchema == null)
      return;

    if (immutableClazz == null) {
      immutableSchemaVersion = immutableSchema.getVersion();
      immutableClazz = (OImmutableClass) immutableSchema.getClass(className);
    } else {
      if (immutableSchemaVersion < immutableSchema.getVersion()) {
        immutableSchemaVersion = immutableSchema.getVersion();
        immutableClazz = (OImmutableClass) immutableSchema.getClass(className);
      }
    }

  }

  @Override
  protected void track(OIdentifiable id) {
    if (isTrackingChanges() && id.getIdentity().getClusterId() != -2)
      super.track(id);
  }

  @Override
  protected void unTrack(OIdentifiable id) {
    if (isTrackingChanges() && id.getIdentity().getClusterId() != -2)
      super.unTrack(id);
  }

  private UpdateTypeValueType getUpdateForListWithPreviousValue(Object currentValue, Object previousValue, ODocumentEntry parent,
      List<Object> ownersTrace, OType currentValueLinkedType, OType previousValueType, OType previousValueLinkedType) {
    UpdateTypeValueType retVal = null;
    if (previousValueType.isList()) {
      retVal = new UpdateTypeValueType();
      retVal.setUpdateType(UpdateDeltaValueType.LIST_UPDATE);
      List deltaList = new ArrayList();
      retVal.setValue(deltaList);
      retVal.setValueType(OType.EMBEDDEDLIST);
      //now find a diff
      List currentList = (List) currentValue;
      List previousList = (List) previousValue;
      int i = 0;
      for (i = 0; i < currentList.size(); i++) {
        Object currentElement = currentList.get(i);
        if (i > previousList.size() - 1) {
          //these elements doesn't have pair in original list
          ODocumentDelta deltaElement = new ODocumentDelta();
          deltaElement.field("t", new ValueType(UpdateDeltaValueType.LIST_ELEMENT_ADD.getOrd(), OType.BYTE));
          deltaElement.field("v", new ValueType(currentElement,
              currentValueLinkedType != null ? currentValueLinkedType : ODeltaDocumentFieldType.getTypeByValue(currentElement)));
          deltaList.add(deltaElement);
          continue;
        }
        //these elements have pairs in original list, so we need to calculate deltas
        Object originalElement = previousList.get(i);
        //check if can we go just with value change
        OType currentElementType =
            currentValueLinkedType != null ? currentValueLinkedType : OType.getTypeByClass(currentElement.getClass());
        OType previousElementType =
            previousValueLinkedType != null ? previousValueLinkedType : OType.getTypeByClass(originalElement.getClass());
        if ((currentElementType != previousElementType && currentElementType.isList() != previousElementType.isList()) || (
            !(currentElement instanceof ODocument) && !(currentElement instanceof List))) {
          if (!Objects.equals(currentElement, originalElement)) {
            ODocumentDelta deltaElement = new ODocumentDelta();
            deltaElement.field("t", new ValueType(UpdateDeltaValueType.LIST_ELEMENT_CHANGE.getOrd(), OType.BYTE));
            deltaElement.field("v", new ValueType(currentElement, currentElementType));
            deltaElement.field("i", new ValueType(i, OType.INTEGER));
            deltaList.add(deltaElement);
          }
        } else {
          if (!Objects.equals(currentElement, originalElement)) {
            //now we want to go in depth to get delta value
            ODocumentDelta deltaElement = new ODocumentDelta();
            deltaElement.field("t", new ValueType(UpdateDeltaValueType.LIST_ELEMENT_UPDATE.getOrd(), OType.BYTE));

            //we cann't determine linked type directly from document because this can be double nested collection
            UpdateTypeValueType delta = getUpdateDeltaValue(currentElement, originalElement, true, parent, ownersTrace,
                currentElementType, null, previousElementType, null);
            ODocumentDelta doc = new ODocumentDelta();
            doc.field("t", new ValueType(delta.getUpdateType().getOrd(), OType.BYTE));
            doc.field("v", new ValueType(delta.getValue(), delta.getValueType()));
            deltaElement.field("v", new ValueType(doc, ODeltaDocumentFieldType.DELTA_RECORD));
            deltaElement.field("i", new ValueType(i, OType.INTEGER));
            deltaList.add(deltaElement);
          }
          //this means that elements are equal so nothig to do
        }
      }
      //these element are contained only in original list , so they should be removed
      for (int j = i; j < previousList.size(); j++) {
        ODocumentDelta deltaElement = new ODocumentDelta();
        deltaElement.field("t", new ValueType(UpdateDeltaValueType.LIST_ELEMENT_REMOVE.getOrd(), OType.BYTE));
        deltaElement.field("i", new ValueType(j, OType.INTEGER));
        deltaList.add(deltaElement);
      }
    }
    return retVal;
  }

  private UpdateTypeValueType getUpdateForRidbagWithPreviousValue(ORidBag currentValue, ORidBag previousValue, boolean changed) {
    UpdateTypeValueType retVal = null;
    if (currentValue != null && previousValue != null && currentValue.isEmbedded() && previousValue.isEmbedded() && changed) {
      retVal = new UpdateTypeValueType();
      retVal.setUpdateType(UpdateDeltaValueType.RIDBAG_UPDATE);
      List deltaList = new ArrayList();
      retVal.setValue(deltaList);
      retVal.setValueType(OType.EMBEDDEDLIST);
      //now find a diff
      int i = 0;
      Iterator<OIdentifiable> currentRidbagIter = currentValue.rawIterator();
      Iterator<OIdentifiable> previousRidbagIter = previousValue.rawIterator();
      while (currentRidbagIter.hasNext()) {
        OIdentifiable currentElement = currentRidbagIter.next();
        if (previousRidbagIter.hasNext() == false) {
          //these elements doesn't have pair in original list
          ODocumentDelta deltaElement = new ODocumentDelta();
          deltaElement.field("t", new ValueType(UpdateDeltaValueType.LIST_ELEMENT_ADD.getOrd(), OType.BYTE));
          deltaElement.field("v", new ValueType(currentElement, OType.LINK));
          deltaList.add(deltaElement);
          continue;
        }
        //these elements have pairs in original list, so we need to calculate deltas
        OIdentifiable originalElement = previousRidbagIter.next();
        //check if can we go just with value change
        if (!Objects.equals(currentElement, originalElement)) {
          ODocumentDelta deltaElement = new ODocumentDelta();
          deltaElement.field("t", new ValueType(UpdateDeltaValueType.LIST_ELEMENT_CHANGE.getOrd(), OType.BYTE));
          deltaElement.field("v", new ValueType(currentElement, OType.LINK));
          deltaElement.field("i", new ValueType(i, OType.INTEGER));
          deltaList.add(deltaElement);
        }
        //this means that elements are equal so nothig to do
        i++;
      }
      //these element are contained only in original list , so they should be removed
      while (previousRidbagIter.hasNext()) {
        OIdentifiable element = previousRidbagIter.next();
        ODocumentDelta deltaElement = new ODocumentDelta();
        deltaElement.field("t", new ValueType(UpdateDeltaValueType.LIST_ELEMENT_REMOVE.getOrd(), OType.BYTE));
        deltaElement.field("v", new ValueType(element, OType.LINK));
        deltaList.add(deltaElement);
        ++i;
      }
    }
    return retVal;
  }

  private UpdateTypeValueType getUpdateForRidbagWithoutPreviousValue(ORidBag currentValue, ODocumentEntry parent) {
    UpdateTypeValueType retVal = null;
    if (currentValue != null && currentValue.isEmbedded() && ODocumentHelper.isChangedRidbag(currentValue, parent)) {
      retVal = new UpdateTypeValueType();
      retVal.setUpdateType(UpdateDeltaValueType.RIDBAG_UPDATE);
      List deltaList = new ArrayList();
      retVal.setValue(deltaList);
      retVal.setValueType(OType.EMBEDDEDLIST);
      //now create diff from multi value change events
      if (parent.timeLine != null) {
        List<OMultiValueChangeEvent<Object, Object>> timeline = parent.timeLine.getMultiValueChangeEvents();
        for (OMultiValueChangeEvent<Object, Object> event : timeline) {
          if (event.getChangeType() == OMultiValueChangeEvent.OChangeType.ADD) {
            ODocumentDelta deltaElement = new ODocumentDelta();
            deltaElement.field("t", new ValueType(UpdateDeltaValueType.LIST_ELEMENT_ADD.getOrd(), OType.BYTE));
            deltaElement.field("v", new ValueType(event.getKey(), OType.LINK));
            deltaList.add(deltaElement);
          } else if (event.getChangeType() == OMultiValueChangeEvent.OChangeType.REMOVE) {
            ODocumentDelta deltaElement = new ODocumentDelta();
            deltaElement.field("t", new ValueType(UpdateDeltaValueType.LIST_ELEMENT_REMOVE.getOrd(), OType.BYTE));
            deltaElement.field("v", new ValueType(event.getKey(), OType.LINK));
            deltaList.add(deltaElement);
          }
        }
      }
    }
    return retVal;
  }

  private UpdateTypeValueType getUpdateForListWithoutPreviousValue(Object currentValue, ODocumentEntry parent,
      List<Object> ownersTrace, OType currentValueLinkedType) {
    List currentList = (List) currentValue;
    UpdateTypeValueType retVal = new UpdateTypeValueType();
    retVal.setUpdateType(UpdateDeltaValueType.LIST_UPDATE);
    List<ODocumentDelta> deltaList = new ArrayList<>();
    retVal.setValue(deltaList);
    retVal.setValueType(OType.EMBEDDEDLIST);
    for (int i = 0; i < currentList.size(); i++) {
      Object currentElement = currentList.get(i);
      OType currentElementType = currentValueLinkedType != null ? currentValueLinkedType : OType.getTypeByValue(currentElement);
      if (currentElement instanceof ODocument) {
        ODocument currentElementDoc = (ODocument) currentElement;
        if (currentElementDoc.isChangedInDepth()) {
          ODocumentDelta listElementDelta = new ODocumentDelta();
          listElementDelta.field("i", new ValueType(i, OType.INTEGER));
          listElementDelta.field("t", new ValueType(UpdateDeltaValueType.LIST_ELEMENT_UPDATE.getOrd(), OType.BYTE));
          ODocumentDelta deltaUpdate = currentElementDoc.getDeltaFromOriginalForUpdate();
          listElementDelta.field("v", new ValueType(deltaUpdate, ODeltaDocumentFieldType.DELTA_RECORD));
          deltaList.add(listElementDelta);
        }
      } else if (currentElement instanceof List) {
        List currentElementAsList = (List) currentElement;
        if (ODocumentHelper.isChangedCollection(currentElementAsList, parent, new ArrayList<>(ownersTrace), 1)) {
          ODocumentDelta listElementDelta = new ODocumentDelta();
          listElementDelta.field("i", new ValueType(i, OType.INTEGER));
          listElementDelta.field("t", new ValueType(UpdateDeltaValueType.LIST_ELEMENT_UPDATE.getOrd(), OType.BYTE));
          UpdateTypeValueType deltaUpdate = getUpdateDeltaValue(currentElementAsList, null, false, parent, ownersTrace,
              currentElementType, null, null, null);
          ODocumentDelta deltaValue = new ODocumentDelta();
          deltaValue.field("t", new ValueType(deltaUpdate.getUpdateType().getOrd(), OType.BYTE));
          deltaValue.field("v", new ValueType(deltaUpdate.getValue(), deltaUpdate.getValueType()));
          listElementDelta.field("v", new ValueType(deltaValue, ODeltaDocumentFieldType.DELTA_RECORD));
          deltaList.add(listElementDelta);
        }
      } else {
        //element
        OMultiValueChangeEvent.OChangeType changeType = ODocumentHelper
            .isNestedValueChanged(parent, currentElement, ownersTrace, 1, i);
        if (changeType != null) {
          ODocumentDelta deltaElement = new ODocumentDelta();
          deltaElement.field("v", new ValueType(currentElement, currentElementType));
          deltaElement.field("i", new ValueType(i, OType.INTEGER));
          switch (changeType) {
          case ADD:
            deltaElement.field("t", new ValueType(UpdateDeltaValueType.LIST_ELEMENT_ADD.getOrd(), OType.BYTE));
            break;
          case UPDATE:
          case NESTED:
            deltaElement.field("t", new ValueType(UpdateDeltaValueType.LIST_ELEMENT_CHANGE.getOrd(), OType.BYTE));
            break;
          }
          deltaList.add(deltaElement);
        }
      }
    }
    List<Integer> indexesToRemove = getRemovedIndexes(parent, currentList, ownersTrace, 1);
    int counter = 0;
    for (Integer index : indexesToRemove) {
      ODocumentDelta removeDleta = new ODocumentDelta();
      removeDleta.field("t", new ValueType(UpdateDeltaValueType.LIST_ELEMENT_REMOVE.getOrd(), OType.BYTE));
      removeDleta.field("i", new ValueType(index + counter++, OType.INTEGER));
      deltaList.add(removeDleta);
    }
    return retVal;
  }

  private UpdateTypeValueType getUpdateForNestedDocument(Object currentValue) {
    UpdateTypeValueType retVal = new UpdateTypeValueType();
    ODocument docVal;
    if (currentValue instanceof ODocumentSerializable) {
      docVal = ((ODocumentSerializable) currentValue).toDocument();
    } else {
      docVal = (ODocument) currentValue;
    }
    retVal.setValue(docVal.getDeltaFromOriginalForUpdate());
    retVal.setUpdateType(UpdateDeltaValueType.UPDATE);
    retVal.setValueType(ODeltaDocumentFieldType.DELTA_RECORD);
    return retVal;
  }

  //this method is callen only for changed fields or directly and undirectly changed lists
  private UpdateTypeValueType getUpdateDeltaValue(Object currentValue, Object previousValue, boolean changed, ODocumentEntry parent,
      List<Object> ownersTrace, OType currentValueType, OType currentValueLinkedType, OType previousValueType,
      OType previousValueLinkedType) {
    if (currentValueLinkedType == OType.ANY) {
      currentValueLinkedType = null;
    }
    if (previousValueLinkedType == OType.ANY) {
      previousValueLinkedType = null;
    }
    //TODO review this clause once again
    ownersTrace.add(currentValue);
    if (changed || !(currentValue instanceof ODocument)) {
      //separate processing for embedded lists
      //if previous value is null we want to send whole list to reduce overhead
      if (currentValueType.isList() && previousValue != null) {
        //if previous value differs in type we want to send whole list to reduce overhead
        UpdateTypeValueType retVal = getUpdateForListWithPreviousValue(currentValue, previousValue, parent, ownersTrace,
            currentValueLinkedType, previousValueType, previousValueLinkedType);
        if (retVal != null) {
          return retVal;
        }
      }
      UpdateTypeValueType retVal = new UpdateTypeValueType();
      //this means that some list memeber document is changed
      if (currentValueType.isList() && previousValue == null) {
        return getUpdateForListWithoutPreviousValue(currentValue, parent, ownersTrace, currentValueLinkedType);
      }

      if (currentValueType == OType.LINKBAG && previousValue != null && previousValueType == OType.LINKBAG) {
        ORidBag currentRidbag = (ORidBag) currentValue;
        ORidBag previousRidbag = (ORidBag) previousValue;
        UpdateTypeValueType ridBagUpdate = getUpdateForRidbagWithPreviousValue(currentRidbag, previousRidbag, changed);
        if (ridBagUpdate != null) {
          return ridBagUpdate;
        }
      }

      if (currentValueType == OType.LINKBAG && previousValue == null) {
        ORidBag currentRidbag = (ORidBag) currentValue;
        UpdateTypeValueType ridBagUpdate = getUpdateForRidbagWithoutPreviousValue(currentRidbag, parent);
        if (ridBagUpdate != null) {
          return ridBagUpdate;
        }
      }

      //check for nested document
      if (currentValueType == OType.LINK && previousValueType != null && previousValueType == OType.LINK) {
        return getUpdateForNestedDocument(currentValue);
      }

      //in this case we want to swap values
      retVal.setValue(currentValue);
      retVal.setUpdateType(UpdateDeltaValueType.CHANGE);
      retVal.setValueType(currentValueType);
      return retVal;
    } else {
      return getUpdateForNestedDocument(currentValue);
    }
  }

  private static List<Integer> getRemovedIndexes(ONestedMultiValueChangeEvent event, List list, List<Object> ownersTrace,
      int ownersTraceOffset) {
    List<Integer> retList = new ArrayList<>();
    if (event.getTimeLine() != null) {
      List<OMultiValueChangeEvent<Object, Object>> events = event.getTimeLine().getMultiValueChangeEvents();
      if (events != null) {
        for (OMultiValueChangeEvent<Object, Object> nestedEvent : events) {
          if (ownersTraceOffset < ownersTrace.size() && nestedEvent.getKey() == ownersTrace.get(ownersTraceOffset)
              && nestedEvent instanceof ONestedMultiValueChangeEvent) {
            ONestedMultiValueChangeEvent ne = (ONestedMultiValueChangeEvent) nestedEvent;
            List<Integer> ret = getRemovedIndexes(ne, list, ownersTrace, ownersTraceOffset + 1);
            if (ret != null) {
              return ret;
            }
          } else if (ownersTraceOffset == ownersTrace.size()) {
            if (nestedEvent.getChangeType() == OMultiValueChangeEvent.OChangeType.REMOVE && nestedEvent
                .getKey() instanceof Integer) {
              retList.add((Integer) nestedEvent.getKey());
            }
          }
        }
      }
    }

    return retList;
  }

  public static List<Integer> getRemovedIndexes(ODocumentEntry entry, List list, List<Object> ownersTrace, int ownersTraceOffset) {
    List<Integer> retList = new ArrayList<>();
    if (entry.timeLine != null) {
      List<OMultiValueChangeEvent<Object, Object>> timeline = entry.timeLine.getMultiValueChangeEvents();
      if (timeline != null) {
        for (OMultiValueChangeEvent<Object, Object> event : timeline) {
          if (ownersTraceOffset < ownersTrace.size() && event.getKey() == ownersTrace.get(ownersTraceOffset)
              && event instanceof ONestedMultiValueChangeEvent) {
            ONestedMultiValueChangeEvent nestedEvent = (ONestedMultiValueChangeEvent) event;
            List<Integer> ret = getRemovedIndexes(nestedEvent, list, ownersTrace, ownersTraceOffset + 1);
            if (ret != null) {
              retList.addAll(ret);
            }
          } else if (ownersTraceOffset == ownersTrace.size()) {
            if (event.getChangeType() == OMultiValueChangeEvent.OChangeType.REMOVE && event.getKey() instanceof Integer) {
              retList.add((Integer) event.getKey());
            }
          }
        }
      }
    }

    return retList;
  }

  private Object getDeleteDeltaValue(Object currentValue, boolean exist) {
    //TODO review this clause once again
    if (!exist || (!(currentValue instanceof ODocument) && !(currentValue instanceof List))) {
      return currentValue;
    } else if (currentValue instanceof ODocument) {
      ODocument docVal;
      if (currentValue instanceof ODocumentSerializable) {
        docVal = ((ODocumentSerializable) currentValue).toDocument();
      } else {
        docVal = (ODocument) currentValue;
      }
      return docVal.getDeltaFromOriginalForDelete();
    } else if (currentValue instanceof List) {
      //want to detect all elements where in depth some field is deleted
      List currentList = (List) currentValue;
      List<ODocumentDelta> deltaList = new ArrayList<>();
      for (int i = 0; i < currentList.size(); i++) {
        Object element = currentList.get(i);
        Object innerDelta = getDeleteDeltaValue(element, true);
        if (innerDelta != null) {
          ODocumentDelta deltaDoc = new ODocumentDelta();
          deltaDoc.field("i", new ValueType(i, OType.INTEGER));
          deltaDoc.field("v", new ValueType(innerDelta, ODeltaDocumentFieldType.getTypeByValue(innerDelta)));
          deltaList.add(deltaDoc);
        }
      }
      if (!deltaList.isEmpty()) {
        return deltaList;
      }
    }
    return null;
  }

  private ODocumentDelta getDeltaFromOriginalForUpdate() {
    return getDeltaFromOriginalForUpdate(new ArrayList<>());
  }

  private ODocumentDelta getDeltaFromOriginalForUpdate(List<Object> ownersTrace) {
    ODocumentDelta updated = new ODocumentDelta();
    //get updated and new records
    for (Map.Entry<String, ODocumentEntry> fieldVal : fields.entrySet()) {
      ODocumentEntry val = fieldVal.getValue();
      if (val.isChangedTree(new ArrayList<Object>(ownersTrace))) {
        String fieldName = fieldVal.getKey();
        OType currentFieldType = val.type != null ? val.type : OType.getTypeByValue(val.value);
        OType currentFieldLinkedType = HelperClasses.getLinkedType(this, currentFieldType, fieldName);
        OType previousFieldValueType = OType.getTypeByValue(val.original);
        OType previousFieldValueLinkedType = null;
        UpdateTypeValueType deltaValue = getUpdateDeltaValue(val.value, val.original, val.isChanged(), val, ownersTrace,
            currentFieldType, currentFieldLinkedType, previousFieldValueType, previousFieldValueLinkedType);
        ODocumentDelta doc = new ODocumentDelta();
        doc.field("v", new ValueType(deltaValue.getValue(), deltaValue.getValueType()));
        doc.field("t", new ValueType(deltaValue.getUpdateType().getOrd(), OType.BYTE));
        updated.field(fieldName, new ValueType(doc, ODeltaDocumentFieldType.DELTA_RECORD));
      }
    }
    return updated;
  }

  private ODocumentDelta getDeltaFromOriginalForDelete() {
    ODocumentDelta delete = new ODocumentDelta();
    //get updated and new records
    for (Map.Entry<String, ODocumentEntry> fieldVal : fields.entrySet()) {
      ODocumentEntry val = fieldVal.getValue();
      if (val.hasNonExistingTree()) {
        String fieldName = fieldVal.getKey();
        Object deltaValue = getDeleteDeltaValue(val.value, val.exist());
        delete.field(fieldName, new ValueType(deltaValue, ODeltaDocumentFieldType.getTypeByValue(deltaValue)));
      }
    }
    if (delete.fields().isEmpty()) {
      return null;
    }
    return delete;
  }

  public ODocumentDelta getDeltaFromOriginal() {
    ODocumentDelta ret = new ODocumentDelta();
    ODocumentDelta updated = getDeltaFromOriginalForUpdate();
    ret.field("u", new ValueType(updated, ODeltaDocumentFieldType.DELTA_RECORD));

    //get deleted delta
    ODocumentDelta deleted = getDeltaFromOriginalForDelete();
    ret.field("d", new ValueType(deleted, ODeltaDocumentFieldType.DELTA_RECORD));

    ORecordId id = (ORecordId) getIdentity();
    ret.setIdentity(id);

    int version = getVersion();
    ret.setVersion(version);

    return ret;
  }

  public boolean isChangedInDepth() {
    for (Map.Entry<String, ODocumentEntry> field : fields.entrySet()) {
      if (field.getValue().isChangedTree(new ArrayList<>())) {
        return true;
      }
    }
    return false;
  }

  public boolean hasNonExistingInDepth() {
    for (Map.Entry<String, ODocumentEntry> field : fields.entrySet()) {
      if (field.getValue().hasNonExistingTree()) {
        return true;
      }
    }
    return false;
  }

}
