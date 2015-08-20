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
package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLCreateIndex;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Index that consist of several indexDefinitions like {@link OPropertyIndexDefinition}.
 */

public class OCompositeIndexDefinition extends OAbstractIndexDefinition {
  private final List<OIndexDefinition> indexDefinitions;
  private String                       className;
  private int                          multiValueDefinitionIndex = -1;
  private OCompositeCollate            collate                   = new OCompositeCollate(this);

  public OCompositeIndexDefinition() {
    indexDefinitions = new ArrayList<OIndexDefinition>(5);
  }

  /**
   * Constructor for new index creation.
   * 
   * @param iClassName
   *          - name of class which is owner of this index
   */
  public OCompositeIndexDefinition(final String iClassName, int version) {
    super();

    indexDefinitions = new ArrayList<OIndexDefinition>(5);
    className = iClassName;
  }

  /**
   * Constructor for new index creation.
   * 
   * @param iClassName
   *          - name of class which is owner of this index
   * @param iIndexes
   *          List of indexDefinitions to add in given index.
   */
  public OCompositeIndexDefinition(final String iClassName, final List<? extends OIndexDefinition> iIndexes, int version) {
    super();

    indexDefinitions = new ArrayList<OIndexDefinition>(5);
    for (OIndexDefinition indexDefinition : iIndexes) {
      indexDefinitions.add(indexDefinition);
      collate.addCollate(indexDefinition.getCollate());

      if (indexDefinition instanceof OIndexDefinitionMultiValue)
        if (multiValueDefinitionIndex == -1)
          multiValueDefinitionIndex = indexDefinitions.size() - 1;
        else
          throw new OIndexException("Composite key cannot contain more than one collection item");
    }

    className = iClassName;
  }

  /**
   * {@inheritDoc}
   */
  public String getClassName() {
    return className;
  }

  /**
   * Add new indexDefinition in current composite.
   * 
   * @param indexDefinition
   *          Index to add.
   */
  public void addIndex(final OIndexDefinition indexDefinition) {
    indexDefinitions.add(indexDefinition);
    if (indexDefinition instanceof OIndexDefinitionMultiValue) {
      if (multiValueDefinitionIndex == -1)
        multiValueDefinitionIndex = indexDefinitions.size() - 1;
      else
        throw new OIndexException("Composite key cannot contain more than one collection item");
    }

    collate.addCollate(indexDefinition.getCollate());
  }

  /**
   * {@inheritDoc}
   */
  public List<String> getFields() {
    final List<String> fields = new LinkedList<String>();
    for (final OIndexDefinition indexDefinition : indexDefinitions) {
      fields.addAll(indexDefinition.getFields());
    }
    return Collections.unmodifiableList(fields);
  }

  /**
   * {@inheritDoc}
   */
  public List<String> getFieldsToIndex() {
    final List<String> fields = new LinkedList<String>();
    for (final OIndexDefinition indexDefinition : indexDefinitions) {
      fields.addAll(indexDefinition.getFieldsToIndex());
    }
    return Collections.unmodifiableList(fields);
  }

  /**
   * {@inheritDoc}
   */
  public Object getDocumentValueToIndex(final ODocument iDocument) {
    final List<OCompositeKey> compositeKeys = new ArrayList<OCompositeKey>(10);
    final OCompositeKey firstKey = new OCompositeKey();
    boolean containsCollection = false;

    compositeKeys.add(firstKey);

    for (final OIndexDefinition indexDefinition : indexDefinitions) {
      final Object result = indexDefinition.getDocumentValueToIndex(iDocument);

      if (result == null && isNullValuesIgnored())
        return null;

      containsCollection = addKey(firstKey, compositeKeys, containsCollection, result);
    }

    if (!containsCollection)
      return firstKey;

    return compositeKeys;
  }

  public int getMultiValueDefinitionIndex() {
    return multiValueDefinitionIndex;
  }

  public String getMultiValueField() {
    if (multiValueDefinitionIndex >= 0)
      return indexDefinitions.get(multiValueDefinitionIndex).getFields().get(0);

    return null;
  }

  /**
   * {@inheritDoc}
   */
  public Object createValue(final List<?> params) {
    int currentParamIndex = 0;
    final OCompositeKey firstKey = new OCompositeKey();

    final List<OCompositeKey> compositeKeys = new ArrayList<OCompositeKey>(10);
    compositeKeys.add(firstKey);

    boolean containsCollection = false;

    for (final OIndexDefinition indexDefinition : indexDefinitions) {
      if (currentParamIndex + 1 > params.size())
        break;

      final int endIndex;
      if (currentParamIndex + indexDefinition.getParamCount() > params.size())
        endIndex = params.size();
      else
        endIndex = currentParamIndex + indexDefinition.getParamCount();

      final List<?> indexParams = params.subList(currentParamIndex, endIndex);
      currentParamIndex += indexDefinition.getParamCount();

      final Object keyValue = indexDefinition.createValue(indexParams);

      if (keyValue == null && isNullValuesIgnored())
        return null;

      containsCollection = addKey(firstKey, compositeKeys, containsCollection, keyValue);
    }

    if (!containsCollection)
      return firstKey;

    return compositeKeys;
  }

  public OIndexDefinitionMultiValue getMultiValueDefinition() {
    if (multiValueDefinitionIndex > -1)
      return (OIndexDefinitionMultiValue) indexDefinitions.get(multiValueDefinitionIndex);

    return null;
  }

  public OCompositeKey createSingleValue(final List<?> params) {
    final OCompositeKey compositeKey = new OCompositeKey();
    int currentParamIndex = 0;

    for (final OIndexDefinition indexDefinition : indexDefinitions) {
      if (currentParamIndex + 1 > params.size())
        break;

      final int endIndex;
      if (currentParamIndex + indexDefinition.getParamCount() > params.size())
        endIndex = params.size();
      else
        endIndex = currentParamIndex + indexDefinition.getParamCount();

      final List<?> indexParams = params.subList(currentParamIndex, endIndex);
      currentParamIndex += indexDefinition.getParamCount();

      final Object keyValue;

      if (indexDefinition instanceof OIndexDefinitionMultiValue)
        keyValue = ((OIndexDefinitionMultiValue) indexDefinition).createSingleValue(indexParams.toArray());
      else
        keyValue = indexDefinition.createValue(indexParams);

      if (keyValue == null && isNullValuesIgnored())
        return null;

      compositeKey.addKey(keyValue);
    }

    return compositeKey;
  }

  private static boolean addKey(OCompositeKey firstKey, List<OCompositeKey> compositeKeys, boolean containsCollection,
      Object keyValue) {
    if (keyValue instanceof Collection) {
      final Collection<?> collectionKey = (Collection<?>) keyValue;
      if (!containsCollection)
        for (int i = 1; i < collectionKey.size(); i++) {
          final OCompositeKey compositeKey = new OCompositeKey(firstKey.getKeys());
          compositeKeys.add(compositeKey);
        }
      else
        throw new OIndexException("Composite key cannot contain more than one collection item");

      int compositeIndex = 0;
      for (final Object keyItem : collectionKey) {
        final OCompositeKey compositeKey = compositeKeys.get(compositeIndex);
        compositeKey.addKey(keyItem);

        compositeIndex++;
      }

      containsCollection = true;
    } else if (containsCollection)
      for (final OCompositeKey compositeKey : compositeKeys)
        compositeKey.addKey(keyValue);
    else
      firstKey.addKey(keyValue);

    return containsCollection;
  }

  /**
   * {@inheritDoc}
   */
  public Object createValue(final Object... params) {
    if (params.length == 1 && params[0] instanceof Collection)
      return params[0];

    return createValue(Arrays.asList(params));
  }

  public void processChangeEvent(OMultiValueChangeEvent<?, ?> changeEvent, Map<OCompositeKey, Integer> keysToAdd,
      Map<OCompositeKey, Integer> keysToRemove, Object... params) {

    final OIndexDefinitionMultiValue indexDefinitionMultiValue = (OIndexDefinitionMultiValue) indexDefinitions
        .get(multiValueDefinitionIndex);

    final CompositeWrapperMap compositeWrapperKeysToAdd = new CompositeWrapperMap(keysToAdd, indexDefinitions, params,
        multiValueDefinitionIndex);

    final CompositeWrapperMap compositeWrapperKeysToRemove = new CompositeWrapperMap(keysToRemove, indexDefinitions, params,
        multiValueDefinitionIndex);

    indexDefinitionMultiValue.processChangeEvent(changeEvent, compositeWrapperKeysToAdd, compositeWrapperKeysToRemove);
  }

  /**
   * {@inheritDoc}
   */
  public int getParamCount() {
    int total = 0;
    for (final OIndexDefinition indexDefinition : indexDefinitions)
      total += indexDefinition.getParamCount();
    return total;
  }

  /**
   * {@inheritDoc}
   */
  public OType[] getTypes() {
    final List<OType> types = new LinkedList<OType>();
    for (final OIndexDefinition indexDefinition : indexDefinitions)
      Collections.addAll(types, indexDefinition.getTypes());

    return types.toArray(new OType[types.size()]);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final OCompositeIndexDefinition that = (OCompositeIndexDefinition) o;

    if (!className.equals(that.className))
      return false;
    if (!indexDefinitions.equals(that.indexDefinitions))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = indexDefinitions.hashCode();
    result = 31 * result + className.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "OCompositeIndexDefinition{" + "indexDefinitions=" + indexDefinitions + ", className='" + className + '\'' + '}';
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ODocument toStream() {
    document.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);
    try {
      serializeToStream();
    } finally {
      document.setInternalStatus(ORecordElement.STATUS.LOADED);
    }

    return document;
  }

  @Override
  protected void serializeToStream() {
    super.serializeToStream();

    final List<ODocument> inds = new ArrayList<ODocument>(indexDefinitions.size());
    final List<String> indClasses = new ArrayList<String>(indexDefinitions.size());

    document.field("className", className);
    for (final OIndexDefinition indexDefinition : indexDefinitions) {
      final ODocument indexDocument = indexDefinition.toStream();
      inds.add(indexDocument);

      indClasses.add(indexDefinition.getClass().getName());
    }
    document.field("indexDefinitions", inds, OType.EMBEDDEDLIST);
    document.field("indClasses", indClasses, OType.EMBEDDEDLIST);
    document.field("nullValuesIgnored", isNullValuesIgnored());
  }

  /**
   * {@inheritDoc}
   */
  public String toCreateIndexDDL(final String indexName, final String indexType, String engine) {
    final StringBuilder ddl = new StringBuilder("create index ");
    ddl.append(indexName).append(" on ").append(className).append(" ( ");

    final Iterator<String> fieldIterator = getFieldsToIndex().iterator();
    if (fieldIterator.hasNext()) {
      ddl.append(fieldIterator.next());
      while (fieldIterator.hasNext()) {
        ddl.append(", ").append(fieldIterator.next());
      }
    }
    ddl.append(" ) ").append(indexType).append(' ');

    if (engine != null)
      ddl.append(OCommandExecutorSQLCreateIndex.KEYWORD_ENGINE + " " + engine).append(' ');

    if (multiValueDefinitionIndex == -1) {
      boolean first = true;
      for (OType oType : getTypes()) {
        if (first)
          first = false;
        else
          ddl.append(", ");

        ddl.append(oType.name());
      }
    }

    return ddl.toString();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void fromStream() {
    serializeFromStream();
  }

  @Override
  protected void serializeFromStream() {
    super.serializeFromStream();

    try {
      className = document.field("className");

      final List<ODocument> inds = document.field("indexDefinitions");
      final List<String> indClasses = document.field("indClasses");

      indexDefinitions.clear();

      collate = new OCompositeCollate(this);

      for (int i = 0; i < indClasses.size(); i++) {
        final Class<?> clazz = Class.forName(indClasses.get(i));
        final ODocument indDoc = inds.get(i);

        final OIndexDefinition indexDefinition = (OIndexDefinition) clazz.getDeclaredConstructor().newInstance();
        indexDefinition.fromStream(indDoc);

        indexDefinitions.add(indexDefinition);
        collate.addCollate(indexDefinition.getCollate());

        if (indexDefinition instanceof OIndexDefinitionMultiValue)
          multiValueDefinitionIndex = indexDefinitions.size() - 1;
      }

      setNullValuesIgnored(!Boolean.FALSE.equals(document.<Boolean> field("nullValuesIgnored")));
    } catch (final ClassNotFoundException e) {
      throw new OIndexException("Error during composite index deserialization", e);
    } catch (final NoSuchMethodException e) {
      throw new OIndexException("Error during composite index deserialization", e);
    } catch (final InvocationTargetException e) {
      throw new OIndexException("Error during composite index deserialization", e);
    } catch (final InstantiationException e) {
      throw new OIndexException("Error during composite index deserialization", e);
    } catch (final IllegalAccessException e) {
      throw new OIndexException("Error during composite index deserialization", e);
    }
  }

  @Override
  public OCollate getCollate() {
    return collate;
  }

  @Override
  public void setCollate(OCollate collate) {
    throw new UnsupportedOperationException();
  }

  private static final class CompositeWrapperMap implements Map<Object, Integer> {
    private final Map<OCompositeKey, Integer> underlying;
    private final Object[]                    params;
    private final List<OIndexDefinition>      indexDefinitions;
    private final int                         multiValueIndex;

    private CompositeWrapperMap(Map<OCompositeKey, Integer> underlying, List<OIndexDefinition> indexDefinitions, Object[] params,
        int multiValueIndex) {
      this.underlying = underlying;
      this.params = params;
      this.multiValueIndex = multiValueIndex;
      this.indexDefinitions = indexDefinitions;
    }

    public int size() {
      return underlying.size();
    }

    public boolean isEmpty() {
      return underlying.isEmpty();
    }

    public boolean containsKey(Object key) {
      final OCompositeKey compositeKey = convertToCompositeKey(key);

      return underlying.containsKey(compositeKey);
    }

    public boolean containsValue(Object value) {
      return underlying.containsValue(value);
    }

    public Integer get(Object key) {
      return underlying.get(convertToCompositeKey(key));
    }

    public Integer put(Object key, Integer value) {
      final OCompositeKey compositeKey = convertToCompositeKey(key);
      return underlying.put(compositeKey, value);
    }

    public Integer remove(Object key) {
      return underlying.remove(convertToCompositeKey(key));
    }

    public void putAll(Map<? extends Object, ? extends Integer> m) {
      throw new UnsupportedOperationException("Unsupported because of performance reasons");
    }

    public void clear() {
      underlying.clear();
    }

    public Set<Object> keySet() {
      throw new UnsupportedOperationException("Unsupported because of performance reasons");
    }

    public Collection<Integer> values() {
      return underlying.values();
    }

    public Set<Entry<Object, Integer>> entrySet() {
      throw new UnsupportedOperationException();
    }

    private OCompositeKey convertToCompositeKey(Object key) {
      final OCompositeKey compositeKey = new OCompositeKey();

      int paramsIndex = 0;
      for (int i = 0; i < indexDefinitions.size(); i++) {
        final OIndexDefinition indexDefinition = indexDefinitions.get(i);
        if (i != multiValueIndex) {
          compositeKey.addKey(indexDefinition.createValue(params[paramsIndex]));
          paramsIndex++;
        } else
          compositeKey.addKey(((OIndexDefinitionMultiValue) indexDefinition).createSingleValue(key));
      }
      return compositeKey;
    }
  }

  @Override
  public boolean isAutomatic() {
    return indexDefinitions.get(0).isAutomatic();
  }
}
