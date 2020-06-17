/*
 * Copyright 2010-2014 OrientDB LTD (info--at--orientdb.com)
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

package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Collection;
import java.util.Set;

/**
 * Abstract Delegate for OProperty interface.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com) (http://orientdb.com)
 */
public class OPropertyAbstractDelegate implements OProperty {

  protected final OProperty delegate;

  public OPropertyAbstractDelegate(final OProperty delegate) {
    this.delegate = delegate;
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public Integer getId() {
    return delegate.getId();
  }

  @Override
  public String getFullName() {
    return delegate.getFullName();
  }

  @Override
  public OProperty setName(final String iName) {
    delegate.setName(iName);
    return this;
  }

  @Override
  public String getDescription() {
    return delegate.getDescription();
  }

  @Override
  public OProperty setDescription(String iDescription) {
    delegate.setDescription(iDescription);
    return this;
  }

  @Override
  public void set(final ATTRIBUTES attribute, final Object iValue) {
    delegate.set(attribute, iValue);
  }

  @Override
  public OType getType() {
    return delegate.getType();
  }

  @Override
  public OClass getLinkedClass() {
    return delegate.getLinkedClass();
  }

  @Override
  public OType getLinkedType() {
    return delegate.getLinkedType();
  }

  @Override
  public boolean isNotNull() {
    return delegate.isNotNull();
  }

  @Override
  public OProperty setNotNull(final boolean iNotNull) {
    delegate.setNotNull(iNotNull);
    return this;
  }

  @Override
  public OCollate getCollate() {
    return delegate.getCollate();
  }

  @Override
  public OProperty setCollate(final String iCollateName) {
    delegate.setCollate(iCollateName);
    return this;
  }

  @Override
  public boolean isMandatory() {
    return delegate.isMandatory();
  }

  @Override
  public OProperty setMandatory(final boolean mandatory) {
    delegate.setMandatory(mandatory);
    return this;
  }

  @Override
  public boolean isReadonly() {
    return delegate.isReadonly();
  }

  @Override
  public OProperty setReadonly(final boolean iReadonly) {
    delegate.setReadonly(iReadonly);
    return this;
  }

  @Override
  public String getMin() {
    return delegate.getMin();
  }

  @Override
  public OProperty setMin(final String min) {
    delegate.setMin(min);
    return this;
  }

  @Override
  public String getMax() {
    return delegate.getMax();
  }

  @Override
  public OProperty setMax(final String max) {
    delegate.setMax(max);
    return this;
  }

  @Override
  public String getDefaultValue() {
    return delegate.getDefaultValue();
  }

  @Override
  public OProperty setDefaultValue(final String defaultValue) {
    delegate.setDefaultValue(defaultValue);
    return this;
  }

  @Override
  public OIndex createIndex(final OClass.INDEX_TYPE iType) {
    return delegate.createIndex(iType);
  }

  @Override
  public OIndex createIndex(final String iType) {
    return delegate.createIndex(iType);
  }

  @Override
  public OIndex createIndex(String iType, ODocument metadata) {
    return delegate.createIndex(iType, metadata);
  }

  @Override
  public OIndex createIndex(OClass.INDEX_TYPE iType, ODocument metadata) {
    return delegate.createIndex(iType, metadata);
  }

  @Override
  public OProperty setLinkedClass(OClass oClass) {
    delegate.setLinkedClass(oClass);
    return this;
  }

  @Override
  public OProperty setLinkedType(OType type) {
    delegate.setLinkedType(type);
    return this;
  }

  @Override
  public OProperty setCollate(OCollate collate) {
    delegate.setCollate(collate);
    return this;
  }

  @Override
  @Deprecated
  public OProperty dropIndexes() {
    delegate.dropIndexes();
    return this;
  }

  @Override
  @Deprecated
  public Set<OIndex> getIndexes() {
    return delegate.getIndexes();
  }

  @Override
  @Deprecated
  public OIndex getIndex() {
    return delegate.getIndex();
  }

  @Override
  public Collection<OIndex> getAllIndexes() {
    return delegate.getAllIndexes();
  }

  @Override
  @Deprecated
  public boolean isIndexed() {
    return delegate.isIndexed();
  }

  @Override
  public String getRegexp() {
    return delegate.getRegexp();
  }

  @Override
  public OProperty setRegexp(final String regexp) {
    delegate.setRegexp(regexp);
    return this;
  }

  @Override
  public OProperty setType(final OType iType) {
    delegate.setType(iType);
    return this;
  }

  @Override
  public String getCustom(final String iName) {
    return delegate.getCustom(iName);
  }

  @Override
  public OProperty setCustom(final String iName, final String iValue) {
    delegate.setCustom(iName, iValue);
    return this;
  }

  @Override
  public void removeCustom(final String iName) {
    delegate.removeCustom(iName);
  }

  @Override
  public void clearCustom() {
    delegate.clearCustom();
  }

  @Override
  public Set<String> getCustomKeys() {
    return delegate.getCustomKeys();
  }

  @Override
  public OClass getOwnerClass() {
    return delegate.getOwnerClass();
  }

  @Override
  public Object get(final ATTRIBUTES iAttribute) {
    return delegate.get(iAttribute);
  }

  @Override
  public int compareTo(final OProperty o) {
    return delegate.compareTo(o);
  }
}
