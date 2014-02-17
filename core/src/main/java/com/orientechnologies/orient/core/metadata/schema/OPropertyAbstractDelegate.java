/*
 * Copyright 2010-2014 Orient Technologies LTD (info--at--orientechnologies.com)
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

import java.util.Collection;
import java.util.Set;

/**
 * Abstract Delegate for OProperty interface.
 * 
 * @author Luca Garulli (http://www.orientechnologies.com)
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
  public String getFullName() {
    return delegate.getFullName();
  }

  @Override
  public OProperty setName(String iName) {
    return delegate.setName(iName);
  }

  @Override
  public void set(ATTRIBUTES attribute, Object iValue) {
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
  public OProperty setNotNull(boolean iNotNull) {
    return delegate.setNotNull(iNotNull);
  }

  @Override
  public OCollate getCollate() {
    return delegate.getCollate();
  }

  @Override
  public OProperty setCollate(String iCollateName) {
    return delegate.setCollate(iCollateName);
  }

  @Override
  public boolean isMandatory() {
    return delegate.isMandatory();
  }

  @Override
  public OProperty setMandatory(boolean mandatory) {
    return delegate.setMandatory(mandatory);
  }

  @Override
  public boolean isReadonly() {
    return delegate.isReadonly();
  }

  @Override
  public OPropertyImpl setReadonly(boolean iReadonly) {
    return delegate.setReadonly(iReadonly);
  }

  @Override
  public String getMin() {
    return delegate.getMin();
  }

  @Override
  public OProperty setMin(String min) {
    return delegate.setMin(min);
  }

  @Override
  public String getMax() {
    return delegate.getMax();
  }

  @Override
  public OProperty setMax(String max) {
    return delegate.setMax(max);
  }

  @Override
  public OIndex<?> createIndex(OClass.INDEX_TYPE iType) {
    return delegate.createIndex(iType);
  }

  @Override
  public OIndex<?> createIndex(String iType) {
    return delegate.createIndex(iType);
  }

  @Override
  @Deprecated
  public OPropertyImpl dropIndexes() {
    return delegate.dropIndexes();
  }

  @Override
  @Deprecated
  public Set<OIndex<?>> getIndexes() {
    return delegate.getIndexes();
  }

  @Override
  @Deprecated
  public OIndex<?> getIndex() {
    return delegate.getIndex();
  }

  @Override
  public Collection<OIndex<?>> getAllIndexes() {
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
  public OPropertyImpl setRegexp(String regexp) {
    return delegate.setRegexp(regexp);
  }

  @Override
  public OPropertyImpl setType(OType iType) {
    return delegate.setType(iType);
  }

  @Override
  public String getCustom(String iName) {
    return delegate.getCustom(iName);
  }

  @Override
  public OPropertyImpl setCustom(String iName, String iValue) {
    return delegate.setCustom(iName, iValue);
  }

  @Override
  public void removeCustom(String iName) {
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
  public Object get(ATTRIBUTES iAttribute) {
    return delegate.get(iAttribute);
  }

  @Override
  public int compareTo(OProperty o) {
    return delegate.compareTo(o);
  }
}
