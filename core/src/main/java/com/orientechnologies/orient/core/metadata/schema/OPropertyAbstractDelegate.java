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
  public OProperty setName(final String iName) {
    return delegate.setName(iName);
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
    return delegate.setNotNull(iNotNull);
  }

  @Override
  public OCollate getCollate() {
    return delegate.getCollate();
  }

  @Override
  public OProperty setCollate(final String iCollateName) {
    return delegate.setCollate(iCollateName);
  }

  @Override
  public boolean isMandatory() {
    return delegate.isMandatory();
  }

  @Override
  public OProperty setMandatory(final boolean mandatory) {
    return delegate.setMandatory(mandatory);
  }

  @Override
  public boolean isReadonly() {
    return delegate.isReadonly();
  }

  @Override
  public OPropertyImpl setReadonly(final boolean iReadonly) {
    return delegate.setReadonly(iReadonly);
  }

  @Override
  public String getMin() {
    return delegate.getMin();
  }

  @Override
  public OProperty setMin(final String min) {
    return delegate.setMin(min);
  }

  @Override
  public String getMax() {
    return delegate.getMax();
  }

  @Override
  public OProperty setMax(final String max) {
    return delegate.setMax(max);
  }

  @Override
  public OIndex<?> createIndex(final OClass.INDEX_TYPE iType) {
    return delegate.createIndex(iType);
  }

  @Override
  public OIndex<?> createIndex(final String iType) {
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
  public OPropertyImpl setRegexp(final String regexp) {
    return delegate.setRegexp(regexp);
  }

  @Override
  public OPropertyImpl setType(final OType iType) {
    return delegate.setType(iType);
  }

  @Override
  public String getCustom(final String iName) {
    return delegate.getCustom(iName);
  }

  @Override
  public OPropertyImpl setCustom(final String iName, final String iValue) {
    return delegate.setCustom(iName, iValue);
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
