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

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

/**
 * Abstract Delegate for OClass interface.
 * 
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
public class OClassAbstractDelegate implements OClass {

  protected final OClass delegate;

  public OClassAbstractDelegate(final OClass delegate) {
    this.delegate = delegate;
  }

  @Override
  public boolean isStrictMode() {
    return delegate.isStrictMode();
  }

  @Override
  public <T> T newInstance() throws InstantiationException, IllegalAccessException {
    return delegate.newInstance();
  }

  @Override
  public boolean isAbstract() {
    return delegate.isAbstract();
  }

  @Override
  public OClass setAbstract(boolean iAbstract) {
    return delegate.setAbstract(iAbstract);
  }

  @Override
  public OClass setStrictMode(boolean iMode) {
    return delegate.setStrictMode(iMode);
  }

  @Override
  public OClass getSuperClass() {
    return delegate.getSuperClass();
  }

  @Override
  public OClass setSuperClass(OClass iSuperClass) {
    return delegate.setSuperClass(iSuperClass);
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public String getStreamableName() {
    return delegate.getStreamableName();
  }

  @Override
  public Collection<OProperty> declaredProperties() {
    return delegate.declaredProperties();
  }

  @Override
  public Collection<OProperty> properties() {
    return delegate.properties();
  }

  @Override
  public Collection<OProperty> getIndexedProperties() {
    return delegate.getIndexedProperties();
  }

  @Override
  public OProperty getProperty(String iPropertyName) {
    return delegate.getProperty(iPropertyName);
  }

  @Override
  public OProperty createProperty(String iPropertyName, OType iType) {
    return delegate.createProperty(iPropertyName, iType);
  }

  @Override
  public OProperty createProperty(String iPropertyName, OType iType, OClass iLinkedClass) {
    return delegate.createProperty(iPropertyName, iType, iLinkedClass);
  }

  @Override
  public OProperty createProperty(String iPropertyName, OType iType, OType iLinkedType) {
    return delegate.createProperty(iPropertyName, iType, iLinkedType);
  }

  @Override
  public void dropProperty(String iPropertyName) {
    delegate.dropProperty(iPropertyName);
  }

  @Override
  public boolean existsProperty(String iPropertyName) {
    return delegate.existsProperty(iPropertyName);
  }

  @Override
  public Class<?> getJavaClass() {
    return delegate.getJavaClass();
  }

  @Override
  public int getDefaultClusterId() {
    return delegate.getDefaultClusterId();
  }

  @Override
  public int[] getClusterIds() {
    return delegate.getClusterIds();
  }

  @Override
  public OClass addClusterId(int iId) {
    return delegate.addClusterId(iId);
  }

  @Override
  public OClass removeClusterId(int iId) {
    return delegate.removeClusterId(iId);
  }

  @Override
  public int[] getPolymorphicClusterIds() {
    return delegate.getPolymorphicClusterIds();
  }

  @Override
  public Collection<OClass> getBaseClasses() {
    return delegate.getBaseClasses();
  }

  @Override
  public Collection<OClass> getAllBaseClasses() {
    return delegate.getAllBaseClasses();
  }

  @Override
  public long getSize() {
    return delegate.getSize();
  }

  @Override
  public float getOverSize() {
    return delegate.getOverSize();
  }

  @Override
  public OClass setOverSize(float overSize) {
    return delegate.setOverSize(overSize);
  }

  @Override
  public long count() {
    return delegate.count();
  }

  @Override
  public long count(boolean iPolymorphic) {
    return delegate.count(iPolymorphic);
  }

  @Override
  public void truncate() throws IOException {
    delegate.truncate();
  }

  @Override
  public boolean isSubClassOf(String iClassName) {
    return delegate.isSubClassOf(iClassName);
  }

  @Override
  public boolean isSubClassOf(OClass iClass) {
    return delegate.isSubClassOf(iClass);
  }

  @Override
  public boolean isSuperClassOf(OClass iClass) {
    return delegate.isSuperClassOf(iClass);
  }

  @Override
  public String getShortName() {
    return delegate.getShortName();
  }

  @Override
  public OClass setShortName(String shortName) {
    return delegate.setShortName(shortName);
  }

  @Override
  public Object get(ATTRIBUTES iAttribute) {
    return delegate.get(iAttribute);
  }

  @Override
  public OClass set(ATTRIBUTES attribute, Object iValue) {
    return delegate.set(attribute, iValue);
  }

  @Override
  public OIndex<?> createIndex(String iName, INDEX_TYPE iType, String... fields) {
    return delegate.createIndex(iName, iType, fields);
  }

  @Override
  public OIndex<?> createIndex(String iName, String iType, String... fields) {
    return delegate.createIndex(iName, iType, fields);
  }

  @Override
  public OIndex<?> createIndex(String iName, INDEX_TYPE iType, OProgressListener iProgressListener, String... fields) {
    return delegate.createIndex(iName, iType, iProgressListener, fields);
  }

  @Override
  public OIndex<?> createIndex(String iName, String iType, OProgressListener iProgressListener, ODocument metadata,
      String algorithm, String... fields) {
    return delegate.createIndex(iName, iType, iProgressListener, metadata, algorithm, fields);
  }

  @Override
  public OIndex<?> createIndex(String iName, String iType, OProgressListener iProgressListener, ODocument metadata,
      String... fields) {
    return delegate.createIndex(iName, iType, iProgressListener, metadata, fields);
  }

  @Override
  public Set<OIndex<?>> getInvolvedIndexes(Collection<String> fields) {
    return delegate.getInvolvedIndexes(fields);
  }

  @Override
  public Set<OIndex<?>> getInvolvedIndexes(String... fields) {
    return delegate.getInvolvedIndexes(fields);
  }

  @Override
  public Set<OIndex<?>> getClassInvolvedIndexes(Collection<String> fields) {
    return delegate.getClassInvolvedIndexes(fields);
  }

  @Override
  public Set<OIndex<?>> getClassInvolvedIndexes(String... fields) {
    return delegate.getClassInvolvedIndexes(fields);
  }

  @Override
  public boolean areIndexed(Collection<String> fields) {
    return delegate.areIndexed(fields);
  }

  @Override
  public boolean areIndexed(String... fields) {
    return delegate.areIndexed(fields);
  }

  @Override
  public OIndex<?> getClassIndex(String iName) {
    return delegate.getClassIndex(iName);
  }

  @Override
  public Set<OIndex<?>> getClassIndexes() {
    return delegate.getClassIndexes();
  }

  @Override
  public Set<OIndex<?>> getIndexes() {
    return delegate.getIndexes();
  }

  @Override
  public void setDefaultClusterId(int iDefaultClusterId) {
    delegate.setDefaultClusterId(iDefaultClusterId);
  }

  @Override
  public String getCustom(String iName) {
    return delegate.getCustom(iName);
  }

  @Override
  public OClassImpl setCustom(String iName, String iValue) {
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
  public int compareTo(OClass o) {
    return delegate.compareTo(o);
  }
}
