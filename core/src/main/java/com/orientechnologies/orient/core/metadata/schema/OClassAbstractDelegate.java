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

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.clusterselection.OClusterSelectionStrategy;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;

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
  public OProperty createProperty(final String iPropertyName, final OType iType) {
    return delegate.createProperty(iPropertyName, iType);
  }

  @Override
  public OProperty createProperty(final String iPropertyName, final OType iType, final OClass iLinkedClass) {
    return delegate.createProperty(iPropertyName, iType, iLinkedClass);
  }

  @Override
  public OProperty createProperty(final String iPropertyName, final OType iType, final OType iLinkedType) {
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
  public int getClusterForNewInstance() {
    return delegate.getClusterForNewInstance();
  }

  @Override
  public int getDefaultClusterId() {
    return delegate.getDefaultClusterId();
  }

  @Override
  public void setDefaultClusterId(final int iDefaultClusterId) {
    delegate.setDefaultClusterId(iDefaultClusterId);
  }

  @Override
  public int[] getClusterIds() {
    return delegate.getClusterIds();
  }

  @Override
  public OClass addClusterId(final int iId) {
    return delegate.addClusterId(iId);
  }

  @Override
  public OClusterSelectionStrategy getClusterSelection() {
    return delegate.getClusterSelection();
  }

  @Override
  public OClass setClusterSelection(final OClusterSelectionStrategy clusterSelection) {
    return delegate.setClusterSelection(clusterSelection);
  }

  @Override
  public OClass setClusterSelection(final String iStrategyName) {
    return delegate.setClusterSelection(iStrategyName);
  }

  @Override
  public OClass addCluster(final String iClusterName) {
    return delegate.addCluster(iClusterName);
  }

  @Override
  public OClass addCluster(final String iClusterName, final OStorage.CLUSTER_TYPE iClusterType) {
    return delegate.addCluster(iClusterName, iClusterType);
  }

  @Override
  public OClass removeClusterId(final int iId) {
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
  public OClass setOverSize(final float overSize) {
    return delegate.setOverSize(overSize);
  }

  @Override
  public long count() {
    return delegate.count();
  }

  @Override
  public long count(final boolean iPolymorphic) {
    return delegate.count(iPolymorphic);
  }

  @Override
  public void truncate() throws IOException {
    delegate.truncate();
  }

  @Override
  public boolean isSubClassOf(final String iClassName) {
    return delegate.isSubClassOf(iClassName);
  }

  @Override
  public boolean isSubClassOf(final OClass iClass) {
    return delegate.isSubClassOf(iClass);
  }

  @Override
  public boolean isSuperClassOf(final OClass iClass) {
    return delegate.isSuperClassOf(iClass);
  }

  @Override
  public String getShortName() {
    return delegate.getShortName();
  }

  @Override
  public OClass setShortName(final String shortName) {
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
  public OIndex<?> createIndex(final String iName, final INDEX_TYPE iType, final String... fields) {
    return delegate.createIndex(iName, iType, fields);
  }

  @Override
  public OIndex<?> createIndex(final String iName, final String iType, final String... fields) {
    return delegate.createIndex(iName, iType, fields);
  }

  @Override
  public OIndex<?> createIndex(final String iName, final INDEX_TYPE iType, final OProgressListener iProgressListener,
      final String... fields) {
    return delegate.createIndex(iName, iType, iProgressListener, fields);
  }

  @Override
  public OIndex<?> createIndex(final String iName, final String iType, final OProgressListener iProgressListener,
      final ODocument metadata, String algorithm, String... fields) {
    return delegate.createIndex(iName, iType, iProgressListener, metadata, algorithm, fields);
  }

  @Override
  public OIndex<?> createIndex(final String iName, final String iType, final OProgressListener iProgressListener,
      final ODocument metadata, String... fields) {
    return delegate.createIndex(iName, iType, iProgressListener, metadata, fields);
  }

  @Override
  public Set<OIndex<?>> getInvolvedIndexes(final Collection<String> fields) {
    return delegate.getInvolvedIndexes(fields);
  }

  @Override
  public Set<OIndex<?>> getInvolvedIndexes(final String... fields) {
    return delegate.getInvolvedIndexes(fields);
  }

  @Override
  public Set<OIndex<?>> getClassInvolvedIndexes(final Collection<String> fields) {
    return delegate.getClassInvolvedIndexes(fields);
  }

  @Override
  public Set<OIndex<?>> getClassInvolvedIndexes(final String... fields) {
    return delegate.getClassInvolvedIndexes(fields);
  }

  @Override
  public boolean areIndexed(final Collection<String> fields) {
    return delegate.areIndexed(fields);
  }

  @Override
  public boolean areIndexed(final String... fields) {
    return delegate.areIndexed(fields);
  }

  @Override
  public OIndex<?> getClassIndex(final String iName) {
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
  public String getCustom(final String iName) {
    return delegate.getCustom(iName);
  }

  @Override
  public OClassImpl setCustom(final String iName, String iValue) {
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
  public int compareTo(final OClass o) {
    return delegate.compareTo(o);
  }
}
