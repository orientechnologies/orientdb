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
package com.orientechnologies.orient.core.metadata.function;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OProxedResource;
import java.util.Set;

/**
 * Proxy class to access to the centralized Function Library instance.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OFunctionLibraryProxy extends OProxedResource<OFunctionLibraryImpl>
    implements OFunctionLibrary {
  public OFunctionLibraryProxy(
      final OFunctionLibraryImpl iDelegate, final ODatabaseDocumentInternal iDatabase) {
    super(iDelegate, iDatabase);
  }

  @Override
  public Set<String> getFunctionNames() {
    return delegate.getFunctionNames();
  }

  @Override
  public OFunction getFunction(final String iName) {
    return delegate.getFunction(iName);
  }

  @Override
  public OFunction createFunction(final String iName) {
    return delegate.createFunction(database, iName);
  }

  @Override
  public void create() {
    delegate.create(database);
  }

  @Override
  public void load() {
    delegate.load(database);
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public void dropFunction(OFunction function) {
    delegate.dropFunction(function);
  }

  @Override
  public void dropFunction(String iName) {
    delegate.dropFunction(iName);
  }
}
