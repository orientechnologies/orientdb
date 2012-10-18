/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.metadata.function;

import java.util.Set;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OProxedResource;

/**
 * Proxy class to access to the centralized Function Library instance.
 * 
 * @author Luca Garulli
 * 
 */
public class OFunctionLibraryProxy extends OProxedResource<OFunctionLibrary> implements OFunctionLibrary {
  public OFunctionLibraryProxy(final OFunctionLibrary iDelegate, final ODatabaseRecord iDatabase) {
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
    return delegate.createFunction(iName);
  }

  @Override
  public void create() {
    delegate.create();
  }

  @Override
  public void load() {
    delegate.load();
  }

  @Override
  public void close() {
    delegate.close();
  }
}
