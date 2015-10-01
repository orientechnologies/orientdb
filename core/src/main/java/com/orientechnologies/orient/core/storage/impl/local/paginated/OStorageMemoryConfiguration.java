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

package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.storage.OStorage;

import java.io.IOException;
import java.util.Map;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 7/15/14
 */
public class OStorageMemoryConfiguration extends OStorageConfiguration {
  private static final long serialVersionUID = 7001342008735208586L;

  private byte[]            serializedContent;

  public OStorageMemoryConfiguration(OStorage iStorage) {
    super(iStorage);
  }

  public void close() throws IOException {
    super.close();
  }

  public void create() throws IOException {
    super.create();
  }

  @Override
  public OStorageConfiguration load(final Map<String, Object> iProperties) throws OSerializationException {
    initConfiguration();

    bindPropertiesToContext(iProperties);

    try {
      fromStream(serializedContent);
    } catch (Exception e) {
      throw OException.wrapException(new OSerializationException(
          "Cannot load database's configuration. The database seems to be corrupted"), e);
    }
    return this;
  }

  @Override
  public void lock() throws IOException {
  }

  @Override
  public void unlock() throws IOException {
  }

  @Override
  public void update() throws OSerializationException {
    try {
      serializedContent = toStream();
    } catch (Exception e) {
      throw OException.wrapException(new OSerializationException("Error on update storage configuration"), e);
    }
  }

  public void synch() throws IOException {
  }

  @Override
  public void setSoftlyClosed(boolean softlyClosed) throws IOException {
  }

}
