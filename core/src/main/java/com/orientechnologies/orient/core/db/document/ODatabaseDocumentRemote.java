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

package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.cache.OLocalRecordCache;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.security.*;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;
import com.orientechnologies.orient.core.storage.OStorage;

import java.util.Collections;
import java.util.Map;

/**
 * Created by tglman on 30/06/16.
 */
public class ODatabaseDocumentRemote extends ODatabaseDocumentTx {

  public ODatabaseDocumentRemote(final OStorage storage) {
    activateOnCurrentThread();

    try {
      status = STATUS.CLOSED;

      // OVERWRITE THE URL
      url = storage.getURL();
      this.storage = storage;
      this.componentsFactory = storage.getComponentsFactory();

      unmodifiableHooks = Collections.unmodifiableMap(hooks);

      localCache = new OLocalRecordCache();

      init();

      databaseOwner = this;
    } catch (Exception t) {
      ODatabaseRecordThreadLocal.INSTANCE.remove();

      throw OException.wrapException(new ODatabaseException("Error on opening database "), t);
    }

    setSerializer(defaultSerializer);
  }

  public <DB extends ODatabase> DB open(final String iUserName, final String iUserPassword) {
    throw new UnsupportedOperationException("Use OrientDBFactory");
  }

  @Deprecated
  public <DB extends ODatabase> DB open(final OToken iToken) {
    throw new UnsupportedOperationException("Deprecated Method");
  }

  @Override
  public <DB extends ODatabase> DB create() {
    throw new UnsupportedOperationException("Deprecated Method");
  }

  @Override
  public <DB extends ODatabase> DB create(String incrementalBackupPath) {
    throw new UnsupportedOperationException("use OrientDBFactory");
  }

  @Override
  public <DB extends ODatabase> DB create(final Map<OGlobalConfiguration, Object> iInitialSettings) {
    throw new UnsupportedOperationException("use OrientDBFactory");
  }

  @Override
  public void drop() {
    throw new UnsupportedOperationException("use OrientDBFactory");
  }

  public ODatabaseDocumentInternal copy() {
    return null;
  }

  @Override
  public boolean exists() {
    throw new UnsupportedOperationException("use OrientDBFactory");
  }

  public void internalOpen(String user, String password, OrientDBConfig config) {
    boolean failure = true;
    setupThreadOwner();
    try {

      storage.open(user, password, config.getConfigurations());

      status = STATUS.OPEN;

      initAtFirstOpen(user, password);

      // WAKE UP LISTENERS
      callOnOpenListeners();

      failure = false;
    } catch (OException e) {
      close();
      throw e;
    } catch (Exception e) {
      close();
      throw OException.wrapException(new ODatabaseException("Cannot open database url=" + getURL()), e);
    } finally {
      if (failure)
        owner.set(null);
    }
  }

  private void initAtFirstOpen(String iUserName, String iUserPassword) {
    if (initialized)
      return;

    ORecordSerializerFactory serializerFactory = ORecordSerializerFactory.instance();
    String serializeName = getStorage().getConfiguration().getRecordSerializer();
    if (serializeName == null)
      serializeName = ORecordSerializerSchemaAware2CSV.NAME;
    serializer = serializerFactory.getFormat(serializeName);
    if (serializer == null)
      throw new ODatabaseException("RecordSerializer with name '" + serializeName + "' not found ");
    if (getStorage().getConfiguration().getRecordSerializerVersion() > serializer.getMinSupportedVersion())
      throw new ODatabaseException("Persistent record serializer version is not support by the current implementation");

    componentsFactory = getStorage().getComponentsFactory();

    localCache.startup();

    user = null;

    loadMetadata();

    user = new OImmutableUser(-1, new OUser(iUserName, OUser.encryptPassword(iUserPassword))
        .addRole(new ORole("passthrough", null, ORole.ALLOW_MODES.ALLOW_ALL_BUT)));
    installHooksRemote();

    initialized = true;
  }

}
