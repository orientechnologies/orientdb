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
package com.orientechnologies.orient.object.db;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.object.OObjectLazyMultivalueElement;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.hook.ORecordHook.RESULT;
import com.orientechnologies.orient.core.hook.ORecordHook.TYPE;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.orientechnologies.orient.object.enhancement.OObjectProxyMethodHandler;
import com.orientechnologies.orient.object.serialization.OObjectSerializerHelper;
import javassist.util.proxy.Proxy;
import javassist.util.proxy.ProxyObject;

import java.lang.reflect.Field;
import java.util.*;
import java.util.Map.Entry;

@SuppressWarnings("unchecked")
public abstract class ODatabasePojoAbstract<T extends Object> extends ODatabaseWrapperAbstract<ODatabaseDocumentInternal, T> {
  protected IdentityHashMap<Object, ODocument> objects2Records = new IdentityHashMap<Object, ODocument>();
  protected IdentityHashMap<ODocument, T>      records2Objects = new IdentityHashMap<ODocument, T>();
  protected HashMap<ORID, ODocument>           rid2Records     = new HashMap<ORID, ODocument>();
  protected boolean                            retainObjects   = true;

  public ODatabasePojoAbstract(final ODatabaseDocumentInternal iDatabase) {
    super(iDatabase);
    iDatabase.setDatabaseOwner(this);
  }

  public abstract ODocument pojo2Stream(final T iPojo, final ODocument record);

  public abstract Object stream2pojo(final ODocument record, final Object iPojo, final String iFetchPlan);

  @Override
  public void close() {
    objects2Records.clear();
    records2Objects.clear();
    rid2Records.clear();
    super.close();
  }

  public OTransaction getTransaction() {
    return underlying.getTransaction();
  }

  public ODatabase<T> begin() {
    return (ODatabase<T>) underlying.begin();
  }

  public ODatabase<T> begin(final TXTYPE iType) {
    return (ODatabase<T>) underlying.begin(iType);
  }

  public ODatabase<T> begin(final OTransaction iTx) {
    return (ODatabase<T>) underlying.begin(iTx);
  }

  /**
   * Sets as dirty a POJO. This is useful when you change the object and need to tell to the engine to treat as dirty.
   *
   * @param iPojo User object
   */
  public void setDirty(final Object iPojo) {
    if (iPojo == null)
      return;

    final ODocument record = getRecordByUserObject(iPojo, false);
    if (record == null)
      throw new OObjectNotManagedException("The object " + iPojo + " is not managed by current database");

    record.setDirty();
  }

  /**
   * Sets as not dirty a POJO. This is useful when you change some other object and need to tell to the engine to treat this one as
   * not dirty.
   *
   * @param iPojo User object
   */
  public void unsetDirty(final Object iPojo) {
    if (iPojo == null)
      return;

    final ODocument record = getRecordByUserObject(iPojo, false);
    if (record == null)
      return;

    ORecordInternal.unsetDirty(record);
  }

  public void setInternal(final ATTRIBUTES attribute, final Object iValue) {
    underlying.setInternal(attribute, iValue);
  }

  public abstract ORID getIdentity(final Object iPojo);

  public OSecurityUser getUser() {
    return underlying.getUser();
  }

  public void setUser(OSecurityUser user) {
    underlying.setUser(user);
  }


  @Override
  public <RET extends List<?>> RET query(OQuery<?> iCommand, Object... iArgs) {
    checkOpenness();

    convertParameters(iArgs);

    final List<ODocument> result = underlying.query(iCommand, iArgs);

    if (result == null)
      return null;

    final List<Object> resultPojo = new ArrayList<Object>();
    Object obj;
    for (OIdentifiable doc : result) {
      if (doc instanceof ODocument) {
        // GET THE ASSOCIATED DOCUMENT
        if (((ODocument) doc).getClassName() == null)
          obj = doc;
        else
          obj = getUserObjectByRecord(((ODocument) doc), iCommand.getFetchPlan(), true);

        resultPojo.add(obj);
      } else {
        resultPojo.add(doc);
      }

    }

    return (RET) resultPojo;
  }

  @Override
  public OResultSet query(String query, Object... args) {
    return underlying.query(query, args);//TODO
  }

  @Override
  public OResultSet query(String query, Map args) {
    return underlying.query(query, args);//TODO
  }

  @Override
  public OResultSet command(String query, Object... args) {
    return underlying.query(query, args);//TODO
  }

  @Override
  public OResultSet command(String query, Map args) {
    return underlying.query(query, args);//TODO
  }

  public ODatabase<T> delete(final ORecord iRecord) {
    underlying.delete(iRecord);
    return this;
  }

  public <DBTYPE extends ODatabase<?>> DBTYPE registerHook(final ORecordHook iHookImpl) {
    underlying.registerHook(iHookImpl);
    return (DBTYPE) this;
  }

  public <DBTYPE extends ODatabase<?>> DBTYPE registerHook(final ORecordHook iHookImpl, ORecordHook.HOOK_POSITION iPosition) {
    underlying.registerHook(iHookImpl, iPosition);
    return (DBTYPE) this;
  }

  public RESULT callbackHooks(final TYPE iType, final OIdentifiable iObject) {
    return underlying.callbackHooks(iType, iObject);
  }

  public Map<ORecordHook, ORecordHook.HOOK_POSITION> getHooks() {
    return underlying.getHooks();
  }

  public <DBTYPE extends ODatabase<?>> DBTYPE unregisterHook(final ORecordHook iHookImpl) {
    underlying.unregisterHook(iHookImpl);
    return (DBTYPE) this;
  }

  public boolean isMVCC() {
    return underlying.isMVCC();
  }

  public <DBTYPE extends ODatabase<?>> DBTYPE setMVCC(final boolean iMvcc) {
    underlying.setMVCC(iMvcc);
    return (DBTYPE) this;
  }

  /**
   * Returns true if current configuration retains objects, otherwise false
   *
   * @see #setRetainObjects(boolean)
   */
  public boolean isRetainObjects() {
    return retainObjects;
  }

  /**
   * Specifies if retain handled objects in memory or not. Setting it to false can improve performance on large inserts. Default is
   * enabled.
   *
   * @param iValue True to enable, false to disable it.
   *
   * @see #isRetainObjects()
   */
  public ODatabasePojoAbstract<T> setRetainObjects(final boolean iValue) {
    retainObjects = iValue;
    return this;
  }
  public abstract ODocument getRecordByUserObject(final Object iPojo, final boolean iCreateIfNotAvailable) ;

  public boolean existsUserObjectByRID(ORID iRID) {
    return rid2Records.containsKey(iRID);
  }

  public ODocument getRecordById(final ORID iRecordId) {
    return iRecordId.isValid() ? rid2Records.get(iRecordId) : null;
  }

  public boolean isManaged(final Object iEntity) {
    return objects2Records.containsKey(iEntity);
  }

  public T getUserObjectByRecord(final OIdentifiable iRecord, final String iFetchPlan) {
    return getUserObjectByRecord(iRecord, iFetchPlan, true);
  }

  public abstract T getUserObjectByRecord(final OIdentifiable iRecord, final String iFetchPlan, final boolean iCreate);

  public abstract <RET extends Object> RET newInstance(String iClassName);

  protected void clearNewEntriesFromCache() {
    for (Iterator<Entry<ORID, ODocument>> it = rid2Records.entrySet().iterator(); it.hasNext(); ) {
      Entry<ORID, ODocument> entry = it.next();
      if (entry.getKey().isNew()) {
        it.remove();
      }
    }

    for (Iterator<Entry<Object, ODocument>> it = objects2Records.entrySet().iterator(); it.hasNext(); ) {
      Entry<Object, ODocument> entry = it.next();
      if (entry.getValue().getIdentity().isNew()) {
        it.remove();
      }
    }

    for (Iterator<Entry<ODocument, T>> it = records2Objects.entrySet().iterator(); it.hasNext(); ) {
      Entry<ODocument, T> entry = it.next();
      if (entry.getKey().getIdentity().isNew()) {
        it.remove();
      }
    }
  }

  /**
   * Converts an array of parameters: if a POJO is used, then replace it with its record id.
   *
   * @param iArgs Array of parameters as Object
   *
   * @see #convertParameter(Object)
   */
  protected void convertParameters(final Object... iArgs) {
    if (iArgs == null)
      return;

    // FILTER PARAMETERS
    for (int i = 0; i < iArgs.length; ++i)
      iArgs[i] = convertParameter(iArgs[i]);
  }

  /**
   * Convert a parameter: if a POJO is used, then replace it with its record id.
   *
   * @param iParameter Parameter to convert, if applicable
   *
   * @see #convertParameters(Object...)
   */
  protected Object convertParameter(final Object iParameter) {
    if (iParameter != null)
      // FILTER PARAMETERS
      if (iParameter instanceof Map<?, ?>) {
        Map<String, Object> map = (Map<String, Object>) iParameter;

        for (Entry<String, Object> e : map.entrySet()) {
          map.put(e.getKey(), convertParameter(e.getValue()));
        }

        return map;
      } else if (iParameter instanceof Collection<?>) {
        List<Object> result = new ArrayList<Object>();
        for (Object object : (Collection<Object>) iParameter) {
          result.add(convertParameter(object));
        }
        return result;
      } else if (iParameter.getClass().isEnum()) {
        return ((Enum<?>) iParameter).name();
      } else if (!OType.isSimpleType(iParameter)) {
        final ORID rid = getIdentity(iParameter);
        if (rid != null && rid.isValid())
          // REPLACE OBJECT INSTANCE WITH ITS RECORD ID
          return rid;
      }

    return iParameter;
  }
}
