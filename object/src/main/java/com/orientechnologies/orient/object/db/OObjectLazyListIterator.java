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
package com.orientechnologies.orient.object.db;

import java.io.Serializable;
import java.util.ConcurrentModificationException;
import java.util.Iterator;

import javassist.util.proxy.ProxyObject;

import com.orientechnologies.orient.object.enhancement.OObjectProxyMethodHandler;

/**
 * Lazy implementation of Iterator that load the records only when accessed. It keep also track of changes to the source record
 * avoiding to call setDirty() by hand.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings({ "unchecked" })
public class OObjectLazyListIterator<TYPE> implements Iterator<TYPE>, Serializable {
  private static final long           serialVersionUID = 3714399452499077452L;

  private final ProxyObject           sourceRecord;
  private final OObjectLazyList<TYPE> list;
  private String                      fetchPlan;
  private int                         cursor           = 0;
  private int                         lastRet          = -1;

  public OObjectLazyListIterator(final OObjectLazyList<TYPE> iSourceList, final ProxyObject iSourceRecord) {
    this.list = iSourceList;
    this.sourceRecord = iSourceRecord;
  }

  public TYPE next() {
    return next(fetchPlan);
  }

  public TYPE next(final String iFetchPlan) {
    final Object value = list.get(cursor);
    lastRet = cursor++;
    return (TYPE) value;
  }

  public boolean hasNext() {
    return cursor < (list.size());
  }

  public void remove() {
    if (lastRet == -1)
      throw new IllegalStateException();
    try {
      list.remove(lastRet);
      if (lastRet < cursor)
        cursor--;
      lastRet = -1;
    } catch (IndexOutOfBoundsException e) {
      throw new ConcurrentModificationException(e);
    }
    if (sourceRecord != null) {
      ((OObjectProxyMethodHandler) sourceRecord.getHandler()).setDirty();
    }
  }

  public String getFetchPlan() {
    return fetchPlan;
  }

  public void setFetchPlan(String fetchPlan) {
    this.fetchPlan = fetchPlan;
  }
}
