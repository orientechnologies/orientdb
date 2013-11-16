/*
 * Copyright 2010-2013 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.graph.console;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

/**
 * This is the factory to create OrientBaseGraph instances. This factory manages also a pool of instances to recycle them instead of
 * creating new instances everytime.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OrientGraphPool {
  protected final ODatabaseDocumentPool pool;
  protected boolean                     tx = true;

  public OrientGraphPool() {
    pool = new ODatabaseDocumentPool();
  }

  public OrientGraphPool(final String url, final String userName, final String userPassword) {
    checkArguments(url, userName, userPassword);

    pool = new ODatabaseDocumentPool(url, userName, userPassword);
  }

  public OrientBaseGraph acquire() {
    return create(pool.acquire());
  }

  public OrientBaseGraph acquire(final String url, final String userName, final String userPassword) {
    checkArguments(url, userName, userPassword);
    return create(pool.acquire(url, userName, userPassword));
  }

  public void release(final OrientBaseGraph iGraphToRelease) {
    if (iGraphToRelease == null)
      throw new IllegalArgumentException("graph instance cannot be null");
    pool.release(iGraphToRelease.getRawGraph());
  }

  public boolean isTx() {
    return tx;
  }

  public void setTx(final boolean tx) {
    this.tx = tx;
  }

  protected OrientBaseGraph create(final ODatabaseDocumentTx iConnection) {
    if (tx)
      return new OrientGraph(iConnection);
    return new OrientGraphNoTx(iConnection);
  }

  protected void checkArguments(final String url, final String userName, final String userPassword) {
    if (url == null)
      throw new IllegalArgumentException("url parameter cannot be null");
    if (userName == null)
      throw new IllegalArgumentException("userName parameter cannot be null");
    if (userPassword == null)
      throw new IllegalArgumentException("userPassword parameter cannot be null");
  }
}
