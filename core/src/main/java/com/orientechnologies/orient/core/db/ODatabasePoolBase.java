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
package com.orientechnologies.orient.core.db;

import com.orientechnologies.common.concur.resource.OReentrantResourcePool;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;

import java.util.Map;

/**
 * Database pool base class.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class ODatabasePoolBase<DB extends ODatabaseInternal> extends Thread {
  protected final String              url;
  protected final String              userName;
  protected final String              userPassword;
  protected ODatabasePoolAbstract<DB> dbPool;

  protected ODatabasePoolBase() {
    url = userName = userPassword = null;
  }

  protected ODatabasePoolBase(final String iURL, final String iUserName, final String iUserPassword) {
    url = iURL;
    userName = iUserName;
    userPassword = iUserPassword;
  }

  public ODatabasePoolBase<DB> setup() {
    if (dbPool == null)
      setup(OGlobalConfiguration.DB_POOL_MIN.getValueAsInteger(), OGlobalConfiguration.DB_POOL_MAX.getValueAsInteger());

    return this;
  }

  public ODatabasePoolBase<DB> setup(final int iMinSize, final int iMaxSize) {
    if (dbPool == null)
      setup(iMinSize, iMaxSize, OGlobalConfiguration.DB_POOL_IDLE_TIMEOUT.getValueAsLong(),
          OGlobalConfiguration.DB_POOL_IDLE_CHECK_DELAY.getValueAsLong());

    return this;
  }

  public ODatabasePoolBase<DB> setup(final int iMinSize, final int iMaxSize, final long idleTimeout,
      final long timeBetweenEvictionRunsMillis) {
    if (dbPool == null)
      synchronized (this) {
        if (dbPool == null) {
          dbPool = new ODatabasePoolAbstract<DB>(this, iMinSize, iMaxSize, idleTimeout, timeBetweenEvictionRunsMillis) {

            public void onShutdown() {
              if (owner instanceof ODatabasePoolBase<?>)
                ((ODatabasePoolBase<?>) owner).close();
            }

            public DB createNewResource(final String iDatabaseName, final Object... iAdditionalArgs) {
              if (iAdditionalArgs.length < 2)
                throw new OSecurityAccessException("Username and/or password missed");

              return createResource(owner, iDatabaseName, iAdditionalArgs);
            }

            public boolean reuseResource(final String iKey, final Object[] iAdditionalArgs, final DB iValue) {
              if (((ODatabasePooled) iValue).isUnderlyingOpen()) {
                ((ODatabasePooled) iValue).reuse(owner, iAdditionalArgs);
                if (iValue.getStorage().isClosed())
                  // STORAGE HAS BEEN CLOSED: REOPEN IT
                  iValue.getStorage().open((String) iAdditionalArgs[0], (String) iAdditionalArgs[1], null);
                else if (!iValue.getUser().checkPassword((String) iAdditionalArgs[1]))
                  throw new OSecurityAccessException(iValue.getName(), "User or password not valid for database: '"
                      + iValue.getName() + "'");

                return true;
              }
              return false;
            }
          };
        }
      }
    return this;
  }

  /**
   * Acquires a connection from the pool using the configured URL, user-name and user-password. If the pool is empty, then the
   * caller thread will wait for it.
   * 
   * @return A pooled database instance
   */
  public DB acquire() {
    setup();
    return dbPool.acquire(url, userName, userPassword);
  }

  /**
   * Acquires a connection from the pool. If the pool is empty, then the caller thread will wait for it.
   * 
   * @param iName
   *          Database name
   * @param iUserName
   *          User name
   * @param iUserPassword
   *          User password
   * @return A pooled database instance
   */
  public DB acquire(final String iName, final String iUserName, final String iUserPassword) {
    setup();
    return dbPool.acquire(iName, iUserName, iUserPassword);
  }

  /**
   * Returns amount of available connections which you can acquire for given source and user name. Source id is consist of
   * "source name" and "source user name".
   * 
   * @param name
   *          Source name.
   * @param userName
   *          User name which is used to acquire source.
   * @return amount of available connections which you can acquire for given source and user name.
   */
  public int getAvailableConnections(final String name, final String userName) {
    setup();
    return dbPool.getAvailableConnections(name, userName);
  }

	public int getCreatedInstances(final  String name, final String userName) {
		setup();
		return dbPool.getCreatedInstances(name, userName);
	}

  /**
   * Acquires a connection from the pool specifying options. If the pool is empty, then the caller thread will wait for it.
   * 
   * @param iName
   *          Database name
   * @param iUserName
   *          User name
   * @param iUserPassword
   *          User password
   * @return A pooled database instance
   */
  public DB acquire(final String iName, final String iUserName, final String iUserPassword,
      final Map<String, Object> iOptionalParams) {
    setup();
    return dbPool.acquire(iName, iUserName, iUserPassword, iOptionalParams);
  }

  public int getConnectionsInCurrentThread(final String name, final String userName) {
    if (dbPool == null)
      return 0;
    return dbPool.getConnectionsInCurrentThread(name, userName);
  }

  /**
   * Don't call it directly but use database.close().
   * 
   * @param iDatabase
   */
  public void release(final DB iDatabase) {
    if (dbPool != null)
      dbPool.release(iDatabase);
  }

  /**
   * Closes the entire pool freeing all the connections.
   */
  public void close() {
    if (dbPool != null) {
      dbPool.close();
      dbPool = null;
    }
  }

  /**
   * Returns the maximum size of the pool
   * 
   */
  public int getMaxSize() {
    setup();
    return dbPool.getMaxSize();
  }

  /**
   * Returns all the configured pools.
   * 
   */
  public Map<String, OReentrantResourcePool<String, DB>> getPools() {
    return dbPool.getPools();
  }

  /**
   * Removes a pool by name/user
   * 
   */
  public void remove(final String iName, final String iUser) {
    dbPool.remove(iName, iUser);
  }

  @Override
  public void run() {
    close();
  }

  protected abstract DB createResource(Object owner, String iDatabaseName, Object... iAdditionalArgs);
}
