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

/**
 * Basic interface for pooled database implementations.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public interface ODatabasePooled {

	/**
	 * Reuses current instance.
	 * 
	 * @param iOwner
	 * @param iAdditionalArgs
	 */
	public void reuse(final Object iOwner, final Object[] iAdditionalArgs);

	/**
	 * Tells if the underlying database is closed.
	 */
	public boolean isUnderlyingOpen();

	/**
	 * Force closing the current instance avoiding to being reused.
	 */
	public void forceClose();
}
