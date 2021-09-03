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
package com.orientechnologies.orient.core.command;

import com.orientechnologies.common.listener.OProgressListener;
import java.util.Map;

/**
 * Internal specialization of generic OCommand interface.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public interface OCommandRequestInternal extends OCommandRequest {

  Map<Object, Object> getParameters();

  OCommandResultListener getResultListener();

  void setResultListener(OCommandResultListener iListener);

  OProgressListener getProgressListener();

  OCommandRequestInternal setProgressListener(OProgressListener iProgressListener);

  void reset();

  boolean isCacheableResult();

  void setCacheableResult(boolean iValue);

  /**
   * Communicate to a listener if the result set is an record based or anything else
   *
   * @param recordResultSet
   */
  void setRecordResultSet(boolean recordResultSet);

  boolean isRecordResultSet();
}
