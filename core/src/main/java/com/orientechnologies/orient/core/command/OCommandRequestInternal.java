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
package com.orientechnologies.orient.core.command;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.serialization.OSerializableStream;

import java.util.Map;

/**
 * Internal specialization of generic OCommand interface.
 * 
 * @author Luca Garulli
 * 
 */
public interface OCommandRequestInternal extends OCommandRequest, OSerializableStream {

  Map<Object, Object> getParameters();

  OCommandResultListener getResultListener();

  void setResultListener(OCommandResultListener iListener);

  OProgressListener getProgressListener();

  OCommandRequestInternal setProgressListener(OProgressListener iProgressListener);

  void reset();
}
