/*
 *
 *  * Copyright 2010-2017 OrientDB Ltd (http://orientdb.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package com.orientechnologies.orient.etl.loader;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.etl.OETLPipeline;
import com.orientechnologies.orient.etl.OETLProcessHaltedException;

/**
 * Loader implementation based on events. It allows to stream data from the an ETL process and work with the transformed data in
 * real-time without storing the data anywhere.
 *
 * @author Luca Garulli
 */
public class OStreamLoader extends OAbstractLoader {
  private Listener listener;

  public interface Listener {
    void onLoad(Object input, OCommandContext context);
  }

  public void registerListener(final Listener listener) {
    this.listener = listener;
  }

  @Override
  public void load(OETLPipeline pipeline, Object input, OCommandContext context) {
    if (listener == null)
      throw new OETLProcessHaltedException("Listener not configured for stream loader");
    listener.onLoad(input, context);
  }

  @Override
  public String getUnit() {
    return "documents";
  }

  @Override
  public void rollback(OETLPipeline pipeline) {
  }

  @Override
  public String getName() {
    return "stream";
  }
}
