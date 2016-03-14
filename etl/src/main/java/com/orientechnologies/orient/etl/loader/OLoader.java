/*
 *
 *  * Copyright 2010-2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
import com.orientechnologies.orient.etl.OETLComponent;
import com.orientechnologies.orient.etl.OETLPipeline;
import com.orientechnologies.orient.etl.OETLPipelineComponent;

/**
 * ETL Loader.
 */
public interface OLoader extends OETLComponent {
  void load(OETLPipeline pipeline, final Object input, OCommandContext context);

  void beginLoader(OETLPipeline pipeline);

  void endLoader(OETLPipeline pipeline);

  long getProgress();

  String getUnit();

  void rollback(OETLPipeline pipeline);
}
