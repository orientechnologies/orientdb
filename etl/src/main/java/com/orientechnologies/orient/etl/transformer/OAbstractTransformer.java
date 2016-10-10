/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (info(-at-)orientdb.com)
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

package com.orientechnologies.orient.etl.transformer;

import com.orientechnologies.orient.etl.OAbstractETLPipelineComponent;

import static com.orientechnologies.orient.etl.OETLProcessor.LOG_LEVELS.*;

/**
 * Abstract Transformer.
 */
public abstract class OAbstractTransformer extends OAbstractETLPipelineComponent implements OTransformer {
  @Override
  public Object transform(final Object input) {
    log(DEBUG, "Transformer input: %s", input);

    if (input == null)
      return null;

    if (!skip(input)) {
      context.setVariable("input", input);
      //to assure that db instance is on ThreadLocal
      databaseProvider.getDocumentDatabase();
      databaseProvider.getGraphDatabase();

      final Object result = executeTransform(input);
      if (output == null) {
        log(DEBUG, "Transformer output: %s", result);
        return result;
      }
      context.setVariable(output, result);
    }
    log(DEBUG, "Transformer output (same as input): %s", input);
    return input;
  }

  protected abstract Object executeTransform(final Object input);
}
