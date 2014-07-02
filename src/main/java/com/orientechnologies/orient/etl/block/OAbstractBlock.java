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

package com.orientechnologies.orient.etl.block;

import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.etl.OAbstractETLComponent;

/**
 * Abstract Block.
 */
public abstract class OAbstractBlock extends OAbstractETLComponent implements OBlock {
  @Override
  public void execute() {
    if (!skip())
      executeBlock();
  }

  protected abstract void executeBlock();

  protected boolean skip() {
    if (ifFilter != null) {
      final Object result = ifFilter.evaluate(null, null, context);
      if (!(result instanceof Boolean))
        throw new OConfigurationException("'if' expression in Transformer " + getName() + " returned '" + result
            + "' instead of boolean");

      return !((Boolean) result).booleanValue();
    }
    return false;
  }
}
