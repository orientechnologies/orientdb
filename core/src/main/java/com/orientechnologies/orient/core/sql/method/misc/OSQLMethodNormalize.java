/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 * Copyright 2013 Geomatys.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.sql.method.misc;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.util.OPatternConst;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import java.text.Normalizer;

/**
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLMethodNormalize extends OAbstractSQLMethod {

  public static final String NAME = "normalize";

  public OSQLMethodNormalize() {
    super(NAME, 0, 2);
  }

  @Override
  public Object execute(
      Object iThis,
      OIdentifiable iCurrentRecord,
      OCommandContext iContext,
      Object ioResult,
      Object[] iParams) {

    if (ioResult != null) {
      final Normalizer.Form form;
      if (iParams != null && iParams.length > 0) {
        form = Normalizer.Form.valueOf(OIOUtils.getStringContent(iParams[0].toString()));
      } else {
        form = Normalizer.Form.NFD;
      }

      String normalized = Normalizer.normalize(ioResult.toString(), form);
      if (iParams != null && iParams.length > 1) {
        normalized = normalized.replaceAll(OIOUtils.getStringContent(iParams[0].toString()), "");
      } else {
        normalized = OPatternConst.PATTERN_DIACRITICAL_MARKS.matcher(normalized).replaceAll("");
      }
      ioResult = normalized;
    }
    return ioResult;
  }
}
