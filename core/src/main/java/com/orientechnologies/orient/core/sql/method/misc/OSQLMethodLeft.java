/*
 * Copyright 2013 Orient Technologies.
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

import com.orientechnologies.orient.core.db.record.OIdentifiable;

/**
 *
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli
 */
public class OSQLMethodLeft extends OAbstractSQLMethod {

    public static final String NAME = "left";

    public OSQLMethodLeft() {
        super(NAME, 1);
    }

    @Override
    public Object execute(OIdentifiable iCurrentRecord, Object ioResult, Object[] iMethodParams) {
        final int len = Integer.parseInt(iMethodParams[0].toString());
        ioResult = ioResult != null ? ioResult.toString().substring(0,
                len <= ioResult.toString().length() ? len : ioResult.toString().length()) : null;
        return ioResult;
    }
}
