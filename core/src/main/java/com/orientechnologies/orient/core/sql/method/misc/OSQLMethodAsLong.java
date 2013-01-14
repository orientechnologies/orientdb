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
import java.util.Date;

/**
 *
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli
 */
public class OSQLMethodAsLong extends OAbstractSQLMethod {

    public static final String NAME = "aslong";

    public OSQLMethodAsLong() {
        super(NAME);
    }

    @Override
    public Object execute(OIdentifiable iCurrentRecord, Object ioResult, Object[] iMethodParams) {
        if (ioResult instanceof Number) {
            ioResult = ((Number) ioResult).longValue();
        } else if (ioResult instanceof Date) {
            ioResult = ((Date) ioResult).getTime();
        } else {
            ioResult = ioResult != null ? new Long(ioResult.toString().trim()) : null;
        }
        return ioResult;
    }
}
