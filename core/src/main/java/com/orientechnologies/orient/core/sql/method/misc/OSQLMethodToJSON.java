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

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 *
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli
 */
public class OSQLMethodToJSON extends OAbstractSQLMethod {

    public static final String NAME = "tojson";

    public OSQLMethodToJSON() {
        super(NAME,0,1);
    }

    @Override
    public Object execute(OIdentifiable iCurrentRecord, OCommandContext iContext, Object ioResult, Object[] iMethodParams) {
        ioResult = 
        		ioResult != null && ioResult instanceof ODocument ? 
        				iMethodParams.length==1 ? ((ODocument) ioResult).toJSON(((String)iMethodParams[0]).replace("\"","")) : ((ODocument) ioResult).toJSON()  
        		: null;
        return ioResult;
    }
}
