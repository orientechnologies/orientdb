/*
 * Copyright 2013 Geomatys
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.sql.functions.misc;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.serialization.OBase64Utils;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;

/**
 * Encode a string in various format (only base64 for now)
 *
 * @author Johann Sorel (Geomatys)
 */
public class OSQLFunctionDecode extends OSQLFunctionAbstract {

    public static final String NAME = "decode";

    /**
     * Get the date at construction to have the same date for all the iteration.
     */
    public OSQLFunctionDecode() {
        super(NAME, 2, 2);
    }

    @Override
    public Object execute(OIdentifiable iCurrentRecord, ODocument iCurrentResult, final Object[] iParameters, OCommandContext iContext) {

        final String candidate = iParameters[0].toString();
        final String format = iParameters[1].toString();

        if(OSQLFunctionEncode.FORMAT_BASE64.equalsIgnoreCase(format)){
            return OBase64Utils.decode(candidate);
        }else{
            throw new OException("unknowned format :"+format);
        }
    }

    @Override
    public String getSyntax() {
        return "Syntax error: decode(<binaryfield>, <format>)";
    }
}
