/*
 * Copyright 2012 Orient Technologies.
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
package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Default OrientDB index factory.<br>
 * Supports index types :
 * <ul>
 * <li>UNIQUE</li>
 * <li>NOTUNIQUE</li>
 * <li>FULLTEXT</li>
 * <li>DICTIONARY</li>
 * </ul>
 */
public class OMVRBIndexFactory implements OIndexFactory{

    private static final Set<String> TYPES;
    static {
        final Set<String> types = new HashSet<String>();
        types.add(OIndexUnique.TYPE_ID);
        types.add(OIndexNotUnique.TYPE_ID);
        types.add(OIndexFullText.TYPE_ID);
        types.add(OIndexDictionary.TYPE_ID);
        TYPES = Collections.unmodifiableSet(types);
    }
    
    /**
     * Index types : 
     * <ul> 
     * <li>UNIQUE</li> 
     * <li>NOTUNIQUE</li> 
     * <li>FULLTEXT</li>
     * <li>DICTIONARY</li> 
     * </ul>
     */
    public Set<String> getTypes() {
        return TYPES;
    }

    @Override
    public OIndexInternal createIndex(ODatabaseRecord iDatabase, String iIndexType) throws OConfigurationException {
        
        if(OIndexUnique.TYPE_ID.equals(iIndexType)){
            return new OIndexUnique();
        }else if(OIndexNotUnique.TYPE_ID.equals(iIndexType)){
            return new OIndexNotUnique();
        }else if(OIndexFullText.TYPE_ID.equals(iIndexType)){
            return new OIndexFullText();
        }else if(OIndexDictionary.TYPE_ID.equals(iIndexType)){
            return new OIndexDictionary();
        }
        
        throw new OConfigurationException("Unsupported type : "+iIndexType);
    }
    
}
