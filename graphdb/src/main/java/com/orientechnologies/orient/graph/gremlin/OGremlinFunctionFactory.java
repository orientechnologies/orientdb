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
package com.orientechnologies.orient.graph.gremlin;

import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionFactory;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Johann Sorel (Geomatys)
 * @module pending
 */
public class OGremlinFunctionFactory implements OSQLFunctionFactory{

    private static final Map<String,Object> FUNCTIONS = new HashMap<String, Object>();
    static {
		FUNCTIONS.put(OSQLFunctionGremlin.NAME.toUpperCase(Locale.ENGLISH), OSQLFunctionGremlin.class);
    }
    
    public Set<String> getNames() {
        return FUNCTIONS.keySet();
    }

    public OSQLFunction createFunction(String name) {
        final Object obj = FUNCTIONS.get(name);
        
        if(obj == null){
            throw new OCommandExecutionException("Unknowned function name :" + name);
        }
        
        if(obj instanceof OSQLFunction){
            return (OSQLFunction) obj;
        }else{
            //it's a class
            final Class clazz = (Class) obj;
            try {
				return (OSQLFunction) clazz.newInstance();
			} catch (Exception e) {
				throw new OCommandExecutionException("Error in creation of function " + name
						+ "(). Probably there is not an empty constructor or the constructor generates errors", e);
			}
        }
        
    }
    
}
