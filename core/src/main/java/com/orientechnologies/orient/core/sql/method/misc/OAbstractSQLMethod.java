/*
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
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;
import com.orientechnologies.orient.core.sql.method.OSQLMethod;
import java.text.ParseException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Johann Sorel (Geomatys)
 */
public abstract class OAbstractSQLMethod extends OSQLFunctionAbstract implements OSQLMethod {

    public OAbstractSQLMethod(String name) {
        this(name,0);
    }
    public OAbstractSQLMethod(String name, int nbparams) {
        this(name,nbparams,nbparams);
    }
    
    public OAbstractSQLMethod(String name, int minparams, int maxparams) {
        super(name, minparams+1, maxparams+1);
    }
    
    @Override
    public String getSyntax() {
        final int minparams = getMethodMinParams();
        final int maxparams = getMethodMaxParams();
        
        final StringBuilder sb = new StringBuilder("<field>.");
        sb.append(getName());
        sb.append('(');
        for(int i=0;i<minparams;i++){
            if(i!=0){
                sb.append(", ");
            }
            sb.append("param");
            sb.append(i+1);
        }
        if(minparams != maxparams){
            sb.append('[');
            for(int i=minparams;i<maxparams;i++){
                if(i!=0){
                    sb.append(", ");
                }
                sb.append("param");
                sb.append(i+1);
            }
            sb.append(']');
        }
        sb.append(')');
        
        return sb.toString();
    }

    @Override
    public final int getMethodMinParams() {
        return getMinParams()-1;
    }

    @Override
    public final int getMethodMaxParams() {
        return getMaxParams()-1;
    }

    @Override
    public Object execute(OIdentifiable iCurrentRecord, ODocument iCurrentResult, Object[] iFuncParams, OCommandContext iContext) {
        final Object self = iFuncParams[0];
        final Object[] methodParams = Arrays.copyOfRange(iFuncParams, 1, iFuncParams.length);
        try {
            return execute(iCurrentRecord, iContext, self, methodParams);
        } catch (ParseException ex) {
            Logger.getLogger(OAbstractSQLMethod.class.getName()).log(Level.WARNING, ex.getMessage(), ex);
            return null;
        }
    }
    
    protected  Object getParameterValue(final OIdentifiable iRecord, final String iValue) {
        if(iValue == null){
            return null;
        }

        if(iValue.charAt(0) == '\'' || iValue.charAt(0) == '"'){
            // GET THE VALUE AS STRING
            return iValue.substring(1, iValue.length() - 1);
        }

        // SEARCH FOR FIELD
        return ((ODocument) iRecord.getRecord()).field(iValue);
    }

    @Override
    public int compareTo(OSQLMethod o) {
        return this.getName().compareTo(o.getName());
    }
    
    
}
