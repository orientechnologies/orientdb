/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.sql.functions.math;

import com.orientechnologies.common.types.ORef;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;

/**
 * Abstract class for math functions.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public abstract class OSQLFunctionMathAbstract extends OSQLFunctionAbstract {

	public OSQLFunctionMathAbstract(String iName, int iMinParams, int iMaxParams) {
		super(iName, iMinParams, iMaxParams);
	}

	protected Number getContextValue(final ORef<Object> iContext, final Class<? extends Number> iClass) {
		if (iClass != iContext.value.getClass()) {
			// CHANGE TYPE
			if (iClass == Long.class)
				iContext.value = new Long(((Number) iContext.value).longValue());
			else if (iClass == Float.class)
				iContext.value = new Float(((Number) iContext.value).floatValue());
			else if (iClass == Double.class)
				iContext.value = new Double(((Number) iContext.value).doubleValue());
		}

		return (Number) iContext.value;
	}

	protected Class<? extends Number> getClassWithMorePrecision(final Class<? extends Number> iClass1,
			final Class<? extends Number> iClass2) {
		if (iClass1 == iClass2)
			return iClass1;

		if (iClass1 == Integer.class && (iClass2 == Long.class || iClass2 == Float.class || iClass2 == Double.class))
			return iClass2;
		else if (iClass1 == Long.class && (iClass2 == Float.class || iClass2 == Double.class))
			return iClass2;
		else if (iClass1 == Float.class && (iClass2 == Double.class))
			return iClass2;

		return iClass1;
	}
}
