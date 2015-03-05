/*
  *
  *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
  *  *
  *  *  Licensed under the Apache License, Version 2.0 (the "License");
  *  *  you may not use this file except in compliance with the License.
  *  *  You may obtain a copy of the License at
  *  *
  *  *       http://www.apache.org/licenses/LICENSE-2.0
  *  *
  *  *  Unless required by applicable law or agreed to in writing, software
  *  *  distributed under the License is distributed on an "AS IS" BASIS,
  *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  *  *  See the License for the specific language governing permissions and
  *  *  limitations under the License.
  *  *
  *  * For more information: http://www.orientechnologies.com
  *
  */
package com.orientechnologies.orient.core.sql.filter;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;

/**
 * Represents a constant value, used to bind parameters.
 * 
 * @author Luca Garulli
 * 
 */
public class OSQLFilterItemParameter implements OSQLFilterItem {
	private final String				name;
	private Object							value				= NOT_SETTED;

	private static final String	NOT_SETTED	= "?";

	public OSQLFilterItemParameter(final String iName) {
		this.name = iName;
	}

	public Object getValue(final OIdentifiable iRecord, Object iCurrentResult, OCommandContext iContetx) {
		return value;
	}

	@Override
	public String toString() {
		if (value == NOT_SETTED) {
                    return name.equals("?") ? "?" : ":" + name;
                } else {
                    return value == null ? "null" : value.toString();
                }
	}

	public String getName() {
		return name;
	}

	public void setValue(Object value) {
		this.value = value;
	}
}
