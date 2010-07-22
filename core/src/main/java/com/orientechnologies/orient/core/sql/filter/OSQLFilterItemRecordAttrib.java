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
package com.orientechnologies.orient.core.sql.filter;

import java.io.IOException;

import com.orientechnologies.orient.core.command.OCommandToParse;
import com.orientechnologies.orient.core.record.ORecordFactory;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;

/**
 * Represent a special attribute value taken directly from the record such as:
 * <ul>
 * <li>@rid, the record id</li>
 * <li>@class, the record class if it's schema aware</li>
 * <li>@version, the version</li>
 * <li>@type, the type between: 'document', 'column', 'flat', 'bytes'</li>
 * <li>@size, the size on disk in bytes
 * </ul>
 * 
 * @author Luca Garulli
 * 
 */
public class OSQLFilterItemRecordAttrib extends OSQLFilterItemAbstract {
	public OSQLFilterItemRecordAttrib(final OCommandToParse iQueryToParse, final String iName) {
		super(iQueryToParse, iName);
	}

	public Object getValue(final ORecordInternal<?> iRecord) {
		if (name.equals("@RID"))
			return transformValue(iRecord.getDatabase(), iRecord.getIdentity());
		else if (name.equals("@VERSION"))
			return transformValue(iRecord.getDatabase(), iRecord.getVersion());
		else if (name.equals("@CLASS") && iRecord instanceof ORecordSchemaAware<?>)
			return transformValue(iRecord.getDatabase(), ((ORecordSchemaAware<?>) iRecord).getClassName());
		else if (name.equals("@TYPE"))
			return transformValue(iRecord.getDatabase(), ORecordFactory.getRecordTypeName(iRecord.getRecordType()));
		else if (name.equals("@SIZE")) {
			try {
				final byte[] stream = iRecord.toStream();
				if (stream != null)
					return transformValue(iRecord.getDatabase(), stream.length);
			} catch (IOException e) {
			}
		}

		return null;
	}
}
