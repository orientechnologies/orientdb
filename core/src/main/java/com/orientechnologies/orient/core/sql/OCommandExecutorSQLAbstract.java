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
package com.orientechnologies.orient.core.sql;

import java.util.Locale;

import com.orientechnologies.orient.core.command.OCommandExecutorAbstract;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;

/**
 * SQL abstract Command Executor implementation.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OCommandExecutorSQLAbstract extends OCommandExecutorAbstract {

	public static final String	KEYWORD_FROM		= "FROM";
	public static final String	KEYWORD_WHERE		= "WHERE";
	public static final String	KEYWORD_KEY			= "key";
	public static final String	KEYWORD_RID			= "rid";
	public static final String	CLUSTER_PREFIX	= "CLUSTER:";
	public static final String	CLASS_PREFIX		= "CLASS:";
	public static final String	INDEX_PREFIX		= "INDEX:";

	@Override
	public OCommandExecutorSQLAbstract init(final ODatabaseRecord iDatabase, String iText) {
		iText = iText.trim();
		textUpperCase = iText.toUpperCase(Locale.ENGLISH);
		return (OCommandExecutorSQLAbstract) super.init(iDatabase, iText);
	}
}
