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
package com.orientechnologies.orient.core.sql.functions.geo;

import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;

/**
 * Haversine formula to compute the distance between 2 gro points.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionDistance extends OSQLFunctionAbstract {
	public static final String	NAME					= "distance";

	private final static double	EARTH_RADIUS	= 6371;

	public OSQLFunctionDistance() {
		super(NAME, 4, 5);
	}

	public Object execute(ORecord<?> iCurrentRecord, final Object[] iParameters, OCommandExecutor iRequester) {
		try {
			double distance;

			final double[] values = new double[4];

			for (int i = 0; i < iParameters.length; ++i) {
				if (iParameters[i] == null)
					return null;

				values[i] = ((Double) OType.convert(iParameters[i], Double.class)).doubleValue();
			}

			final double deltaLat = Math.toRadians(values[2] - values[0]);
			final double deltaLon = Math.toRadians(values[3] - values[1]);

			final double a = Math.pow(Math.sin(deltaLat / 2), 2) + Math.cos(Math.toRadians(values[0]))
					* Math.cos(Math.toRadians(values[2])) * Math.pow(Math.sin(deltaLon / 2), 2);
			distance = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)) * EARTH_RADIUS;

			return distance;
		} catch (Exception e) {
			return null;
		}
	}

	public String getSyntax() {
		return "Syntax error: distance(<field-x>,<field-y>,<x-value>,<y-value>[,<unit>])";
	}
}
