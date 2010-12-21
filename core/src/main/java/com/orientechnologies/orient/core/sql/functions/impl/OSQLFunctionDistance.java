package com.orientechnologies.orient.core.sql.functions.impl;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionAbstract;

public class OSQLFunctionDistance extends OSQLFunctionAbstract {
	public static final String	NAME					= "distance";

	private final static double	EARTH_RADIUS	= 6371;

	public OSQLFunctionDistance() {
		super(NAME, 4, 5);
	}

	public Object execute(final ORecordInternal<?> iRecord, final Object[] iParameters) {
		double distance;

		final double[] values = new double[4];

		for (int i = 0; i < iParameters.length; ++i) {
			if (iParameters[i] == null)
				return null;

			values[i] = ((Double) OType.convert(iParameters[i], Double.class)).doubleValue();

			// CONVERT VALUES IN RADIANS
			values[i] = Math.toRadians(values[i]);
		}

		final double deltaLat = values[2] - values[0];
		final double deltaLon = values[3] - values[1];

		final double a = Math.pow(Math.sin(deltaLat / 2), 2) + Math.cos(values[0]) * Math.cos(values[2])
				* Math.pow(Math.sin(deltaLon / 2), 2);
		distance = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)) * EARTH_RADIUS;

		return distance;
	}

	public String getSyntax() {
		return "Syntax error: distance(<field-x>,<field-y>,<x-value>,<y-value>[,<unit>])";
	}
}
