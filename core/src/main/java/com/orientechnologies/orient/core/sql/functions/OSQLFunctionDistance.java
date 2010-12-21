package com.orientechnologies.orient.core.sql.functions;

import java.util.List;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OSQLHelper;

public class OSQLFunctionDistance extends OSQLFunctionAbstract {
	public static final String	NAME	= "distance";

	public enum ALGORITHM {
		SPHERE
	}

	public enum UNIT {
		KM, MILES, NAUTICAL_MILES
	}

	private final static double	EARTH_RADIUS			= 6371;

	private String[]						configuredFields	= new String[4];
	private double[]						configuredValues	= new double[4];
	private double[]						recordValues			= new double[4];
	private ALGORITHM						algorithm					= ALGORITHM.SPHERE;
	private UNIT								unit							= UNIT.KM;

	public OSQLFunctionDistance() {
		super(NAME, 4, 5);
	}

	public void configure(ODatabaseRecord<?> iDatabase, final List<String> iParameters) {
		String paramName;
		Object paramValue;
		for (int i = 0; i < iParameters.size(); ++i) {
			paramName = iParameters.get(i);
			paramValue = OSQLHelper.parseValue(iDatabase, paramName);

			if (paramValue == OSQLHelper.VALUE_NOT_PARSED)
				configuredFields[i] = (String) paramName;
			else
				configuredValues[i] = Math.toRadians(Double.parseDouble(paramValue.toString()));
		}
	}

	public Object execute(final ORecord<?> iRecord) {
		ODocument record = (ODocument) iRecord;

		double distance;

		for (int i = 0; i < configuredFields.length; ++i) {
			if (configuredFields[i] != null) {
				// FILL VALUE WITH THE RECORD FIELD (IF ANY)
				if (!record.containsField(configuredFields[i]))
					// NO FIELD FOUND: JUST BREAK CONDITION EVALUATION
					return false;

				recordValues[i] = ((Double) OType.convert(record.field(configuredFields[i]), Double.class)).doubleValue();

				// CONVERT VALUES IN RADIANS
				recordValues[i] = Math.toRadians(recordValues[i]);
			} else {
				// GET CONFIGURED VALUE
				recordValues[i] = configuredValues[i];
			}
		}

		if (algorithm == ALGORITHM.SPHERE) {
			final double deltaLat = recordValues[2] - recordValues[0];
			final double deltaLon = recordValues[3] - recordValues[1];

			final double a = Math.pow(Math.sin(deltaLat / 2), 2) + Math.cos(recordValues[0]) * Math.cos(recordValues[2])
					* Math.pow(Math.sin(deltaLon / 2), 2);
			distance = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)) * EARTH_RADIUS;
		} else
			distance = 0;

		// CONVERT THE DISTANCE
		switch (unit) {
		case KM:
			break;

		case MILES:
			distance *= 0.621371192237;
			break;

		case NAUTICAL_MILES:
			distance *= 0.539956;
			break;
		}

		return distance;
	}

	public String getSyntax() {
		return "Syntax error: distance(<field-x>,<field-y>,<x-value>,<y-value>[,<unit>])";
	}
}
