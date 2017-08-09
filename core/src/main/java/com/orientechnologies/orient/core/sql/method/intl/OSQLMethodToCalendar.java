/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
package com.orientechnologies.orient.core.sql.method.intl;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.method.misc.OAbstractSQLMethod;
import com.orientechnologies.orient.core.util.ODateHelper;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * @author Saeed Tabrizi (saeed a_t  nowcando.com)
 */
public class OSQLMethodToCalendar extends OAbstractSQLMethod {

    public static final String NAME = "tocalendar";
    private String[] weekWorkingDaysDefault = new String[]{OCalendar.CAL_MONDAY, OCalendar.CAL_TUESDAY,
            OCalendar.CAL_WEDNESDAY, OCalendar.CAL_THURSDAY, OCalendar.CAL_FRIDAY};
    private short[] yearHolidaysDefault = new short[]{};

    public OSQLMethodToCalendar() {
        super(NAME, 0, 3);
    }

    protected Object getParameterValueOrDefault(final OIdentifiable iRecord, final String iValue, final Object iDefault) {
        Object result = getParameterValue(iRecord, iValue);
        if (result == null) {
            return iDefault;
        }
        return result;
    }

    protected Object getParameterValueOrDefault(final Object iValue, final Object iDefault) {

        if (iValue == null) {
            return iDefault;
        }
        return iValue;
    }

    @Override
    public Object execute(final Object iThis, final OIdentifiable iRecord, final OCommandContext iContext, Object ioResult,
                          final Object[] iParams) {

        try {

            if (iParams == null) {
                return ioResult;
            }
            if (iParams.length == 0) {
                iParams[0]=OCalendar.CAL_GREGORIAN;
            }

            String calendarName = iParams.length >= 1 ? (String) getParameterValueOrDefault(iParams[0], OCalendar.CAL_GREGORIAN) : OCalendar.CAL_GREGORIAN;
            String startOfWeek = iParams.length >= 2 ? (String) getParameterValueOrDefault(iParams[1], weekWorkingDaysDefault[0]) : weekWorkingDaysDefault[0];
            String[] weekWorkingDays = iParams.length >= 3 ? (String[]) getParameterValueOrDefault(iParams[2], weekWorkingDaysDefault) : weekWorkingDaysDefault;
            short[] yearHolidays = iParams.length >= 4 ? (short[]) getParameterValueOrDefault(iParams[3], yearHolidaysDefault) : yearHolidaysDefault;


            if (calendarName instanceof String) {
                calendarName = ((String) calendarName).toLowerCase();
            }

            if (startOfWeek instanceof String) {
                startOfWeek = ((String) startOfWeek).toLowerCase();
            }


            if (ioResult != null) {
                Date dt = null;
                if (ioResult instanceof Date) { // if input is a Date type
                    dt = (Date) ioResult;

                } else if (ioResult instanceof Long || ioResult instanceof Integer) { // if input is a parsable date string .
                    dt = new Date((long) ioResult);

                } else if (ioResult instanceof String) { // if input is a parsable date string .
                    final DateFormat format = ODateHelper.getDateTimeFormatInstance();
                    try {
                        dt = format.parse(ioResult.toString());

                    } catch (Exception ex) {
                        // nothing to do
                    }

                }

                if (dt != null) {
                    ioResult = new OCalendar(dt, calendarName, startOfWeek, weekWorkingDays, yearHolidays);
                }


            }
        } catch(Exception ex) {
            // nothing to do
        }


        return ioResult;
    }
}
