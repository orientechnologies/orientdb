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

import com.ibm.icu.util.*;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.util.ODateHelper;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Year;
import java.util.Date;
import java.util.TimeZone;

/**
 * @author Saeed Tabrizi (saeed a_t  nowcando.com)
 */
public class OCalendar {

    static final String CAL_SATURDAY = "saturday";
    static final String CAL_SUNDAY = "sunday";
    static final String CAL_MONDAY = "monday";
    static final String CAL_TUESDAY = "tuesday";
    static final String CAL_WEDNESDAY = "wednesday";
    static final String CAL_THURSDAY = "thursday";
    static final String CAL_FRIDAY = "friday";

    static final String YEAR = "year";
    static final String QYEAR = "qyear";// shortcut 90 days
    static final String HYEAR = "hyear";// shortcut 180 days
    static final String MONTH = "month";
    static final String DAY = "day";
    static final String WEEK = "week"; // shortcut 7 days
    static final String HOUR = "hour";
    static final String MINUTE = "minute";
    static final String SECOND = "second";

    static final String CAL_GREGORIAN = "gregorian";
    static final String CAL_BUDDHIST = "buddhist";
    static final String CAL_CHINESE = "chinese";
    static final String CAL_COPTIC = "coptic";
    static final String CAL_HEBREW = "hebrew";
    static final String CAL_INDIAN = "indian";
    static final String CAL_ISLAMIC = "islamic";
    static final String CAL_JAPANESE = "japanese";
    static final String CAL_TAIWAN = "taiwan";
    static final String CAL_PERSIAN = "persian";
    static final String CAL_DANGI = "dangi";

    private String[] fromNowMessagDefault = new String[]{"remains", "ago",
            "year(s)", "month(s)", "day(s)", "hour(s)", "minute(s)", "second(s)", "milisecond(s)"};
    private com.ibm.icu.util.Calendar calendar;

    public OCalendar(Date date) {
        this(date, CAL_GREGORIAN);
    }

    public OCalendar(Date date, String name) {
        this(date, name, CAL_MONDAY);
    }

    public OCalendar(Date date, String name, String weekStaringDay) {
        this(date, name, weekStaringDay, new String[]{OCalendar.CAL_MONDAY, OCalendar.CAL_TUESDAY,
                OCalendar.CAL_WEDNESDAY, OCalendar.CAL_THURSDAY, OCalendar.CAL_FRIDAY});
    }

    public OCalendar(Date date, String name,
                     String weekStaringDay,
                     String[] weekWorkingDays
    ) {
        this(date, name, weekStaringDay, weekWorkingDays, new short[]{});
    }

    public OCalendar(Date date, String name,
                     String weekStaringDay,
                     String[] weekWorkingDays, // we extend this feature in next release
                     short[] yearHolidays // we extend this feature in next release
    ) {
        if (name == null) {
            name = "gregorian";
        }
        name = name.toLowerCase();
        switch (name) {
            case CAL_BUDDHIST:
                this.calendar = new BuddhistCalendar(date);
                break;
            case CAL_DANGI:
                this.calendar = new DangiCalendar(date);
                break;
            case CAL_CHINESE:
                this.calendar = new ChineseCalendar(date);
                break;
            case CAL_COPTIC:
                this.calendar = new CopticCalendar(date);
                break;
            case CAL_HEBREW:
                this.calendar = new HebrewCalendar(date);
                break;
            case CAL_INDIAN:
                this.calendar = new IndianCalendar(date);
                break;
            case CAL_ISLAMIC:
                this.calendar = new IslamicCalendar(date);
                break;
            case CAL_JAPANESE:
                this.calendar = new JapaneseCalendar(date);
                break;
            case CAL_TAIWAN:
                this.calendar = new TaiwanCalendar(date);
                break;
            case CAL_PERSIAN:
                this.calendar = new PersianCalendar(date);
                break;
            case CAL_GREGORIAN:
            default:
                this.calendar = new GregorianCalendar();
                this.calendar.setTime(date);
                break;
        }

        switch (weekStaringDay) {
            case CAL_SATURDAY:
                this.calendar.setFirstDayOfWeek(Calendar.SATURDAY);
                break;
            case CAL_SUNDAY:
                this.calendar.setFirstDayOfWeek(Calendar.SUNDAY);
                break;
            case CAL_MONDAY:
                this.calendar.setFirstDayOfWeek(Calendar.MONDAY);
                break;
            case CAL_TUESDAY:
                this.calendar.setFirstDayOfWeek(Calendar.TUESDAY);
                break;
            case CAL_WEDNESDAY:
                this.calendar.setFirstDayOfWeek(Calendar.WEDNESDAY);
                break;
            case CAL_THURSDAY:
                this.calendar.setFirstDayOfWeek(Calendar.THURSDAY);
                break;
            case CAL_FRIDAY:
                this.calendar.setFirstDayOfWeek(Calendar.FRIDAY);
                break;
            default:
                this.calendar.setFirstDayOfWeek(Calendar.MONDAY);
                break;
        }


    }


    public int getYear() {
        return this.calendar.get(Calendar.YEAR);
    }

    public int getMonth() {
        return this.calendar.get(Calendar.MONTH);
    }

    public int getDay() {
        return this.calendar.get(Calendar.DATE);
    }

    public int getHour() {
        return this.calendar.get(Calendar.HOUR);
    }

    public int getMinute() {
        return this.calendar.get(Calendar.MINUTE);
    }

    public int getSecond() {
        return this.calendar.get(Calendar.SECOND);
    }

    public int getMilisecond() {
        return this.calendar.get(Calendar.MILLISECOND);
    }

    public int getDayOfWeek() {
        return this.calendar.get(Calendar.DAY_OF_WEEK);
    }

    public int getDayOfMonth() {
        return this.calendar.get(Calendar.DAY_OF_MONTH);
    }

    public int getWeekOfMonth() {
        return this.calendar.get(Calendar.WEEK_OF_MONTH);
    }

    public int getWeekOfYear() {
        return this.calendar.get(Calendar.WEEK_OF_YEAR);
    }

    public int getEra() {
        return this.calendar.get(Calendar.ERA);
    }


    public int getDayOfWeekInMonth() {
        return this.calendar.get(Calendar.DAY_OF_WEEK_IN_MONTH);
    }

    public int getDayOfYear() {
        return this.calendar.get(Calendar.DAY_OF_YEAR);
    }

    public int getMaxYear() {
        return this.calendar.getActualMaximum(Calendar.YEAR);
    }

    public int getMaxMonth() {
        return this.calendar.getActualMaximum(Calendar.MONTH);
    }

    public int getMaxDay() {
        return this.calendar.getActualMaximum(Calendar.DATE);
    }

    public int getMaxDayOfMonth() {
        return this.calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
    }

    public int getMinDayOfMonth() {
        return this.calendar.getActualMinimum(Calendar.DAY_OF_MONTH);
    }

    public int getMaxDayOfYear() {
        return this.calendar.getActualMaximum(Calendar.DAY_OF_YEAR);
    }

    public int getMinDayOfYear() {
        return this.calendar.getActualMinimum(Calendar.DAY_OF_YEAR);
    }

    public int getMinDay() {
        return this.calendar.getActualMaximum(Calendar.DATE);
    }


    public int getMinYear() {
        return this.calendar.getActualMinimum(Calendar.YEAR);
    }

    public int getMinMonth() {
        return this.calendar.getActualMinimum(Calendar.MONTH);
    }

    public boolean isLeapYear() {

        return this.calendar.getActualMaximum(Calendar.DAY_OF_YEAR) == 366;
    }

    public boolean isLeapMonth() {

        return this.calendar.get(Calendar.IS_LEAP_MONTH) == 1;
    }

    public ULocale[] getAvailableLocales() {

        return this.calendar.getAvailableULocales();
    }


    public String getDaynameOfWeek() {
        int d = this.calendar.get(Calendar.DAY_OF_WEEK);
        switch (d) {
            case 1:
                return CAL_SUNDAY;
            case 2:
                return CAL_MONDAY;
            case 3:
                return CAL_TUESDAY;
            case 4:
                return CAL_WEDNESDAY;
            case 5:
                return CAL_THURSDAY;
            case 6:
                return CAL_FRIDAY;
            case 7:
                return CAL_SATURDAY;
        }
        return "";
    }

    public OCalendar add(Integer amount, String field) {
        switch (field) {
            case YEAR:
                this.calendar.add(Calendar.YEAR, amount);
                break;
            case QYEAR:
                this.calendar.add(Calendar.MONTH, amount * 3);
                break;
            case HYEAR:
                this.calendar.add(Calendar.MONTH, amount * 6);
                break;
            case MONTH:
                this.calendar.add(Calendar.MONTH, amount);
                break;
            case DAY:
                this.calendar.add(Calendar.DATE, amount);
                break;
            case WEEK:
                this.calendar.add(Calendar.DATE, amount * 7);
                break;
            case HOUR:
                this.calendar.add(Calendar.HOUR, amount);
                break;
            case MINUTE:
                this.calendar.add(Calendar.MINUTE, amount);
                break;
            case SECOND:
                this.calendar.add(Calendar.SECOND, amount);
                break;
            default:
                break;
        }
        return this;
    }

    public OCalendar subtract(Integer amount, String field) {
        switch (field) {
            case YEAR:
                this.calendar.add(Calendar.YEAR, -amount);
                break;
            case QYEAR:
                this.calendar.add(Calendar.MONTH, -amount * 3);
                break;
            case HYEAR:
                this.calendar.add(Calendar.MONTH, -amount * 6);
                break;
            case MONTH:
                this.calendar.add(Calendar.MONTH, -amount);
                break;
            case DAY:
                this.calendar.add(Calendar.DATE, -amount);
                break;
            case WEEK:
                this.calendar.add(Calendar.DATE, -amount * 7);
                break;
            case HOUR:
                this.calendar.add(Calendar.HOUR, -amount);
                break;
            case MINUTE:
                this.calendar.add(Calendar.MINUTE, -amount);
                break;
            case SECOND:
                this.calendar.add(Calendar.SECOND, -amount);
                break;
            default:
                break;
        }
        return this;
    }


    public OCalendar toCalendar(Date date, String name) {

        return new OCalendar(date, name);
    }

    public OCalendar toCalendar(Date date, String name,
                                String weekStaringDay) {

        return new OCalendar(date, name, weekStaringDay);
    }

    public OCalendar toCalendar(Date date, String name,
                                String weekStaringDay,
                                String[] weekWorkingDays
    ) {

        return new OCalendar(date, name, weekStaringDay, weekWorkingDays);
    }

    public OCalendar toCalendar(Date date, String name,
                                String weekStaringDay,
                                String[] weekWorkingDays,
                                short[] yearHolidays) {

        return new OCalendar(date, name, weekStaringDay, weekWorkingDays, yearHolidays);
    }

    public Date toDate() {

        return this.toDate(ODateHelper.getDatabaseTimeZone());
    }


    public Date toDate(TimeZone timezone) {
        com.ibm.icu.util.TimeZone tz = com.ibm.icu.util.TimeZone.getTimeZone(timezone.getID());
        this.calendar.setTimeZone(tz);
        return this.calendar.getTime();
    }

    public OCalendar toTimeZone(TimeZone timezone) {
        com.ibm.icu.util.TimeZone tz = com.ibm.icu.util.TimeZone.getTimeZone(timezone.getID());
        this.calendar.setTimeZone(tz);
        return this;
    }

    public OCalendar setDate(Date date) {
        String tz = ODateHelper.getDatabaseTimeZone().getID();
        return this.setDate(date, tz);
    }

    public OCalendar setDate(Date date, String timezoneid) {
        try {
            this.calendar.setTime(date);
            this.calendar.setTimeZone(com.ibm.icu.util.TimeZone.getTimeZone(timezoneid));
        } catch (Exception ex) {
            // do nothhing
        }

        return this;
    }

    public OCalendar setDate(int year,int month,int day ) {
        try {
            String tz = ODateHelper.getDatabaseTimeZone().getID();
            this.calendar.set(year,month,day);
            this.calendar.setTimeZone(com.ibm.icu.util.TimeZone.getTimeZone(tz));
        } catch (Exception ex) {
            // do nothhing
        }
        return this;
    }

    public OCalendar setDate(int year,int month,int day , String timezoneid) {
        try {
            this.calendar.set(year,month,day);
            this.calendar.setTimeZone(com.ibm.icu.util.TimeZone.getTimeZone(timezoneid));
        } catch (Exception ex) {
            // do nothhing
        }
        return this;
    }

    public OCalendar setDate(int year,int month,int day, int hour, int minute, int second , String timezoneid) {
        try {
            this.calendar.set(year,month,day,hour,minute,second);
            this.calendar.setTimeZone(com.ibm.icu.util.TimeZone.getTimeZone(timezoneid));
        } catch (Exception ex) {
            // do nothhing
        }

        return this;
    }


    public OCalendar startOf(String field) {
        try {
            field = field.toLowerCase();
            field = field.toLowerCase();
            switch (field) {
                case YEAR:
                    this.calendar.set(this.calendar.get(Calendar.YEAR), this.getMinMonth(), this.getMinDayOfYear());
                    break;
                case MONTH:
                    this.calendar.set(this.getYear(), this.getMonth(), this.getMinDayOfMonth());
                    break;
                case WEEK:
                    break;
                // TODO : we extend other combination in next release .

            }
        } catch (Exception ex) {
            // nothing do
        }
        return this;
    }

    public OCalendar endOf(String field) {
        try {
            field = field.toLowerCase();
            switch (field) {
                case YEAR:
                    this.calendar.set(this.calendar.get(Calendar.YEAR), this.getMaxMonth(), 1);
                    this.calendar.set(this.calendar.get(Calendar.YEAR), this.getMonth(), this.getMaxDayOfMonth());
                case MONTH:
                    this.calendar.set(this.calendar.get(Calendar.YEAR), this.getMonth(), this.getMaxDayOfMonth());
                case WEEK:
                    break;
                // TODO : we extend other combination in next release .
            }
        } catch (Exception ex) {
            // nothing do
        }

        return this;
    }

    public String format(String format) {
        return this.format(format, ODateHelper.getDatabaseTimeZone().getID());
    }


    public String format(String format, String localeID) {
        return this.format(format, ODateHelper.getDatabaseTimeZone().getID(), localeID);
    }


    public String format(String format, String timezoneid, String localeID) {
        String result = "";
        try {
            if (localeID == null || localeID.isEmpty()) {
                localeID = "en_US";
            }
            if (timezoneid == null || timezoneid.isEmpty()) {
                timezoneid = "Asia/Tehran";
            }
            ULocale locale = new ULocale(localeID);
            com.ibm.icu.util.TimeZone timezone = com.ibm.icu.util.TimeZone.getTimeZone(timezoneid);
            // Format the current time.
            com.ibm.icu.text.SimpleDateFormat dtf
                    = new com.ibm.icu.text.SimpleDateFormat(format, locale);

            dtf.setCalendar(this.calendar);
            dtf.setTimeZone(timezone);
            result = dtf.format(this.calendar.getTime());
        } catch (Exception ex) {
            // nothing to do
        }

        return result;
    }

    public OTimeSpan toTimeSpan(Date date) {
        return new OTimeSpan(this.toDate(), date);
    }

    public String fromNow() {

        return this.fromNow(this.fromNowMessagDefault, ULocale.ENGLISH);
    }

    public String fromNow(String[] messages) {

        return this.fromNow(messages, ULocale.ENGLISH);
    }

    public String fromNow(String[] messages, ULocale locale) {


        String result = "";
        try {
            OTimeSpan timeSpan = new OTimeSpan(ODateHelper.now(), this.calendar.getTime());
            String postfix_literal = messages[0];
            if (timeSpan.isStartGreater() == true) {
                postfix_literal = messages[1];
            }
            long days = Math.abs(timeSpan.getDays());

            if (days > 0) {
                if (days > 365) {
                    result = String.format("%d %s %s", Math.floorDiv(days, 365), messages[2], postfix_literal);
                } else {
                    if (days > 29) {
                        result = String.format("%d %s %s", Math.floorDiv(days, 12), messages[3], postfix_literal);
                    } else {
                        result = String.format("%d %s %s", days, messages[4], postfix_literal);
                    }
                }
            } else {
                long hours = Math.abs(timeSpan.getHours());
                if (hours > 0) {
                    result = String.format("%d %s %s", hours, messages[5], postfix_literal);
                } else {
                    long minutes = Math.abs(timeSpan.getMinutes());
                    if (minutes > 0) {
                        result = String.format("%d %s %s", minutes, messages[6], postfix_literal);
                    } else {
                        long seconds = Math.abs(timeSpan.getSeconds());
                        if (seconds > 0) {
                            result = String.format("%d %s %s", seconds, messages[7], postfix_literal);
                        } else {
                            result = String.format("%d %s %s", Math.abs(timeSpan.getSeconds()), messages[8], postfix_literal);
                        }
                    }
                }
            }
        } catch (Exception ex) {
            // do nothing
        }

        return result;
    }


}