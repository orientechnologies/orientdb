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




import java.util.Date;

/**
 *
 * @author Saeed Tabrizi (saeed a_t  nowcando.com)
 */
public class OTimeSpan {

    public static final long MilisPerSecond = 1000;
    public static final long MilisPerMinute = MilisPerSecond * 60;
    public static final long MilisPerHour= MilisPerMinute * 60;
    public static final long MilisPerDay = MilisPerHour*24;

    private long  totalDays =0;
    private long  totalHours =0;
    private long  totalMinutes =0;
    private long  totalSeconds =0;
    private long  totalMilis =0;

    private long  days =0;
    private byte  hours =0;
    private byte  minutes =0;
    private byte  seconds =0;
    private long   milis =0;
    private boolean isStartGreater = false;

    public  OTimeSpan(Date start,Date end){
        try{
            long diff = end.getTime() - start.getTime();
            if(diff<0) this.isStartGreater=true;
            this.totalDays = Math.floorDiv(diff,MilisPerDay)   ;
            this.totalHours = Math.floorDiv(diff,MilisPerHour);
            this.totalMinutes = Math.floorDiv(diff,MilisPerMinute);
            this.totalSeconds = Math.floorDiv(diff,MilisPerSecond);
            this.totalMilis = diff ;

            this.days = Math.floorDiv(diff,MilisPerDay) ;
            diff = Math.floorMod(diff,MilisPerDay);
            this.hours = (byte)Math.floorDiv(diff,MilisPerHour) ;
            diff = Math.floorMod(diff,MilisPerHour);
            this.minutes = (byte)Math.floorDiv(diff,MilisPerMinute);
            diff = Math.floorMod(diff,MilisPerSecond);
            this.seconds = (byte)Math.floorDiv(diff,MilisPerSecond);
            this.milis = (long) Math.floorMod(diff,MilisPerSecond) ;
        }
        catch(Exception ex){
            // nothing to do
        }


    }

    public boolean isStartGreater(){
        return this.isStartGreater;
    }

    public long getTotalDays(){
        return this.totalDays;
    }
    public long getTotalHours(){
        return this.totalHours;
    }
    public long getTotalMinutes(){
        return this.totalMinutes;
    }
    public long getTotalSeconds(){
        return this.totalSeconds;
    }
    public long getTotalMilis(){
        return this.totalMilis;
    }

    public long getDays(){
        return this.days;
    }
    public byte getHours(){
        return this.hours;
    }
    public byte getMinutes(){
        return this.minutes;
    }
    public byte getSeconds(){
        return this.seconds;
    }
    public long getMilis(){
        return this.milis;
    }

}
