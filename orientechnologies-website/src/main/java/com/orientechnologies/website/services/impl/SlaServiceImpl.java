package com.orientechnologies.website.services.impl;

import com.orientechnologies.website.model.schema.dto.Contract;
import com.orientechnologies.website.model.schema.dto.Priority;
import com.orientechnologies.website.services.SlaService;
import org.joda.time.DateTime;
import org.joda.time.LocalTime;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by Enrico Risa on 12/05/15.
 */
@Service
public class SlaServiceImpl implements SlaService {
  @Override
  public Date calculateDueTime(Date now, Contract contract, Priority priority) {

    return calculateDiffDate(now, contract, priority);

  }

  private Date calculateDiffDate(Date now, Contract contract, Priority priority) {
    int resolutionHours = findResolutionHour(contract, priority);

    DateTime dt = new DateTime(now);
    int responseInMinutes = resolutionHours * 60;

    while (responseInMinutes > 0) {

      int dayOfMonth = dt.getDayOfMonth();
      dt = incrementMinute(dt, contract);
      int dayOfMonth1 = dt.getDayOfMonth();
      if (dayOfMonth == dayOfMonth1) {
        dt = dt.plusMinutes(1);
        responseInMinutes--;
      }

    }

    return incrementMinute(dt, contract).toDate();
  }

  private DateTime incrementMinute(DateTime dt, Contract contract) {
    if (isInCurrentBusinessDay(dt, contract)) {
      return dt;
    } else {
      return skipDay(dt, contract);
    }
  }

  private boolean isInCurrentBusinessDay(DateTime dt, Contract contract) {

    int dayOfWeek = dt.getDayOfWeek();
    if (dayOfWeek > contract.getBusinessHours().size()) {
      return false;
    } else {
      LocalTime localTime = dt.toLocalTime();
      String s = contract.getBusinessHours().get(dayOfWeek - 1);
      if (s == null) {
        return false;
      }
      List<LocalTime> fromTo = getFromTo(s);
      LocalTime to = fromTo.get(1);
      if (localTime.getHourOfDay() == 23 && localTime.getMinuteOfHour() == 59) {
        return true;
      }
      if (localTime.getHourOfDay() >= to.getHourOfDay() && localTime.getMinuteOfHour() >= to.getMinuteOfHour()) {
        return false;
      }

    }
    return true;
  }

  private DateTime skipDay(DateTime dt, Contract contract) {

    int dayOfWeek = dt.getDayOfWeek();

    int daysToAdd;
    if (dayOfWeek > contract.getBusinessHours().size()) {
      daysToAdd = 8 - dayOfWeek;
      dayOfWeek = 1;
    } else {
      daysToAdd = 1;
    }
    String s = contract.getBusinessHours().get(dayOfWeek - 1);
    List<LocalTime> fromTo = getFromTo(s);
    LocalTime from = fromTo.get(0);
    return dt.plusDays(daysToAdd).hourOfDay().setCopy(from.getHourOfDay()).minuteOfHour().setCopy(from.getMinuteOfHour());
  }

  private int findResolutionHour(Contract contract, Priority priority) {
    Integer integer = contract.getSlas().get(priority.getNumber());
    Integer integer1 = contract.getSlas().get(priority.getNumber().toString());
    if (integer == null && integer1 == null) {
      throw new RuntimeException("Error calculating sla. Configuration not found for" + priority);
    }
    return integer != null ? integer : integer1;
  }

  private List<LocalTime> getFromTo(String business) {

    final String[] split = business.split("-");

    return new ArrayList<LocalTime>() {
      {
        add(parseSingleRule(split[0].trim()));
        add(parseSingleRule(split[1].trim()));
      }

    };

  }

  private LocalTime parseSingleRule(String rule) {

    String[] split = rule.split(":");
    int hour = new Integer(split[0]);
    int minute = new Integer(split[1]);
    return new LocalTime(hour, minute);

  }

}
