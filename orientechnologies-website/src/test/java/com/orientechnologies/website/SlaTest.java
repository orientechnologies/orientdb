package com.orientechnologies.website;

import com.orientechnologies.website.model.schema.dto.Contract;
import com.orientechnologies.website.model.schema.dto.Priority;
import com.orientechnologies.website.repository.ContractRepository;
import com.orientechnologies.website.services.SlaService;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Date;

/**
 * Created by Enrico Risa on 12/05/15.
 */

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = Application.class)
@ActiveProfiles("test")
public class SlaTest {

  @Autowired
  SlaService         slaService;

  @Autowired
  ContractRepository contractRepository;

  // TEST 8/5
  @Test
  public void testSla() {

    Contract contract = new Contract();

    contract.getBusinessHours().add("10:00-19:00");
    contract.getBusinessHours().add("10:00-19:00");
    contract.getBusinessHours().add("10:00-19:00");
    contract.getBusinessHours().add("10:00-19:00");
    contract.getBusinessHours().add("10:00-19:00");

    contract.getSlas().put(1, 2);
    contract.getSlas().put(2, 4);
    contract.getSlas().put(3, 8);
    contract.getSlas().put(4, 24);

    contract = contractRepository.save(contract);

    Assert.assertNotNull(contract);

    Priority priority = new Priority();
    priority.setName("critical");
    priority.setNumber(1);
    DateTime dateTime = new DateTime(2015, 5, 26, 12, 0, 0, 0);

    Date dueDate = slaService.calculateDueTime(dateTime.toDate(), contract, priority);

    DateTime dueTime = new DateTime(dueDate);
    Assert.assertEquals(dateTime.plusHours(2).getMillis(), dueTime.getMillis());

    dateTime = new DateTime(2015, 5, 26, 19, 0, 0, 0);

    dueDate = slaService.calculateDueTime(dateTime.toDate(), contract, priority);

    dueTime = new DateTime(dueDate);

    Assert.assertEquals(dateTime.plusDays(1).hourOfDay().setCopy(12).getMillis(), dueTime.getMillis());

    dateTime = new DateTime(2015, 5, 26, 17, 1, 0, 0);

    dueDate = slaService.calculateDueTime(dateTime.toDate(), contract, priority);

    dueTime = new DateTime(dueDate);

    Assert.assertEquals(dateTime.plusDays(1).hourOfDay().setCopy(10).minuteOfHour().setCopy(1).getMillis(), dueTime.getMillis());

    dateTime = new DateTime(2015, 5, 26, 17, 0, 0, 0);

    dueDate = slaService.calculateDueTime(dateTime.toDate(), contract, priority);

    dueTime = new DateTime(dueDate);

    Assert.assertEquals(dateTime.plusDays(1).hourOfDay().setCopy(10).minuteOfHour().setCopy(0).getMillis(), dueTime.getMillis());

    dateTime = new DateTime(2015, 5, 26, 18, 30, 0, 0);

    dueDate = slaService.calculateDueTime(dateTime.toDate(), contract, priority);

    dueTime = new DateTime(dueDate);

    Assert.assertEquals(dateTime.plusDays(1).hourOfDay().setCopy(11).minuteOfHour().setCopy(30).getMillis(), dueTime.getMillis());

    dateTime = new DateTime(2015, 5, 26, 18, 59, 0, 0);

    dueDate = slaService.calculateDueTime(dateTime.toDate(), contract, priority);

    Assert.assertNotNull(dueDate);

    dueTime = new DateTime(dueDate);

    Assert.assertEquals(dateTime.plusDays(1).hourOfDay().setCopy(11).minuteOfHour().setCopy(59).getMillis(), dueTime.getMillis());

    // HIGH PRIORITY
    priority = new Priority();
    priority.setName("high");
    priority.setNumber(2);

    dateTime = new DateTime(2015, 5, 26, 12, 0, 0, 0);

    dueDate = slaService.calculateDueTime(dateTime.toDate(), contract, priority);
    Assert.assertNotNull(dueDate);

    dueTime = new DateTime(dueDate);

    Assert.assertEquals(dateTime.plusHours(4).getMillis(), dueTime.getMillis());

    dateTime = new DateTime(2015, 5, 26, 18, 0, 0, 0);

    dueDate = slaService.calculateDueTime(dateTime.toDate(), contract, priority);
    Assert.assertNotNull(dueDate);

    dueTime = new DateTime(dueDate);

    Assert.assertEquals(dateTime.plusDays(1).hourOfDay().setCopy(13).getMillis(), dueTime.getMillis());

    // LOW PRIORITY
    priority = new Priority();
    priority.setName("low");
    priority.setNumber(4);

    dateTime = new DateTime(2015, 5, 26, 10, 0, 0, 0);

    dueDate = slaService.calculateDueTime(dateTime.toDate(), contract, priority);
    Assert.assertNotNull(dueDate);

    dueTime = new DateTime(dueDate);

    Assert.assertEquals(dateTime.plusDays(2).hourOfDay().setCopy(16).getMillis(), dueTime.getMillis());

    // HIGT PRIORITY WEEKEND
    priority = new Priority();
    priority.setName("high");
    priority.setNumber(1);

    // SUNDAY
    dateTime = new DateTime(2015, 5, 24, 10, 0, 0, 0);

    dueDate = slaService.calculateDueTime(dateTime.toDate(), contract, priority);
    Assert.assertNotNull(dueDate);

    dueTime = new DateTime(dueDate);

    Assert.assertEquals(dateTime.plusDays(1).hourOfDay().setCopy(12).getMillis(), dueTime.getMillis());

    // SATURDAY
    dateTime = new DateTime(2015, 5, 23, 10, 0, 0, 0);

    dueDate = slaService.calculateDueTime(dateTime.toDate(), contract, priority);
    Assert.assertNotNull(dueDate);

    dueTime = new DateTime(dueDate);

    Assert.assertEquals(dateTime.plusDays(2).hourOfDay().setCopy(12).getMillis(), dueTime.getMillis());
  }

  // TEST 24/7
  @Test
  public void testSla24() {

    Contract contract = new Contract();

    contract.getBusinessHours().add("00:00-23:59");
    contract.getBusinessHours().add("00:00-23:59");
    contract.getBusinessHours().add("00:00-23:59");
    contract.getBusinessHours().add("00:00-23:59");
    contract.getBusinessHours().add("00:00-23:59");

    contract.getSlas().put(1, 2);
    contract.getSlas().put(2, 4);
    contract.getSlas().put(3, 8);
    contract.getSlas().put(4, 24);

    contract = contractRepository.save(contract);

    Assert.assertNotNull(contract);

    Priority priority = new Priority();
    priority.setName("critical");
    priority.setNumber(1);
    DateTime dateTime = new DateTime(2015, 5, 26, 23, 59, 0, 0);

    Date dueDate = slaService.calculateDueTime(dateTime.toDate(), contract, priority);

    Assert.assertNotNull(dueDate);

    DateTime dueTime = new DateTime(dueDate);

    System.out.println(dueDate);
    System.out.println(dateTime.plusHours(2).toDate());
    Assert.assertEquals(dateTime.plusHours(2).getMillis(), dueTime.getMillis());

    priority = new Priority();
    priority.setName("low");
    priority.setNumber(4);
    dateTime = new DateTime(2015, 5, 26, 23, 59, 0, 0);

    dueDate = slaService.calculateDueTime(dateTime.toDate(), contract, priority);

    Assert.assertNotNull(dueDate);

    dueTime = new DateTime(dueDate);
    Assert.assertEquals(dateTime.plusDays(1).getMillis(), dueTime.getMillis());

  }

  // TEST 24/7
  @Test
  public void testSlaCustom() {

    Contract contract = new Contract();

    contract.getBusinessHours().add("10:30-19:30");
    contract.getBusinessHours().add("10:30-19:30");
    contract.getBusinessHours().add("10:30-19:30");
    contract.getBusinessHours().add("10:30-19:30");

    contract.getSlas().put(1, 2);
    contract.getSlas().put(2, 4);
    contract.getSlas().put(3, 8);
    contract.getSlas().put(4, 24);

    contract = contractRepository.save(contract);

    Assert.assertNotNull(contract);

    Priority priority = new Priority();
    priority.setName("critical");
    priority.setNumber(1);
    DateTime dateTime = new DateTime(2015, 5, 26, 19, 0, 0, 0);

    Date dueDate = slaService.calculateDueTime(dateTime.toDate(), contract, priority);

    Assert.assertNotNull(dueDate);

    DateTime dueTime = new DateTime(dueDate);
    Assert.assertEquals(dateTime.plusDays(1).hourOfDay().setCopy(12).getMillis(), dueTime.getMillis());

    priority = new Priority();
    priority.setName("low");
    priority.setNumber(4);
    dateTime = new DateTime(2015, 5, 26, 10, 30, 0, 0);

    dueDate = slaService.calculateDueTime(dateTime.toDate(), contract, priority);

    Assert.assertNotNull(dueDate);

    dueTime = new DateTime(dueDate);
    Assert.assertEquals(dateTime.plusDays(2).hourOfDay().setCopy(16).minuteOfHour().setCopy(30).getMillis(), dueTime.getMillis());

    priority = new Priority();
    priority.setName("critical");
    priority.setNumber(1);
    dateTime = new DateTime(2015, 5, 29, 10, 30, 0, 0);

    dueDate = slaService.calculateDueTime(dateTime.toDate(), contract, priority);

    Assert.assertNotNull(dueDate);

    dueTime = new DateTime(dueDate);
    Assert.assertEquals(dateTime.plusDays(3).hourOfDay().setCopy(12).minuteOfHour().setCopy(30).getMillis(), dueTime.getMillis());

  }
}
