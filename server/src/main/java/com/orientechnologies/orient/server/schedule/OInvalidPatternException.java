/*
 * cron4j - A pure Java cron-like scheduler
 * 
 * Copyright (C) 2007-2010 Carlo Pelliccia (www.sauronsoftware.it)
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License version
 * 2.1, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License 2.1 for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License version 2.1 along with this program.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.orientechnologies.orient.server.schedule;

/**
 * <p>
 * This kind of exception is thrown if an invalid scheduling pattern is encountered by the scheduler.
 * </p>
 * 
 * @author Carlo Pelliccia
 */
public class OInvalidPatternException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  /**
   * Package-reserved construction.
   */
  OInvalidPatternException() {
  }

  /**
   * Package-reserved construction.
   */
  OInvalidPatternException(String message) {
    super(message);
  }

}
