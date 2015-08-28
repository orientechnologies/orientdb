/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *  
 */

package com.orientechnologies.orient.core;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.util.Hashtable;
import java.util.Map.Entry;

@SuppressWarnings("restriction")
public class OSignalHandler implements SignalHandler {
  private Hashtable<Signal, SignalHandler> redefinedHandlers = new Hashtable(4);

  public OSignalHandler() {
  }

  public void listenTo(final String name, final SignalHandler iListener) {
    Signal signal = new Signal(name);
    SignalHandler redefinedHandler = Signal.handle(signal, iListener);
    if (redefinedHandler != null) {
      redefinedHandlers.put(signal, redefinedHandler);
    }
  }

  public void handle(Signal signal) {
    OLogManager.instance().info(this, "Received signal: %s", signal);

    final String s = signal.toString().trim();

    if (Orient.instance().isSelfManagedShutdown()
        && (s.equals("SIGKILL") || s.equals("SIGHUP") || s.equals("SIGINT") || s.equals("SIGTERM"))) {
      Orient.instance().shutdown();
      System.exit(1);
    } else if (s.equals("SIGTRAP")) {
      System.out.println();
      OGlobalConfiguration.dumpConfiguration(System.out);
      System.out.println();
      Orient.instance().getProfiler().dump(System.out);
      System.out.println();
    } else {
      SignalHandler redefinedHandler = redefinedHandlers.get(signal);
      if (redefinedHandler != null) {
        redefinedHandler.handle(signal);
      }
    }
  }

  public void installDefaultSignals() {
    installDefaultSignals(this);
  }

  public void installDefaultSignals(final SignalHandler iListener) {
    // listenTo("HUP", iListener); // DISABLED HUB BECAUSE ON WINDOWS IT'S USED INTERNALLY AND CAUSED JVM KILL
    // listenTo("KILL",iListener);

    try {
      listenTo("INT", iListener);
    } catch (IllegalArgumentException e) {
      // NOT AVAILABLE
    }
    try {
      listenTo("TERM", iListener);
    } catch (IllegalArgumentException e) {
      // NOT AVAILABLE
    }
    try {
      listenTo("TRAP", iListener);
    } catch (IllegalArgumentException e) {
      // NOT AVAILABLE
    }
  }

  public void cancel() {
    for (Entry<Signal, SignalHandler> entry : redefinedHandlers.entrySet()) {
      try {
        // re-install the original handler we replaced
        Signal.handle(entry.getKey(), entry.getValue());
      } catch (IllegalStateException e) {
        // not expected as we were able to redefine it earlier, but just in case
      }
    }
    redefinedHandlers.clear();
  }
}
