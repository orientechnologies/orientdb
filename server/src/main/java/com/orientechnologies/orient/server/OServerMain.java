/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.server;

import com.orientechnologies.orient.core.security.OSecurityManager;
import java.util.Random;
import java.security.SecureRandom;

public class OServerMain {
	protected static OServer	server;

	public static OServer create() throws Exception {
		server = new OServer();
		return server;
	}

	public static OServer server() {
		return server;
	}

	public static void main(final String[] args) throws Exception {

        if (args.length > 0)
        {
            String opt = args[0].trim();

            if (opt.startsWith("-p"))
            {
                System.out.println("OrientDB server passphrase generator");
                System.out.print("Enter passphrase  : ");
                char[] chars1 = System.console().readPassword();
                String input1 = new String(chars1);
                System.out.print("Repeat passphrase : ");
                                 
                char[] chars2 = System.console().readPassword();
                String input2 = new String(chars2);

                if (input1 == null || input2 == null)
                {
                    System.out.println("No input received.");
                    System.exit(1);
                }
                else if (!input1.equals(input2))
                {
                    System.out.println("Phrases do not match, try again.");
                    System.exit(1);
                }

                System.out.println
                (
                    "The hash is       : "
                  + OSecurityManager.instance().digest2String(input1)
                );
            }
            else if (opt.startsWith("-s"))
            {
                Random r = new SecureRandom();
                byte[] salt = new byte[20];
                r.nextBytes(salt);
                System.out.println
                (
                    "Random string to paste into salt value of config xml: "
                  + OSecurityManager.instance().byteArrayToHexStr(salt)
                );
            }
            else help();

            System.exit(0);
        }

		OServerMain.create().startup();

        String correct = server().getUser("root").password;
        // Default hash of "root"
        String defaultPhrase =
              "4813494D137E1631BBA301D5ACAB6E7B"
            + "B7AA74CE1185D456565EF51D737677B2";

        if (correct.equals(defaultPhrase))
        {
            String linesep = System.getProperty("line.separator");
            System.out.println
            (
                linesep + linesep
              + "Passphrase has not been changed from default."
              + linesep
              + "Restart server with -p switch to set new passphrase."
              + linesep
              + "A longer phrase with alphanumerics enhances security."
            );
            System.exit(1);
        }

        requestRootPassPhrase();
        // The string input could now be retained as a
        // key for data encryption

		server().activate();
	}

    public static void requestRootPassPhrase()
    throws Exception
    {
        System.out.println();
        System.out.print("Enter root passphrase: ");
        char[] chars = System.console().readPassword();
        String input = new String(chars);

        if (grantAuthority("root", input))
        {
            System.out.println("Passphrase OK.");
        }
        else
        {
            System.out.println("Passphrase incorrect.");
            System.exit(1);
        }
    }

    /**
     * Grant authority by comparing the stored hash of the correct password
     * against the given string.
     * 
     * @param iUserName
     *          Username to authenticate
     * @param iPassword
     *          Password in clear
     * @return true if authentication is ok, otherwise false
     */
    public static boolean grantAuthority
    (
        String iUserName,
        String iPassword
    )
    throws Exception
    {
        if (server() == null) create();
        String correct = server().getUser(iUserName).password;
        String hashed =
            OSecurityManager
            .instance()
            .digest2String
            (
                iPassword
            );
        if (hashed.equals(correct)) return true;
        return false;
    }

    private static void help()
    {
        System.out.println("OrientDB usage");
        System.out.println(" orientdb.sh [opt]");
        System.out.println("-h  Usage guide");
        System.out.println("-p  Generate passphrase hash");
        System.out.println("-s  Generate a random crytographic salt");
        System.out.println();
    }
}
