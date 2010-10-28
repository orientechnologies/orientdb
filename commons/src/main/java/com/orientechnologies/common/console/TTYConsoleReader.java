package com.orientechnologies.common.console;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TTYConsoleReader implements OConsoleReader
{
    protected List<String> history = new ArrayList<String>();

    public String readLine()
    {
        String consoleInput = "";
        try
        {
            StringBuffer buffer = new StringBuffer();
            int historyNum = history.size();
            while (true)
            {

                boolean escape = false;
                int next = System.in.read();
                if (next == 27)
                {
                    escape = true;
                    System.in.read();
                    next = System.in.read();
                }

                if (escape)
                {
                    if (next == 65 && !history.isEmpty())
                    {
                        if (history.size() > 0)
                        { // UP
                            StringBuffer cleaner = new StringBuffer();
                            for(int i=0;i<buffer.length(); i++){
                                cleaner.append(" ");
                            }
                            rewriteConsole(cleaner);
                            historyNum = historyNum > 0 ? historyNum - 1 : 0;
                            buffer = new StringBuffer(history.get(historyNum));
                            rewriteConsole(buffer);
//                            writeHistory(historyNum);
                        }
                    }
                    if (next == 66 && !history.isEmpty())
                    { // DOWN
                        if (history.size() > 0)
                        {
                            StringBuffer cleaner = new StringBuffer();
                            for(int i=0;i<buffer.length(); i++){
                                cleaner.append(" ");
                            }
                            rewriteConsole(cleaner);
                            
                            historyNum = historyNum < history.size() ? historyNum + 1 : history.size();
                            if (historyNum == history.size())
                            {
                                buffer = new StringBuffer("");
                            }
                            else
                            {
                                buffer = new StringBuffer(history.get(historyNum));
                            }
                            rewriteConsole(buffer);
//                            writeHistory(historyNum);
                        }
                    }
                    else
                    {
                    }
                }
                else
                {
                    if (next == 10)
                    {
                        System.out.println();
                        break;
                    }
                    else if (next == 127)
                    {
                        if (buffer.length() > 0)
                        {
                            buffer.deleteCharAt(buffer.length() - 1);
                            StringBuffer cleaner = new StringBuffer(buffer);
                            cleaner.append(" ");
                            rewriteConsole(cleaner);
                            rewriteConsole(buffer);
                        }
                    }
                    else
                    {
                        if (next > 31 && next < 127)
                        {
                            System.out.print((char) next);
                            buffer.append((char) next);
                        }
                        else
                        {
                            System.out.println();
                            System.out.print(buffer);
                        }
                    }
                    historyNum = history.size();
                }
            }
            consoleInput = buffer.toString();
            history.remove(consoleInput);
            history.add(consoleInput);
            historyNum = history.size();
        }
        catch (IOException e)
        {
            return null;
        }
        return consoleInput;
    }

    private void writeHistory(int historyNum)
    {
        for (int i = 0; i < 30; i++)
        {
            System.out.println();
        }
        if (historyNum == history.size())
        {
            System.out.print("> ");
            return;
        }
        for (int i = 0; i < history.size(); i++)
        {
            System.out.print(historyNum == i ? "-> " : "   ");
            System.out.println(history.get(i));
        }
        System.out.println();
        System.out.print("> " + history.get(historyNum));
    }

    private void rewriteConsole(StringBuffer buffer)
    {
        System.out.print("\r");
        System.out.print("> ");
        System.out.print(buffer);

    }

}
