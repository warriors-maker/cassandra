package org.apache.cassandra.Treas;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.utils.FBUtilities;

public class Logger
{



    private static Logger logger = new Logger();

    public static Logger getLogger() {
        return logger;
    }

    private final String absPath = "/root/cassandra/logs/";

    private  void initFile(String name){
        File file = new File(name);
        try
        {
            file.createNewFile();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public synchronized void writeStats(String action, long startTime, long endTime, String value, int opID, String tag, String key) {

        String myAddr = FBUtilities.getJustLocalAddress().toString().substring(1);
        int index = TreasConfig.getAddressMap().get(FBUtilities.getJustLocalAddress().toString().substring(1));
        String name = absPath + "treasStats" + index + ".txt";
        FileWriter writer = null;
        try
        {
            initFile(name);
            writer = new FileWriter(name,true);
        } catch  (IOException e) {
            e.printStackTrace();
        }
        BufferedWriter printWriter = new BufferedWriter (writer);
        try {
            printWriter.write(action +  ' '+ myAddr + '/' + opID + ' ' + startTime + ' ' + endTime + ' ' + tag + ' ' + key + ' ');
            printWriter.write(value +  ' ');
            printWriter.newLine();
            printWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//    public void writeReadStats(long num, long startTime, long endTime, String value, int opID) {
//        synchronized (obj2) {
//            int index = TreasConfig.getAddressMap().get(FBUtilities.getJustLocalAddress().toString().substring(1));
//            String name = absPath + "oreasReadStats" + index + ".txt";
//            FileWriter writer = null;
//            try
//            {
//                initFile(name);
//                writer = new FileWriter(name,true);
//            } catch  (IOException e) {
//                e.printStackTrace();
//            }
//            BufferedWriter printWriter = new BufferedWriter (writer);
//            try {
//                printWriter.write(num +  " " + startTime + ' ' + endTime + ' ');
//                printWriter.write(value + ' ');
//                printWriter.write(opID + ' ');
//                printWriter.newLine();
//                printWriter.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//    }


}
