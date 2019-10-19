package gr.ds.unipi.sttk.syntheticDataset;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class SyntheticDatasetJob2 {

    private static final String path = "/mnt/sbt/synthetic-dataset2/";

    private static final double maxLongitude = 26.6041955909;
    private static final double minLongitude = 20.1500159034;

    private static final double maxLatitude = 41.8269046087;
    private static final double minLatitude = 34.9199876979;

    public static void main(String args[]) throws ParseException, FileNotFoundException {

        final SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        final Date maxDate = sd.parse("2018-06-30 23:59:59.999");
        final Date minDate = sd.parse("2017-06-29 00:00:00.000");

        double lonDiff = maxLongitude - minLongitude;
        double latDiff = maxLatitude - minLatitude;

        Random r = new Random();
        PrintWriter pw;


            pw = new PrintWriter(path + File.separator + "synthetic" + ".csv");

            for (int j = 0; j < 3448; j++) {
                pw.write((r.nextInt(899) + 100) + "_" + (r.nextInt(899) + 100) + ";" + String.format("%.6f", minLongitude + Math.random() * lonDiff) + ";" + String.format("%.6f", minLatitude + Math.random() * latDiff) + ";" + sd.format(new Date(ThreadLocalRandom.current().nextLong(minDate.getTime(), maxDate.getTime()))) + "\r\n");
            }



            pw.close();



    }
}
