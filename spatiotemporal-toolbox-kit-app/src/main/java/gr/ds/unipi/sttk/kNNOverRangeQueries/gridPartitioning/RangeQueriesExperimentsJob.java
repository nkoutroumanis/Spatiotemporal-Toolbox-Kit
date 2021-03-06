package gr.ds.unipi.sttk.kNNOverRangeQueries.gridPartitioning;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import gr.ds.unipi.sttk.FilesParse;
import org.bson.Document;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RangeQueriesExperimentsJob {
    public static void main(String args[]) throws IOException {

        MongoCredential credential = MongoCredential.createCredential("myUserAdmin", "test", "abc123".toCharArray());//admin for global

        MongoClientSettings clientSettings = MongoClientSettings.builder().credential(credential).applyToClusterSettings(builder ->
                builder.hosts(Arrays.asList(new ServerAddress("83.212.102.163", 28017))))
                .applyToConnectionPoolSettings(builder -> builder.maxConnectionIdleTime(90000000, TimeUnit.SECONDS)).build();

        MongoClient mongoClient = MongoClients.create(clientSettings);
        MongoCollection m = mongoClient.getDatabase("test").getCollection("geoPoints");
        Random r = new Random();


        final long xNumberOfCells = Long.valueOf(args[1]);
        final long yNumberOfCells = Long.valueOf(args[2]);

        final double maxx = 121.57;
        final double minx = -26.64;
        final double maxy = 59.94;
        final double miny = 0.0;

        final String exportPath = args[3];
        final String filesPath = args[0];
        final String filesExtension = ".csv";
        final String separator = ";";
        final int numberOfColumnLongitude = 2;
        final int numberOfColumnLatitude = 3;
        final int numberOfColumnDate = 4;


        final double radius = Math.sqrt(Math.pow(((maxx - minx) / xNumberOfCells), 2) + Math.pow(((maxy - miny) / yNumberOfCells), 2));
        System.out.println("formed radius " + radius);

        List<Path> files = Files.walk(Paths.get(filesPath)).filter(path -> path.getFileName().toString().endsWith(filesExtension)).collect(Collectors.toList());
        final int numberOfFiles = files.size();

        System.out.println("files path loaded");

        Stream.of(0.25d, 0.5d, 1d, 1.5d, 1.75d).forEach(percentage -> {

            Stream.of(0).forEach(dh -> {

                int points = 1000;

                List<Integer> results = new ArrayList<>();

                for (int i = 0; i < points; i++) {


                    double longitude = -1000;
                    double latitude = -1000;

                    int b = 0;
                    while (b == 0) {

                        try {
                            int randomFile = r.nextInt(numberOfFiles);
                            List<String> lines = Files.lines(files.get(randomFile)).collect(Collectors.toList());
                            int randomLine = r.nextInt(lines.size());
                            String line = lines.get(randomLine);

                            String[] separatedLine = line.split(separator);

                            if (FilesParse.empty.test(separatedLine[numberOfColumnLongitude - 1]) || FilesParse.empty.test(separatedLine[numberOfColumnLatitude - 1]) || FilesParse.empty.test(separatedLine[numberOfColumnDate - 1])) {
                                continue;
                            }

                            longitude = Double.parseDouble(separatedLine[numberOfColumnLongitude - 1]);
                            latitude = Double.parseDouble(separatedLine[numberOfColumnLatitude - 1]);

                            if (FilesParse.longitudeInGreekRegion.test(longitude) && FilesParse.latitudeInGreekRegion.test(latitude)) {
                                b = 1;
                            }

                        } catch (ArrayIndexOutOfBoundsException | IOException e) {
                            System.out.println("Exeption while generating random point ");
                            System.out.println(e);
                            System.out.println("Repeating the generation");
                        }
                    }

                    double randomX;
                    double randomY;

                    if (r.nextInt(2) == 1) {
                        randomX = (longitude + dh);
                    } else {
                        randomX = (longitude - dh);
                    }

                    if (r.nextInt(2) == 1) {
                        randomY = (latitude + dh);
                    } else {
                        randomY = (latitude - dh);
                    }

                    double km = (FilesParse.harvesine(randomX, randomY, randomX + (radius * percentage), randomY) + FilesParse.harvesine(randomX, randomY, randomX, randomY + (radius * percentage))) / 2;
                    System.out.println("formed km " + km);

//        if(r.nextInt(1)==1){
//            randomX = (longitude - dh) + ((longitude + dh) - (longitude - dh)) * r.nextDouble();
//        }
//        else{
//            randomX = (longitude - dh) + ((longitude + dh) - (longitude - dh)) * r.nextDouble();
//        }
//
//        if(r.nextInt(1)==1){
//            randomY = (latitude - dh) + ((latitude + dh) - (latitude - dh)) * r.nextDouble();
//        }
//        else{
//            randomY = (latitude - dh) + ((latitude + dh) - (latitude - dh)) * r.nextDouble();
//        }


                    //System.out.println("Point: " + randomX + "  " + randomY);


                    //System.out.println("Radius: " + determinedRadius);

                    MongoCursor<Document> cursor1 = m.aggregate(Arrays.asList(Document.parse("{ $match: { location: { $geoWithin : { $centerSphere : [ [" + randomX + ", " + randomY + "], " + (km / 6378.1) + " ] } } } }"), Document.parse("{ $count: \"count\" }"))).iterator();
                    results.add(cursor1.next().getInteger("count"));//(n' - n)/n
                    cursor1.close();
                    //System.out.println(resultsRatio.size() + " Count Finished");


                }

                IntSummaryStatistics ress = results.stream().mapToInt(Integer::valueOf).summaryStatistics();
                double resum = 0;
                for (Integer e : results) {
                    resum = resum + Math.pow(e - ress.getAverage(), 2);
                }
                double ressStd = Math.sqrt(resum / (results.size() - 1));


                try {
                    Files.createDirectories(Paths.get(exportPath + File.separator + xNumberOfCells + "_" + yNumberOfCells));
                } catch (IOException e) {
                    e.printStackTrace();
                }


                try (FileOutputStream fos = new FileOutputStream(exportPath + File.separator + xNumberOfCells + "_" + yNumberOfCells + File.separator + "Percentage_" + percentage + ".txt", true);
                     OutputStreamWriter osw = new OutputStreamWriter(fos, "utf-8"); BufferedWriter bw = new BufferedWriter(osw); PrintWriter pw = new PrintWriter(bw, true)) {

                    pw.write("For the histogram with the following splits;" + "\r\n");
                    pw.write("x-axis " + xNumberOfCells + "\r\n");
                    pw.write("y-axis " + yNumberOfCells + "\r\n");
                    pw.write("The radius is " + radius + " (geographical grids)" + "\r\n");
                    pw.write("\r\n");
                    pw.write("For the " + percentage * 100 + "% " + " of the radius, the following were resulted for the queries;" + "\r\n");

                    pw.write("\r\n");

                    pw.write("Average Results: " + ress.getAverage() + "\r\n");
                    pw.write("Max Results: " + ress.getMax() + "\r\n");
                    pw.write("Min Results: " + ress.getMin() + "\r\n");
                    pw.write("Std of Ratio: " + ressStd + "\r\n");
                    pw.write("\r\n");

                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            });
        });

        mongoClient.close();
    }

}
