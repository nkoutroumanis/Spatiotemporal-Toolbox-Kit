package gr.ds.unipi.sttk.statistics;

import gr.ds.unipi.stpin.Rectangle;
import gr.ds.unipi.stpin.datasources.FileDatasource;
import gr.ds.unipi.stpin.outputs.FileOutput;
import gr.ds.unipi.stpin.parsers.CsvRecordParser;

import java.util.Arrays;

public final class StatisticsJob {


    public static void main(String args[]) throws Exception {

        CsvRecordParser csvRecordParser = new CsvRecordParser(FileDatasource.newFileDatasource("/home/nikolaos/Documents/zeli",".csv"), ";", 7,8,3,"yyyy-MM-dd HH:mm:ss");

        Statistics.newStatistics().filter(Rectangle.newRectangle(-180, -90, 180, 90)).build().calculateElementsFromCSVformat(csvRecordParser, Arrays.asList(26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38), FileOutput.newFileOutput("/home/nikolaos/Documents/gg",false));

    }

}
