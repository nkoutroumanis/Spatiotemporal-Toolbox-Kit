package gr.ds.unipi.sttk.statistics;

import gr.ds.unipi.stpin.Rectangle;
import gr.ds.unipi.stpin.datasources.FileDatasource;
import gr.ds.unipi.stpin.outputs.FileOutput;
import gr.ds.unipi.stpin.parsers.CsvRecordParser;

import java.util.Arrays;

public final class StatisticsJob {


    public static void main(String args[]) throws Exception {

        CsvRecordParser csvRecordParser = new CsvRecordParser(FileDatasource.newFileDatasource("/mnt/sdb/vfi2-enriched/",".csv"), ";", 7,8,3,"yyyy-MM-dd'T'HH:mm:ss.SSS");

        Statistics.newStatistics().filter(Rectangle.newRectangle(-180, -90, 180, 90)).build().calculateElementsFromCSVformat(csvRecordParser, Arrays.asList(49,50,51,52,53,54,55,56,57,58,59,60,61), FileOutput.newFileOutput("/home/nikolaos/Documents/statistikaNick/",false));

    }

}
