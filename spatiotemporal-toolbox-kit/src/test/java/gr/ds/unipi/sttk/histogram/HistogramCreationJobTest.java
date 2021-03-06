package gr.ds.unipi.sttk.histogram;

import gr.ds.unipi.stpin.Rectangle;
import gr.ds.unipi.stpin.datasources.Datasource;
import gr.ds.unipi.stpin.datasources.FileDatasource;
import gr.ds.unipi.stpin.outputs.FileOutput;
import gr.ds.unipi.stpin.parsers.CsvRecordParser;
import gr.ds.unipi.stpin.parsers.RecordParser;
import org.junit.Test;

public class HistogramCreationJobTest {

    @Test
    public void main() throws Exception {

//        //x for longitude, y for latitude - the max values of lon and lat should be increased a little in order to include the whole data in histogram
//        Rectangle space = Rectangle.newSpace2D(-26.64, 0, 121.57, 59.94);
//        //Rectangle space = Rectangle.newSpace2D(20.1500159034, 34.9199876979, 26.61, 41.83);
//        long t1;
//        int j = 1;
//        for (long i = 1000; i <= 10000; i = i + 1000) {
//            t1 = System.currentTimeMillis();
//
//            GridPartition.newGridPartition(space, i, i, "/home/nikolaos/Documents/tambak", 2, 3, 4).build().exportHistogram("/home/nikolaos/Desktop/greek-hist/" + j);
//
//            System.out.println(j + " folder " + ((System.currentTimeMillis() - t1) / 1000) + "sec");
//            j++;
//            System.out.println("------------------");
//
//        }
//    }

//        long t1;
//        int j = 1;
//            t1 = System.currentTimeMillis();
//            GridPartition.newGridPartition(space, 4000000, 4000000, "/home/nikolaos/Documents/tambak", 2, 3, 4).build().exportHistogram("/home/nikolaos/Desktop/large/"+j);
//            System.out.println(j + " folder " + ((System.currentTimeMillis()-t1)/1000) + "sec");
//            j++;


        //x for longitude, y for latitude - the max values of lon and lat should be increased a little in order to include the whole data in histogram
        Rectangle space = Rectangle.newRectangle(20.15, 34.91, 26.61, 41.83);
        //Rectangle space = Rectangle.newSpace2D(20.1500159034, 34.9199876979, 26.604196, 41.826905);
        //Rectangle space = Rectangle.newSpace2D(20, 34.9199876979, 27, 41.826905);

        Datasource ds = FileDatasource.newFileDatasource("./src/test/resources/csv/", ".csv");
        RecordParser rp = new CsvRecordParser(ds, ";", 2, 3, 4, "yyyy-MM-dd HH:mm:ss");
        FileOutput fileOutput = FileOutput.newFileOutput("./src/test/resources/csv-enriched/", true);

        GridPartition.newGridPartition(rp, space, 300, 300).build().exportHistogram("./src/test/resources/histogram/");
    }
}