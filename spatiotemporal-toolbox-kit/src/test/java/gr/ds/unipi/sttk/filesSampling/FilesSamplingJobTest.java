package gr.ds.unipi.sttk.filesSampling;

import gr.ds.unipi.stpin.datasources.Datasource;
import gr.ds.unipi.stpin.datasources.FileDatasource;
import gr.ds.unipi.stpin.outputs.FileOutput;
import gr.ds.unipi.stpin.parsers.CsvRecordParser;
import gr.ds.unipi.stpin.parsers.RecordParser;
import org.junit.Test;

import java.util.Random;

public class FilesSamplingJobTest {

    @Test
    public void main() throws Exception {

        Datasource ds = FileDatasource.newFileDatasource("./src/test/resources/csv/", ".csv");
        RecordParser rp = new CsvRecordParser(ds, ";", 2, 3, 4, "yyyy-MM-dd HH:mm:ss");
        FileOutput fileOutput = FileOutput.newFileOutput("./src/test/resources/sampling/", true);

        FilesSamping.newFilesSamping(rp, 10).build().exportSamplesToFile(fileOutput);
    }

    @Test
    public void randomNumbers() {
        Random r = new Random();
        long generatedLong = 1 + (long) (Math.random() * (10 - 1));
        System.out.println(generatedLong);
    }


}