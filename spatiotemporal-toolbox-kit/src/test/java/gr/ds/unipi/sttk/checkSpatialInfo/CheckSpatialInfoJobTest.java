package gr.ds.unipi.sttk.checkSpatialInfo;

import gr.ds.unipi.stpin.datasources.Datasource;
import gr.ds.unipi.stpin.datasources.FileDatasource;
import gr.ds.unipi.stpin.outputs.FileOutput;
import gr.ds.unipi.stpin.parsers.CsvRecordParser;
import gr.ds.unipi.stpin.parsers.RecordParser;
import org.junit.Test;

public class CheckSpatialInfoJobTest {

    @Test
    public void main() throws Exception {


        Datasource ds = FileDatasource.newFileDatasource("./src/test/resources/csv/", ".csv");
        RecordParser rp = new CsvRecordParser(ds, ";", 2, 3);
        FileOutput fileOutput = FileOutput.newFileOutput("./src/test/resources/checkSpatialInfoJob/", true);


        CheckSpatialInfo.newCheckSpatioTemporalInfo(rp).build().exportInfo(fileOutput);

    }
}