package gr.ds.unipi.sttk.checkSpatialDataInsideBox;

import gr.ds.unipi.stpin.Rectangle;
import gr.ds.unipi.stpin.datasources.Datasource;
import gr.ds.unipi.stpin.datasources.FileDatasource;
import gr.ds.unipi.stpin.outputs.FileOutput;
import gr.ds.unipi.stpin.parsers.CsvRecordParser;
import gr.ds.unipi.stpin.parsers.RecordParser;
import org.junit.Test;

public class CheckSpatialDataInsideBoxJobTest {

    @Test
    public void main() throws Exception {

        Datasource ds = FileDatasource.newFileDatasource("./src/test/resources/csv/", ".csv");
        RecordParser rp = new CsvRecordParser(ds, ";", 2, 3);
        FileOutput fileOutput = FileOutput.newFileOutput("./src/test/resources/checkSpatialDataInsideBox/", true);

        CheckSpatialDataInsideBox.newCheckSpatioTemporalInfo(rp, Rectangle.newRectangle(-106.7282958, -12.5515792, 98.1731682, 82.0)).build().exportInfo(fileOutput);

    }
}