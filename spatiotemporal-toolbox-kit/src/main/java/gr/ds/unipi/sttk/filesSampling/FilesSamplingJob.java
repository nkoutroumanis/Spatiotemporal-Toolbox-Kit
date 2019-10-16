package gr.ds.unipi.sttk.filesSampling;

import gr.ds.unipi.stpin.datasources.Datasource;
import gr.ds.unipi.stpin.datasources.FileDatasource;
import gr.ds.unipi.stpin.outputs.FileOutput;
import gr.ds.unipi.stpin.parsers.CsvRecordParser;
import gr.ds.unipi.stpin.parsers.RecordParser;

public class FilesSamplingJob {
    public static void main(String args[]) throws Exception {
        Datasource ds = FileDatasource.newFileDatasource("/home/nikolaos/Documents/thesis-dataset/", ".csv");
        RecordParser rp = new CsvRecordParser(ds, ";", 2, 3, 4, "yyyy-MM-dd HH:mm:ss");
        FileOutput fileOutput = FileOutput.newFileOutput("/home/nikolaos/Documents/thesis-dataset-sampling/", true);

        FilesSamping.newFilesSamping(rp, 1000000).build().exportSamplesToFile(fileOutput);
    }
}
