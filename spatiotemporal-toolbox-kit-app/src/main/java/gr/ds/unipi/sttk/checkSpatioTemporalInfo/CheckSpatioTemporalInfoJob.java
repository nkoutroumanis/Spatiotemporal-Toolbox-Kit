package gr.ds.unipi.sttk.checkSpatioTemporalInfo;

import com.typesafe.config.Config;
import gr.ds.unipi.stpin.Rectangle;
import gr.ds.unipi.stpin.outputs.FileOutput;
import gr.ds.unipi.stpin.parsers.RecordParser;
import gr.ds.unipi.sttk.AppConfig;

public class CheckSpatioTemporalInfoJob {
    public static void main(String args[]) throws Exception {
        AppConfig config = AppConfig.newAppConfig(args[0]);
        getCheckSpatioTemporalInfo(config.getConfig(), config.getRecordParser(config.getDataSource()));
    }

    private static void getCheckSpatioTemporalInfo(Config config, RecordParser recordParser) throws Exception {
        Config check = config.getConfig("checkSpatioTemporalInfo");
        Config filter = config.getConfig("filter");

        CheckSpatioTemporalInfo.Builder checkSpatioTemporalInfo = CheckSpatioTemporalInfo.newCheckSpatioTemporalInfo(recordParser);

        if(check.getBoolean("filter")){
            checkSpatioTemporalInfo.filter(Rectangle.newRectangle(filter.getDouble("minLon"), filter.getDouble("minLat"), filter.getDouble("maxLon"), filter.getDouble("maxLat")));
        }

        checkSpatioTemporalInfo.build().exportInfo(FileOutput.newFileOutput(check.getString("filesOutputPath"), check.getBoolean("deleteOutputDirectoryIfExists")));
    }

}
