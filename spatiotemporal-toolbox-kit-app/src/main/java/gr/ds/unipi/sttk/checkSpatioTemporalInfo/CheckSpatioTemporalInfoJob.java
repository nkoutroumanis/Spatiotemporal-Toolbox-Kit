package gr.ds.unipi.sttk.checkSpatioTemporalInfo;

public class CheckSpatioTemporalInfoJob {
    public static void main(String args[]) throws Exception {
        AppConfig config = AppConfig.newAppConfig(args[0]);
        config.getCheckSpatioTemporalInfo(config.getRecordParser(config.getDataSource()));
    }

}
