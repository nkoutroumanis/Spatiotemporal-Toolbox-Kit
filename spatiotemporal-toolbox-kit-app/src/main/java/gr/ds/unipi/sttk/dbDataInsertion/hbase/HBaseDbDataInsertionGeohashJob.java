package gr.ds.unipi.sttk.dbDataInsertion.hbase;

import com.typesafe.config.Config;
import gr.ds.unipi.stpin.Rectangle;
import gr.ds.unipi.stpin.parsers.RecordParser;
import gr.ds.unipi.sttk.AppConfig;

public class HBaseDbDataInsertionGeohashJob {
    public static void main(String args[]) throws Exception {
        AppConfig config = AppConfig.newAppConfig(args[0]);
        getHBaseDataInsertionJob(config.getConfig(), config.getRecordParser(config.getDataSource())).insertDatainHBase();
    }

    private static HBaseDataInsertion getHBaseDataInsertionJob(Config config, RecordParser recordParser) throws Exception {
        Config hbase = config.getConfig("hbase");
        Config filter = config.getConfig("filter");
        HBaseDataInsertion.Builder hbaseDbDataInsertion = HBaseDataInsertion.newHBaseDataInsertion(hbase.getString("host"),hbase.getString("table"), hbase.getInt("batchSize"),recordParser, hbase.getInt("length"));
        if(hbase.getBoolean("filter")){
            hbaseDbDataInsertion.filter(Rectangle.newRectangle(filter.getDouble("minLon"), filter.getDouble("minLat"), filter.getDouble("maxLon"), filter.getDouble("maxLat")));
        }
        return hbaseDbDataInsertion.build();
    }
}
