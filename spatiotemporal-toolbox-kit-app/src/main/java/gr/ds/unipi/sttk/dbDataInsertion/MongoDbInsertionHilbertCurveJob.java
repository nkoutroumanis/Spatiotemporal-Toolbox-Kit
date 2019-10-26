package gr.ds.unipi.sttk.dbDataInsertion;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import gr.ds.unipi.stpin.Rectangle;
import gr.ds.unipi.stpin.datasources.Datasource;
import gr.ds.unipi.stpin.datasources.FileDatasource;
import gr.ds.unipi.stpin.datasources.KafkaDatasource;
import gr.ds.unipi.stpin.parsers.CsvRecordParser;
import gr.ds.unipi.stpin.parsers.JsonRecordParser;
import gr.ds.unipi.stpin.parsers.RecordParser;
import gr.ds.unipi.sttk.AppConfig;

import java.io.File;

public final class MongoDbInsertionHilbertCurveJob {

    public static void main(String args[]) throws Exception {

        AppConfig config = AppConfig.newAppConfig(args[0]);
        getMongoDbInsertionHilbertCurveJob(config.getConfig(), config.getRecordParser(config.getDataSource())).insertDataOnCollection();

    }


    private static MongoDbDataInsertionHilbertCurve getMongoDbInsertionHilbertCurveJob(Config config, RecordParser recordParser) throws Exception {
        Config mongodb = config.getConfig("mongodb");
        Config space = config.getConfig("space");
        Config filter = config.getConfig("filter");
        MongoDbDataInsertionHilbertCurve.Builder mongoDbDataInsertion = MongoDbDataInsertionHilbertCurve.newMongoDbDataInsertionHilbertCurve(mongodb.getString("host"),mongodb.getInt("port"),mongodb.getString("database"),mongodb.getString("username"),mongodb.getString("password") ,mongodb.getString("collection"), mongodb.getInt("batchSize"),recordParser, mongodb.getInt("bits"), Rectangle.newRectangle(space.getDouble("minLon"),space.getDouble("minLat"),space.getDouble("maxLon"),space.getDouble("maxLat")),mongodb.getLong("minDate"), mongodb.getLong("maxDate"));
        if(mongodb.getBoolean("filter")){
            mongoDbDataInsertion.filter(Rectangle.newRectangle(filter.getDouble("minLon"), filter.getDouble("minLat"), filter.getDouble("maxLon"), filter.getDouble("maxLat")));
        }
        return mongoDbDataInsertion.build();
    }

}
