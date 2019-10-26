package gr.ds.unipi.sttk.dbDataInsertion;

import com.typesafe.config.Config;
import gr.ds.unipi.stpin.Rectangle;
import gr.ds.unipi.stpin.parsers.RecordParser;
import gr.ds.unipi.sttk.AppConfig;

public final class MongoDbDataInsertionJob {

    public static void main(String args[]) throws Exception {


//        MongoDbConnector connector1 = MongoDbConnector.newMongoDbConnector("localhost", 27017, "real", "real", "real");
//        long t1 = System.currentTimeMillis();
//        MongoDbDataInsertion.newMongoDbDataInsertion(connector1, FileDatasource.newFileDatasource("/home/nikolaos/Documents/thesis-dataset/", ".csv"), 2, 3, 4, "yyyy-MM-dd HH:mm:ss").build().insertDataOnCollection("geoPoints");
//        System.out.println("real: " + (System.currentTimeMillis() - t1) / 1000);
//
//        MongoDbConnector connector2 = MongoDbConnector.newMongoDbConnector("localhost", 27017, "synthetic1", "synthetic1", "synthetic1");
//        long t2 = System.currentTimeMillis();
//        MongoDbDataInsertion.newMongoDbDataInsertion(connector2, FileDatasource.newFileDatasource("/home/nikolaos/Documents/synthetic-dataset1/", ".csv"), 2, 3, 4, "yyyy-MM-dd HH:mm:ss").build().insertDataOnCollection("geoPoints");
//        System.out.println("syn1: " + (System.currentTimeMillis() - t2) / 1000);
//
//        MongoDbConnector connector3 = MongoDbConnector.newMongoDbConnector("localhost", 27017, "synthetic2", "synthetic2", "synthetic2");
//        long t3 = System.currentTimeMillis();
//        MongoDbDataInsertion.newMongoDbDataInsertion(connector3, FileDatasource.newFileDatasource("/home/nikolaos/Documents/synthetic-dataset2/", ".csv"), 2, 3, 4, "yyyy-MM-dd HH:mm:ss").build().insertDataOnCollection("geoPoints");
//        System.out.println("syn2: " + (System.currentTimeMillis() - t3) / 1000);

        AppConfig config = AppConfig.newAppConfig(args[0]);
        getMongoDbInsertion(config.getConfig(), config.getRecordParser(config.getDataSource())).insertDataOnCollection();

    }

    public static MongoDbDataInsertion getMongoDbInsertion(Config config, RecordParser recordParser) throws Exception {
        Config mongodb = config.getConfig("mongodb");
        Config filter = config.getConfig("filter");
        MongoDbDataInsertion.Builder mongoDbDataInsertion = MongoDbDataInsertion.newMongoDbDataInsertion(mongodb.getString("host"),mongodb.getInt("port"),mongodb.getString("database"),mongodb.getString("username"),mongodb.getString("password") ,mongodb.getString("collection"), mongodb.getInt("batchSize"),recordParser);
        if(mongodb.getBoolean("filter")){
            mongoDbDataInsertion.filter(Rectangle.newRectangle(filter.getDouble("minLon"), filter.getDouble("minLat"), filter.getDouble("maxLon"), filter.getDouble("maxLat")));
        }
        return mongoDbDataInsertion.build();
    }

}
