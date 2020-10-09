package gr.ds.unipi.sttk.dbDataInsertion.redis;

import com.typesafe.config.Config;
import gr.ds.unipi.stpin.Rectangle;
import gr.ds.unipi.stpin.parsers.RecordParser;
import gr.ds.unipi.sttk.AppConfig;

public class RedisDbDataInsertionGeohashJob {
    public static void main(String args[]) throws Exception {
        AppConfig config = AppConfig.newAppConfig(args[0]);
        getRedisDataInsertionJob(config.getConfig(), config.getRecordParser(config.getDataSource())).insertDataOnRedis();
    }


    private static RedisDataInsertion getRedisDataInsertionJob(Config config, RecordParser recordParser) throws Exception {
        Config redis = config.getConfig("redis");
        Config filter = config.getConfig("filter");
        RedisDataInsertion.Builder mongoDbDataInsertion = RedisDataInsertion.newRedisDataInsertion(redis.getString("host"),redis.getInt("port"),redis.getString("database"), redis.getInt("batchSize"),recordParser, redis.getInt("length"));
        if(redis.getBoolean("filter")){
            mongoDbDataInsertion.filter(Rectangle.newRectangle(filter.getDouble("minLon"), filter.getDouble("minLat"), filter.getDouble("maxLon"), filter.getDouble("maxLat")));
        }
        return mongoDbDataInsertion.build();
    }
}