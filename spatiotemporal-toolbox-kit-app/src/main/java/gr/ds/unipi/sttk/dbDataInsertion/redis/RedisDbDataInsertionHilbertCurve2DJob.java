package gr.ds.unipi.sttk.dbDataInsertion.redis;

import com.typesafe.config.Config;
import gr.ds.unipi.stpin.Rectangle;
import gr.ds.unipi.stpin.parsers.RecordParser;
import gr.ds.unipi.sttk.AppConfig;

public class RedisDbDataInsertionHilbertCurve2DJob {
    public static void main(String args[]) throws Exception {
        AppConfig config = AppConfig.newAppConfig(args[0]);
        getRedisDataInsertionJob(config.getConfig(), config.getRecordParser(config.getDataSource())).insertDataOnRedis();
    }

    private static RedisDataInsertionHilbertCurve2D getRedisDataInsertionJob(Config config, RecordParser recordParser) throws Exception {
        Config redis = config.getConfig("redis");
        Config filter = config.getConfig("filter");
        Config space = redis.getConfig("space");

        RedisDataInsertionHilbertCurve2D.Builder redisDataInsertion = RedisDataInsertionHilbertCurve2D.newRedisDataInsertion(redis.getString("host"),redis.getInt("port"),redis.getString("database"), redis.getInt("batchSize"),recordParser, redis.getInt("bits"), Rectangle.newRectangle(space.getDouble("minLon"),space.getDouble("minLat"),space.getDouble("maxLon"),space.getDouble("maxLat")), redis.getBoolean("cluster"), redis.getBoolean("fieldIndexes"));
        if(redis.getBoolean("filter")){
            redisDataInsertion.filter(Rectangle.newRectangle(filter.getDouble("minLon"), filter.getDouble("minLat"), filter.getDouble("maxLon"), filter.getDouble("maxLat")));
        }
        return redisDataInsertion.build();
    }
}
