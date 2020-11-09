package gr.ds.unipi.sttk.dbDataInsertion.redis;

import com.github.davidmoten.geo.GeoHash;
import gr.ds.unipi.stpin.Rectangle;
import gr.ds.unipi.stpin.datasources.Datasource;
import gr.ds.unipi.stpin.outputs.RedisClusterOutput;
import gr.ds.unipi.stpin.outputs.RedisInstanceOutput;
import gr.ds.unipi.stpin.outputs.RedisOutput;
import gr.ds.unipi.stpin.parsers.CsvRecordParser;
import gr.ds.unipi.stpin.parsers.Record;
import gr.ds.unipi.stpin.parsers.RecordParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.SecureRandom;
import java.text.ParseException;
import java.util.Date;
import java.util.function.Function;

public class RedisDataInsertion {

    private static final Logger logger = LoggerFactory.getLogger(RedisDataInsertion.class);
    private final RecordParser parser;
    private final Rectangle rectangle;
    private final int length;
    private final RedisOutput redisOutput;

    private RedisDataInsertion(RedisDataInsertion.Builder builder) {
        parser = builder.parser;
        rectangle = builder.rectangle;
        length = builder.length;
        redisOutput = builder.redisOutput;
    }

    public void insertDataOnRedis() throws Exception {
        Function<Record, Date> dateFunction = parser.getDateFunction();

        long startTimeWindow = System.currentTimeMillis();
        long count = 0;


        while (parser.hasNextRecord()) {

            try {
                Record record = parser.nextRecord();

                if (Datasource.empty.test(parser.getLongitude(record)) || Datasource.empty.test(parser.getLatitude(record)) || Datasource.empty.test(parser.getDate(record))) {
                    continue;
                }

                double longitude = Double.parseDouble(parser.getLongitude(record));
                double latitude = Double.parseDouble(parser.getLatitude(record));
                Date d = dateFunction.apply(record);

                if(d == null){
                    continue;
                }

                if(rectangle != null) {
                    //filtering
                    if (((Double.compare(longitude, rectangle.getMaxx()) == 1) || (Double.compare(longitude, rectangle.getMinx()) == -1)) || ((Double.compare(latitude, rectangle.getMaxy()) == 1) || (Double.compare(latitude, rectangle.getMiny()) == -1))) {
                        continue;
                    }
                }

                if(parser instanceof CsvRecordParser){
                    record.getFieldValues().set(((CsvRecordParser) parser).getDateIndex(),d);
                }


                String geoHash = GeoHash.encodeHash(latitude, longitude, this.length);
                redisOutput.out(record,geoHash+"-"+d.getTime()+"-"+randomCharacterNumericString());

                count++;

            } catch (NumberFormatException | ParseException e) {
                continue;
            }
        }

        redisOutput.close();
        logger.info("Totally {} documents have been inserted in Redis ",count);
        logger.info("Elapsed time {}", (System.currentTimeMillis() - startTimeWindow) / 1000 + " sec");

    }

    public static RedisDataInsertion.Builder newRedisDataInsertion(String host, int port, String database,int batchSize, RecordParser parser, int length, boolean isCluster) throws Exception {
        if(isCluster){
            return new RedisDataInsertion.Builder(new RedisClusterOutput(host, port, database,batchSize), parser, length);
        }
        else{
            return new RedisDataInsertion.Builder(new RedisInstanceOutput(host, port, database,batchSize), parser, length);
        }

    }

//    public static RedisDataInsertion.Builder newRedisDataInsertion(String host, int port, String database,int batchSize, RecordParser parser, int length) throws Exception {
//        return new RedisDataInsertion.Builder(new RedisOutput(host, port, database,batchSize), parser, length);
//    }
    private static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    private static SecureRandom rnd = new SecureRandom();

    private static String randomCharacterNumericString(){
        int len = 10;
        StringBuilder sb = new StringBuilder(len);
        for( int i = 0; i < len; i++ )
            sb.append( AB.charAt( rnd.nextInt(AB.length()) ) );
        return sb.toString();
    }


    public static class Builder {

        private final RecordParser parser;
        private final RedisOutput redisOutput;
        private final int length;

        private Rectangle rectangle = null; // = Rectangle.newRectangle(-180, -90, 180, 90);

        public Builder(RedisOutput redisOutput, RecordParser parser, int length) throws Exception {
            this.parser = parser;
            this.length = length;
            this.redisOutput = redisOutput;
        }

        public RedisDataInsertion.Builder filter(Rectangle rectangle) {
            this.rectangle = rectangle;
            return this;
        }

        public RedisDataInsertion build() {
            return new RedisDataInsertion(this);
        }
    }

}
