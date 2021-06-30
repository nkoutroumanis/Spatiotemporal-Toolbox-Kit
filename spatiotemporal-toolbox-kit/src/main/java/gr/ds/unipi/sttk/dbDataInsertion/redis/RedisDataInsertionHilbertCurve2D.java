package gr.ds.unipi.sttk.dbDataInsertion.redis;

import gr.ds.unipi.stpin.Rectangle;
import gr.ds.unipi.stpin.datasources.Datasource;
import gr.ds.unipi.stpin.outputs.RedisClusterOutput;
import gr.ds.unipi.stpin.outputs.RedisInstanceOutput;
import gr.ds.unipi.stpin.outputs.RedisOutput;
import gr.ds.unipi.stpin.parsers.CsvRecordParser;
import gr.ds.unipi.stpin.parsers.Record;
import gr.ds.unipi.stpin.parsers.RecordParser;
import gr.ds.unipi.sttk.dbDataInsertion.GeoUtil;
import org.davidmoten.hilbert.HilbertCurve;
import org.davidmoten.hilbert.SmallHilbertCurve;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.function.Function;

import static java.lang.Math.pow;

public class RedisDataInsertionHilbertCurve2D {

    private static final Logger logger = LoggerFactory.getLogger(RedisDataInsertionHilbertCurve2D.class);
    private final RecordParser parser;
    private final Rectangle rectangle;
    private final RedisOutput redisOutput;

    private final int bits;
    private final long maxOrdinates;
    private final Rectangle space;

    private RedisDataInsertionHilbertCurve2D(RedisDataInsertionHilbertCurve2D.Builder builder) {
        parser = builder.parser;
        rectangle = builder.rectangle;
        redisOutput = builder.redisOutput;

        bits = builder.bits;
        maxOrdinates = 1L << bits;
        space = builder.space;
    }

    public void insertDataOnRedis() throws Exception {
        Function<Record, Date> dateFunction = parser.getDateFunction();

        long startTimeWindow = System.currentTimeMillis();
        long count = 0;

        SmallHilbertCurve hc = HilbertCurve.small().bits(bits).dimensions(2);
        //double digits = pow(2, bits*2);
        //int length = String.valueOf((long) digits).length();

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

                //forbaseline
//                redisOutput.out(record," ");

                long index = hc.index(GeoUtil.scale2DPoint(longitude,space.getMinx(),space.getMaxx(),latitude,space.getMiny(),space.getMaxy(), maxOrdinates));
                redisOutput.out(record,String.valueOf(index)+":");

                count++;

            } catch (NumberFormatException | ParseException e) {
                continue;
            }
        }

        redisOutput.close();
        logger.info("Totally {} records have been inserted in Redis ",count);
        logger.info("Elapsed time {}", (System.currentTimeMillis() - startTimeWindow) / 1000 + " sec");
    }

    public static RedisDataInsertionHilbertCurve2D.Builder newRedisDataInsertion(String host, int port, String database, int batchSize, RecordParser parser, int bits, Rectangle space, boolean isCluster, boolean indexes) throws Exception {
        if(isCluster){
            return new RedisDataInsertionHilbertCurve2D.Builder(new RedisClusterOutput(host, port, database,batchSize, indexes,true,false), parser, bits, space);
        }
        else{
            return new RedisDataInsertionHilbertCurve2D.Builder(new RedisInstanceOutput(host, port, database,batchSize, indexes,true,false), parser, bits, space);
        }
    }

    public static class Builder {

        private final RecordParser parser;
        private final RedisOutput redisOutput;

        private Rectangle rectangle = null; // = Rectangle.newRectangle(-180, -90, 180, 90);

        private final int bits;
        private final Rectangle space;
        private final boolean indexes = false;

        public Builder(RedisOutput redisOutput, RecordParser parser, int bits, Rectangle space) throws Exception {
            this.parser = parser;
            this.redisOutput = redisOutput;

            this.bits = bits;
            this.space = space;
        }

        public RedisDataInsertionHilbertCurve2D.Builder filter(Rectangle rectangle) {
            this.rectangle = rectangle;
            return this;
        }

        public RedisDataInsertionHilbertCurve2D build() {
            return new RedisDataInsertionHilbertCurve2D(this);
        }
    }

}
