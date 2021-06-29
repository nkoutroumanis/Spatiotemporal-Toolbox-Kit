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

public class RedisDataInsertionHilbertCurve3D {

    private static final Logger logger = LoggerFactory.getLogger(RedisDataInsertionHilbertCurve3D.class);
    private final RecordParser parser;
    private final Rectangle rectangle;
    private final RedisOutput redisOutput;

    private final int bits;
    private final long maxOrdinates;
    private final Rectangle space;
    private final Date minDate;
    private final Date maxDate;

    private RedisDataInsertionHilbertCurve3D(RedisDataInsertionHilbertCurve3D.Builder builder) {
        parser = builder.parser;
        rectangle = builder.rectangle;
        redisOutput = builder.redisOutput;

        bits = builder.bits;
        maxOrdinates = 1L << bits;
        space = builder.space;

        minDate = builder.minDate;
        maxDate = builder.maxDate;
    }

    public void insertDataOnRedis() throws Exception {
        Function<Record, Date> dateFunction = parser.getDateFunction();

        long startTimeWindow = System.currentTimeMillis();
        long count = 0;

        SmallHilbertCurve sthc = HilbertCurve.small().bits(bits).dimensions(3);

        SmallHilbertCurve sphc = HilbertCurve.small().bits(bits).dimensions(2);
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

                //forbaseline
//                redisOutput.out(record," ");

                long stIndex = sthc.index(GeoUtil.scale3DPoint(longitude,space.getMinx(),space.getMaxx(),latitude,space.getMiny(),space.getMaxy(),d.getTime(),minDate.getTime(), maxDate.getTime(), maxOrdinates));

                long spIndex = sphc.index(GeoUtil.scale2DPoint(longitude,space.getMinx(),space.getMaxx(),latitude,space.getMiny(),space.getMaxy(), maxOrdinates));

                redisOutput.out(record,String.valueOf(stIndex)+":"+String.valueOf(spIndex));

                count++;

            } catch (NumberFormatException | ParseException e) {
                continue;
            }
        }

        redisOutput.close();
        logger.info("Totally {} records have been inserted in Redis ",count);
        logger.info("Elapsed time {}", (System.currentTimeMillis() - startTimeWindow) / 1000 + " sec");

    }

    public static RedisDataInsertionHilbertCurve3D.Builder newRedisDataInsertion(String host, int port, String database, int batchSize, RecordParser parser, int bits, Rectangle space, String minDate, String maxDate, boolean isCluster) throws Exception {
        if(isCluster){
            return new RedisDataInsertionHilbertCurve3D.Builder(new RedisClusterOutput(host, port, database,batchSize), parser, bits, space, minDate, maxDate);
        }
        else{
            return new RedisDataInsertionHilbertCurve3D.Builder(new RedisInstanceOutput(host, port, database,batchSize), parser, bits, space, minDate, maxDate);
        }

    }

    public static class Builder {

        private final RecordParser parser;
        private final RedisOutput redisOutput;

        private Rectangle rectangle = null; // = Rectangle.newRectangle(-180, -90, 180, 90);

        private final int bits;
        private final Rectangle space;
        private final Date minDate;
        private final Date maxDate;

        public Builder(RedisOutput redisOutput, RecordParser parser, int bits, Rectangle space, String minDate, String maxDate) throws Exception {
            this.parser = parser;
            this.redisOutput = redisOutput;

            this.bits = bits;
            this.space = space;

            if(parser.getDateFormat().equals("unixTimestamp")){
                this.minDate = new Date(Long.valueOf(minDate));
                this.maxDate = new Date(Long.valueOf(maxDate));
            }
            else{
                DateFormat dateFormat = new SimpleDateFormat(parser.getDateFormat());

                try {
                    this.minDate = dateFormat.parse(minDate);
                    this.maxDate = dateFormat.parse(maxDate);
                } catch (ParseException e) {
                    throw new Exception("Min and Max Dates strings should follow the format of the defined dateFormat");
                }
            }

        }

        public RedisDataInsertionHilbertCurve3D.Builder filter(Rectangle rectangle) {
            this.rectangle = rectangle;
            return this;
        }

        public RedisDataInsertionHilbertCurve3D build() {
            return new RedisDataInsertionHilbertCurve3D(this);
        }
    }

}
