package gr.ds.unipi.sttk.dbDataInsertion.hbase;

import com.github.davidmoten.geo.GeoHash;
import gr.ds.unipi.stpin.Rectangle;
import gr.ds.unipi.stpin.datasources.Datasource;
import gr.ds.unipi.stpin.outputs.HBaseOutput;
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

public class HBaseDataInsertion {

    private static final Logger logger = LoggerFactory.getLogger(HBaseDataInsertion.class);
    private final RecordParser parser;
    private final Rectangle rectangle;
    private final int length;
    private final HBaseOutput hbaseOutput;

    private HBaseDataInsertion(HBaseDataInsertion.Builder builder) {
        parser = builder.parser;
        rectangle = builder.rectangle;
        length = builder.length;
        hbaseOutput = builder.hbaseOutput;
    }

    public void insertDatainHBase() throws IOException {
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
                hbaseOutput.out(record,geoHash+"-"+d.getTime()+"-"+randomCharacterNumericString());

                count++;

            } catch (NumberFormatException | ParseException e) {
                continue;
            }
        }

        logger.info("Totally {} documents have been inserted in HBase",count);
        logger.info("Elapsed time {}", (System.currentTimeMillis() - startTimeWindow) / 1000 + " sec");
        hbaseOutput.close();
    }

    public static HBaseDataInsertion.Builder newHBaseDataInsertion(String host, String table, int batchSize, RecordParser parser, int length) throws Exception {
        return new HBaseDataInsertion.Builder(new HBaseOutput(host, table, batchSize), parser, length);
    }

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
        private final HBaseOutput hbaseOutput;
        private final int length;

        private Rectangle rectangle = null; // = Rectangle.newRectangle(-180, -90, 180, 90);

        public Builder(HBaseOutput hbaseOutput, RecordParser parser, int length) throws Exception {
            this.parser = parser;
            this.length = length;
            this.hbaseOutput = hbaseOutput;
        }

        public HBaseDataInsertion.Builder filter(Rectangle rectangle) {
            this.rectangle = rectangle;
            return this;
        }

        public HBaseDataInsertion build() {
            return new HBaseDataInsertion(this);
        }
    }

}
