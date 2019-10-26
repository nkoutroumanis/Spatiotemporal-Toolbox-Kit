package gr.ds.unipi.sttk.dbDataInsertion;

import gr.ds.unipi.stpin.Rectangle;
import gr.ds.unipi.stpin.datasources.Datasource;
import gr.ds.unipi.stpin.outputs.MongoOutput;
import gr.ds.unipi.stpin.parsers.Record;
import gr.ds.unipi.stpin.parsers.RecordParser;
import org.bson.Document;
import org.davidmoten.hilbert.HilbertCurve;
import org.davidmoten.hilbert.SmallHilbertCurve;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

public final class MongoDbDataInsertionHilbertCurve {

    private static final Logger logger = LoggerFactory.getLogger(MongoDbDataInsertionHilbertCurve.class);

    private final MongoOutput mongoOutput;
    private final RecordParser parser;

    private final Rectangle rectangle;

    private final int bits;
    private final long maxOrdinates;
    private final Rectangle space;
    private final long minDate;
    private final long maxDate;

    private MongoDbDataInsertionHilbertCurve(Builder builder) {
        mongoOutput = builder.mongoOutput;
        parser = builder.parser;

        bits = builder.bits;
        maxOrdinates = 1L << bits;
        space = builder.space;
        minDate = builder.minDate;
        maxDate = builder.maxDate;

        rectangle = builder.rectangle;
    }

    public static MongoDbDataInsertionHilbertCurve.Builder newMongoDbDataInsertionHilbertCurve(String host, int port, String database, String username, String password, String collection, int batchSize, RecordParser parser, int bits, Rectangle space, long minDate, long maxDate) throws Exception {
        return new MongoDbDataInsertionHilbertCurve.Builder(new MongoOutput(host, port, database, username, password, collection, batchSize), parser, bits, space, minDate, maxDate);
    }

    public void insertDataOnCollection() throws IOException {

        long startTimeWindow = System.currentTimeMillis();
        long count = 0;


        SmallHilbertCurve hc = HilbertCurve.small().bits(bits).dimensions(3);

        while (parser.hasNextRecord()) {

            try {
                Record record = parser.nextRecord();

                if (Datasource.empty.test(parser.getLongitude(record)) || Datasource.empty.test(parser.getLatitude(record)) || Datasource.empty.test(parser.getDate(record))) {
                    continue;
                }

                DateFormat dateFormat = new SimpleDateFormat(parser.getDateFormat());

                double longitude = Double.parseDouble(parser.getLongitude(record));
                double latitude = Double.parseDouble(parser.getLatitude(record));
                Date d = dateFormat.parse(parser.getDate(record));

                if(rectangle != null) {
                    //filtering
                    if (((Double.compare(longitude, rectangle.getMaxx()) == 1) || (Double.compare(longitude, rectangle.getMinx()) == -1)) || ((Double.compare(latitude, rectangle.getMaxy()) == 1) || (Double.compare(latitude, rectangle.getMiny()) == -1))) {
                        continue;
                    }
                }

                long index = hc.index(GeoUtil.scalePoint(longitude,space.getMinx(),space.getMaxx(),latitude,space.getMiny(),space.getMaxy(),d.getTime(),minDate, maxDate, maxOrdinates));

//                Config config = parser.toConfig(record).withoutPath(parser.getLongitudeFieldName(record)).withoutPath(parser.getLatitudeFieldName(record)).withoutPath(parser.getDateFieldName(record))
//                        .withValue("location.type", ConfigValueFactory.fromAnyRef("Point"))
//                        .withValue("location.coordinates", ConfigValueFactory.fromAnyRef(Arrays.asList(longitude,latitude)))
//                        .withValue("date", ConfigValueFactory.fromAnyRef(parser.getDate(record)));

                Document embeddedDoc = new Document("type", "Point").append("coordinates", Arrays.asList(longitude, latitude)).append("hilIndex", index);
                Document doc = new Document("objectId", parser.getVehicle(record)).append("location", embeddedDoc).append("date", d);

                mongoOutput.out(doc,"");

//                mongoOutput.out(Document.parse(config.root().render(ConfigRenderOptions.concise())),"");
                count++;

//
//                //docs.add( new Document("objectId", separatedLine[0]).append("coordinates", Arrays.asList(longitude, latitude)).append("date",df.parse(separatedLine[numberOfColumnDate - 1])));
//                Document embeddedDoc = new Document("type", "Point").append("coordinates", Arrays.asList(longitude, latitude));
//                docs.add(new Document("objectId", separatedLine[0]).append("location", embeddedDoc).append("date", d));
//                //Document doc = new Document("objectId", separatedLine[0]).append("location", embeddedDoc).append("date", dateFormat.parse(separatedLine[numberOfColumnDate - 1]));
//
            } catch (ArrayIndexOutOfBoundsException | NumberFormatException | ParseException e) {
                continue;
            }
        }
        logger.info("Totally {} documents have been inserted in collection of MongoDB ",count);
        logger.info("Elapsed time {}", (System.currentTimeMillis() - startTimeWindow) / 1000 + " sec");
        mongoOutput.close();

    }

    public static class Builder {

        private final MongoOutput mongoOutput;
        private final RecordParser parser;
        private final int bits;
        private final Rectangle space;
        private final long minDate;
        private final long maxDate;

        private Rectangle rectangle = null; // = Rectangle.newRectangle(-180, -90, 180, 90);

        public Builder(MongoOutput mongoOutput, RecordParser parser, int bits, Rectangle space, long minDate, long maxDate) throws Exception {
            this.mongoOutput = mongoOutput;
            this.parser = parser;
            this.bits = bits;
            this.space = space;
            this.minDate = minDate;
            this.maxDate = maxDate;
        }

        public Builder filter(Rectangle rectangle) {
            this.rectangle = rectangle;
            return this;
        }

        public MongoDbDataInsertionHilbertCurve build() {
            return new MongoDbDataInsertionHilbertCurve(this);
        }
    }

}
