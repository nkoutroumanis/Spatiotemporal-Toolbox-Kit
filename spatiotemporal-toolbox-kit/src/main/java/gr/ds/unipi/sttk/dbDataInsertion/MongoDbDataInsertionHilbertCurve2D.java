package gr.ds.unipi.sttk.dbDataInsertion;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValueFactory;
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
import java.util.function.Function;

public final class MongoDbDataInsertionHilbertCurve2D {

    private static final Logger logger = LoggerFactory.getLogger(MongoDbDataInsertionHilbertCurve2D.class);

    private final MongoOutput mongoOutput;
    private final RecordParser parser;

    private final Rectangle rectangle;

    private final int bits;
    private final long maxOrdinates;
    private final Rectangle space;

    private MongoDbDataInsertionHilbertCurve2D(Builder builder) {
        mongoOutput = builder.mongoOutput;
        parser = builder.parser;

        bits = builder.bits;
        maxOrdinates = 1L << bits;
        space = builder.space;

        rectangle = builder.rectangle;
    }

    public static MongoDbDataInsertionHilbertCurve2D.Builder newMongoDbDataInsertionHilbertCurve(String host, int port, String database, String username, String password, String collection, int batchSize, RecordParser parser, int bits, Rectangle space) throws Exception {
        return new MongoDbDataInsertionHilbertCurve2D.Builder(new MongoOutput(host, port, database, username, password, collection, batchSize), parser, bits, space);
    }

    public void insertDataOnCollection() throws IOException {

        Function<Record, Date> dateFunction = parser.getDateFunction();

        long startTimeWindow = System.currentTimeMillis();
        long count = 0;

        SmallHilbertCurve hc = HilbertCurve.small().bits(bits).dimensions(2);

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

                long index = hc.index(GeoUtil.scale2DPoint(longitude,space.getMinx(),space.getMaxx(),latitude,space.getMiny(),space.getMaxy(), maxOrdinates));

                Config config = parser.toConfig(record).withoutPath(parser.getLongitudeFieldName(record)).withoutPath(parser.getLatitudeFieldName(record)).withoutPath(parser.getDateFieldName(record))
                        .withValue("location.type", ConfigValueFactory.fromAnyRef("Point"))
                        .withValue("location.coordinates", ConfigValueFactory.fromAnyRef(Arrays.asList(longitude,latitude)));//.withValue("vehicleId", ConfigValueFactory.fromAnyRef(parser.getVehicle(record)));
//                        .withValue("date", ConfigValueFactory.fromAnyRef(parser.getDate(record)));

                Document doc = Document.parse(config.root().render(ConfigRenderOptions.concise())).append("date", d).append("hilIndex", index);

//                Document embeddedDoc = new Document("type", "Point").append("coordinates", Arrays.asList(longitude, latitude));
//                Document doc = new Document("objectId", parser.getVehicle(record)).append("location", embeddedDoc).append("date", d).append("hilIndex", (int) index);

                mongoOutput.out(doc,"");

//                mongoOutput.out(Document.parse(config.root().render(ConfigRenderOptions.concise())),"");
                count++;

//
//                //docs.add( new Document("objectId", separatedLine[0]).append("coordinates", Arrays.asList(longitude, latitude)).append("date",df.parse(separatedLine[numberOfColumnDate - 1])));
//                Document embeddedDoc = new Document("type", "Point").append("coordinates", Arrays.asList(longitude, latitude));
//                docs.add(new Document("objectId", separatedLine[0]).append("location", embeddedDoc).append("date", d));
//                //Document doc = new Document("objectId", separatedLine[0]).append("location", embeddedDoc).append("date", dateFormat.parse(separatedLine[numberOfColumnDate - 1]));
//
            } catch (NumberFormatException | ParseException e) {
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

        private Rectangle rectangle = null; // = Rectangle.newRectangle(-180, -90, 180, 90);

        public Builder(MongoOutput mongoOutput, RecordParser parser, int bits, Rectangle space) throws Exception {
            this.mongoOutput = mongoOutput;
            this.parser = parser;
            this.bits = bits;
            this.space = space;

        }

        public Builder filter(Rectangle rectangle) {
            this.rectangle = rectangle;
            return this;
        }

        public MongoDbDataInsertionHilbertCurve2D build() {
            return new MongoDbDataInsertionHilbertCurve2D(this);
        }
    }

}
