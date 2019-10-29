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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.function.Function;

public final class MongoDbDataInsertion {

    private static final Logger logger = LoggerFactory.getLogger(MongoDbDataInsertion.class);

    private final MongoOutput mongoOutput;
    private final RecordParser parser;

    private final Rectangle rectangle;

    private MongoDbDataInsertion(Builder builder) {
        mongoOutput = builder.mongoOutput;
        parser = builder.parser;

        rectangle = builder.rectangle;
    }

    public static MongoDbDataInsertion.Builder newMongoDbDataInsertion(String host, int port, String database, String username, String password, String collection, int batchSize, RecordParser parser) throws Exception {
        return new MongoDbDataInsertion.Builder(new MongoOutput(host, port, database, username, password, collection, batchSize), parser);
    }

//    public void insertDataOnCollection(String collection, CsvRecordParser recordParser){
//
//        insertDataOnCollection(collection, recordParser, (record -> {
//
//
//            recordParser.toConfig(record,";").;
//
//        }));
//    }
//
//    public void insertDataOnCollection(String collection, JsonRecordParser recordParser){
//        insertDataOnCollection(collection, recordParser, (record -> {
//
//
//            recordParser.t
//
//        }));
//    }

//    public void insertDataOnCollection(String collection, RecordParser recordParser, Function<Record, Document> function) throws IOException {
//
//        recordParser.
//        while(parser.hasNextRecord()){
//
//            try {
//                Record record = parser.nextRecord();
//                function.apply()
//
//            } catch (ParseException e) {
//                e.printStackTrace();
//            }
//
//
//        }
//
//
//
//    }


    public void insertDataOnCollection() throws IOException {

        Function<Record, Date> dateFunction = RecordParser.dateFunction(parser);

        long startTimeWindow = System.currentTimeMillis();
        long count = 0;

        while (parser.hasNextRecord()) {

            try {
                Record record = parser.nextRecord();

                if (Datasource.empty.test(parser.getLongitude(record)) || Datasource.empty.test(parser.getLatitude(record)) || Datasource.empty.test(parser.getDate(record))) {
                    continue;
                }

                //DateFormat dateFormat = new SimpleDateFormat(parser.getDateFormat());

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

                Config config = parser.toConfig(record).withoutPath(parser.getLongitudeFieldName(record)).withoutPath(parser.getLatitudeFieldName(record)).withoutPath(parser.getDateFieldName(record))
                        .withValue("location.type", ConfigValueFactory.fromAnyRef("Point"))
                        .withValue("location.coordinates", ConfigValueFactory.fromAnyRef(Arrays.asList(longitude,latitude)));//.withValue("vehicleId", ConfigValueFactory.fromAnyRef(parser.getVehicle(record)));
//                        .withValue("date", ConfigValueFactory.fromAnyRef(parser.getDate(record)));

                Document doc = Document.parse(config.root().render(ConfigRenderOptions.concise())).append("date", d);

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

        private Rectangle rectangle = null; // = Rectangle.newRectangle(-180, -90, 180, 90);

        public Builder(MongoOutput mongoOutput, RecordParser parser) throws Exception {
            this.mongoOutput = mongoOutput;
            this.parser = parser;
        }

        public Builder filter(Rectangle rectangle) {
            this.rectangle = rectangle;
            return this;
        }

        public MongoDbDataInsertion build() {
            return new MongoDbDataInsertion(this);
        }
    }

}
