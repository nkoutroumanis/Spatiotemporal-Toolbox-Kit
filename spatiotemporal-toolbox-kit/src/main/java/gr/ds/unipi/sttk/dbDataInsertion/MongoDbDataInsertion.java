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

        long startTimeWindow = System.currentTimeMillis();
        long count = 0;

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

//                Config config = parser.toConfig(record).withoutPath(parser.getLongitudeFieldName(record)).withoutPath(parser.getLatitudeFieldName(record)).withoutPath(parser.getDateFieldName(record))
//                        .withValue("location.type", ConfigValueFactory.fromAnyRef("Point"))
//                        .withValue("location.coordinates", ConfigValueFactory.fromAnyRef(Arrays.asList(longitude,latitude)))
//                        .withValue("date", ConfigValueFactory.fromAnyRef(parser.getDate(record)));

                Document embeddedDoc = new Document("type", "Point").append("coordinates", Arrays.asList(longitude, latitude));
                Document doc = new Document("objectId", parser.getVehicle(record)).append("location", embeddedDoc).append("date", d);

                mongoOutput.out(doc,"");

//                mongoOutput.out(Document.parse(config.root().render(ConfigRenderOptions.concise())),"");
                count++;

//                String[] separatedLine = line.split(separator);
//
//                if (Datasource.empty.test(separatedLine[numberOfColumnLongitude - 1]) || Datasource.empty.test(separatedLine[numberOfColumnLatitude - 1]) || Datasource.empty.test(separatedLine[numberOfColumnDate - 1])) {
//                    continue;
//                }
//
//                double longitude = Double.parseDouble(separatedLine[numberOfColumnLongitude - 1]);
//                double latitude = Double.parseDouble(separatedLine[numberOfColumnLatitude - 1]);
//                Date d = dateFormat.parse(separatedLine[numberOfColumnDate - 1]);
//
//                //filtering
//                if (((Double.compare(longitude, rectangle.getMaxx()) == 1) || (Double.compare(longitude, rectangle.getMinx()) == -1)) || ((Double.compare(latitude, rectangle.getMaxy()) == 1) || (Double.compare(latitude, rectangle.getMiny()) == -1))) {
//                    continue;
//                }
//
//                //docs.add( new Document("objectId", separatedLine[0]).append("coordinates", Arrays.asList(longitude, latitude)).append("date",df.parse(separatedLine[numberOfColumnDate - 1])));
//                Document embeddedDoc = new Document("type", "Point").append("coordinates", Arrays.asList(longitude, latitude));
//                docs.add(new Document("objectId", separatedLine[0]).append("location", embeddedDoc).append("date", d));
//                //Document doc = new Document("objectId", separatedLine[0]).append("location", embeddedDoc).append("date", dateFormat.parse(separatedLine[numberOfColumnDate - 1]));
//
//                //System.out.println(new Document("objectId", separatedLine[0]).append("location", embeddedDoc).append("date", dateFormat.parse(separatedLine[numberOfColumnDate - 1])));
//                //mongoCollection.insertOne(docs);
//
//                if (docs.size() == 3000) {
//                    mongoCollection.insertMany(docs);
//                    docs = new ArrayList<>();
//                }
            } catch (ArrayIndexOutOfBoundsException | NumberFormatException | ParseException e) {
                continue;
            }
        }
        logger.info("Totally {} documents have been inserted in collection of MongoDB ",count);
        logger.info("Elapsed time {}", (System.currentTimeMillis() - startTimeWindow) / 1000 + " sec");
        mongoOutput.close();
//        if (docs.size() != 0) {
//            mongoCollection.insertMany(docs);
//            docs = null;
//        }
//
//        mongoCollection = null;
//
//        mongoDbConnector.getMongoClient().close();
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

//    @Override
//    public void lineParse(String lineWithMeta, String[] separatedLine, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, double longitude, double latitude) {
//
//        try {
//            //docs.add( new Document("objectId", separatedLine[0]).append("coordinates", Arrays.asList(longitude, latitude)).append("date",df.parse(separatedLine[numberOfColumnDate - 1])));
//            Document embeddedDoc = new Document("type", "Point").append("coordinates", Arrays.asList(longitude, latitude));
//            docs.add(new Document("objectId", separatedLine[0]).append("location", embeddedDoc).append("date", dateFormat.parse(separatedLine[numberOfColumnDate - 1])));
//            //Document doc = new Document("objectId", separatedLine[0]).append("location", embeddedDoc).append("date", dateFormat.parse(separatedLine[numberOfColumnDate - 1]));
//
//            //System.out.println(new Document("objectId", separatedLine[0]).append("location", embeddedDoc).append("date", dateFormat.parse(separatedLine[numberOfColumnDate - 1])));
//            //mongoCollection.insertOne(docs);
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
//    }

//    @Override
//    public void afterLineParse() {
//        if (docs.size() > 0) {
//            mongoCollection.insertMany(docs);
//        }
//    }

}
