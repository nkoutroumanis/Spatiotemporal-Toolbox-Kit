package gr.ds.unipi.sttk.kNNOverRangeQueries.quadTree;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import gr.ds.unipi.qtree.Point;
import gr.ds.unipi.qtree.QuadTree;
import gr.ds.unipi.stpin.datasources.Datasource;
import gr.ds.unipi.stpin.datasources.FileDatasource;
import gr.ds.unipi.stpin.parsers.CsvRecordParser;
import gr.ds.unipi.stpin.parsers.Record;
import gr.ds.unipi.stpin.parsers.RecordParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class QuadTreeConstruction {

    private static final Logger logger = LoggerFactory.getLogger(QuadTreeConstruction.class);
    private static final Config config = ConfigFactory.load();

    public static void main(String args[]) throws IOException, ParseException {


        Config quadTreeProperties = config.getConfig("quadTreeProperties");
        Config filesParsing = config.getConfig("filesParsing");

        Datasource ds = FileDatasource.newFileDatasource(filesParsing.getString("filesPath"), filesParsing.getString("filesExtension"));
        RecordParser recordParser = new CsvRecordParser(ds, filesParsing.getString("separator"), filesParsing.getInt("numberOfColumnLongitude"), filesParsing.getInt("numberOfColumnLatitude"), filesParsing.getInt("numberOfColumnDate"), filesParsing.getString("dateFormat"));
        DateFormat dateFormat = new SimpleDateFormat(recordParser.getDateFormat());

        QuadTree quadTree = QuadTree.newQuadTree(quadTreeProperties.getDouble("minLon"),quadTreeProperties.getDouble("minLat"),quadTreeProperties.getDouble("maxLon"),quadTreeProperties.getDouble("maxLat"),quadTreeProperties.getInt("maxNumberOfPointsInLeaf"));

        while (recordParser.hasNextRecord()) {

            Record record = recordParser.nextRecord();

            try {

                double longitude = Double.parseDouble(recordParser.getLongitude(record));
                double latitude = Double.parseDouble(recordParser.getLatitude(record));
                Date d = dateFormat.parse(recordParser.getDate(record));

                quadTree.insertPoint(Point.newPoint(longitude,latitude));

            } catch (NumberFormatException | ParseException e) {
                logger.warn("Spatio-temporal information of record can not be parsed {} \nLine {}", e, record.getMetadata());
            } catch (ArrayIndexOutOfBoundsException e) {
                logger.warn("Record is incorrect {} \nLine {}", e, record.getMetadata());
            }
        }

        quadTree.serializeQuadTree(quadTreeProperties.getString("exportFilePath"));

    }
}
