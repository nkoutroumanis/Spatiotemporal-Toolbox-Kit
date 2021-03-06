package gr.ds.unipi.sttk.kNNOverRangeQueries.quadTree;

import gr.ds.unipi.qtree.Point;
import gr.ds.unipi.qtree.QuadTree;
import gr.ds.unipi.stpin.datasources.Datasource;
import gr.ds.unipi.stpin.datasources.FileDatasource;
import gr.ds.unipi.stpin.parsers.CsvRecordParser;
import gr.ds.unipi.stpin.parsers.Record;
import gr.ds.unipi.stpin.parsers.RecordParser;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class QuadTreeConstructionTest {

    private static final Logger logger = LoggerFactory.getLogger(QuadTreeConstructionTest.class);


    @Test
    public void main() throws IOException, ParseException {

        Datasource ds = FileDatasource.newFileDatasource("./src/test/resources/csv/", ".csv");
        RecordParser recordParser = new CsvRecordParser(ds, ";", 2, 3, 4, "yyyy-MM-dd HH:mm:ss");
        DateFormat dateFormat = new SimpleDateFormat(recordParser.getDateFormat());

        QuadTree quadTree = QuadTree.newQuadTree(0,0,1000,1000,10);

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

        quadTree.serializeQuadTree("./src/test/resources/quadTree/tree.bin");

    }


}