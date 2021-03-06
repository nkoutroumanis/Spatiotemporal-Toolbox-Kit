package gr.ds.unipi.sttk.checkSpatioTemporalInfo;

import gr.ds.unipi.stpin.Rectangle;
import gr.ds.unipi.stpin.datasources.KafkaDatasource;
import gr.ds.unipi.stpin.outputs.FileOutput;
import gr.ds.unipi.stpin.parsers.Record;
import gr.ds.unipi.stpin.parsers.RecordParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

public final class CheckSpatioTemporalInfo {

    private static final Logger logger = LoggerFactory.getLogger(CheckSpatioTemporalInfo.class);

    private final RecordParser recordParser;
    private final Rectangle rectangle;
    private long numberOfRecords = 0;

    private double maxx = Integer.MIN_VALUE;
    private double minx = Integer.MAX_VALUE;
    private double maxy = Integer.MIN_VALUE;
    private double miny = Integer.MAX_VALUE;

    private Date minDate = new Date(Long.MAX_VALUE);
    private Date maxDate = new Date(Long.MIN_VALUE);

    private CheckSpatioTemporalInfo(Builder builder) {
        recordParser = builder.recordParser;
        rectangle = builder.rectangle;
    }

    public static Builder newCheckSpatioTemporalInfo(RecordParser recordParser) throws Exception {
        return new CheckSpatioTemporalInfo.Builder(recordParser);
    }

    public void exportInfo(FileOutput fileOutput) throws IOException, ParseException {

        Set<String> errorLines = new HashSet<>();
        Set<String> spatioTemporalInformationOutOfRange = new HashSet<>();

        Function<Record, Date> dateFunction = recordParser.getDateFunction();

        while (recordParser.hasNextRecord()) {

            Record record = recordParser.nextRecord();
            String lineMetaData = record.getMetadata();

            try {

                double longitude = Double.parseDouble(recordParser.getLongitude(record));
                double latitude = Double.parseDouble(recordParser.getLatitude(record));
                Date d = dateFunction.apply(record);

                //filtering
                if (((Double.compare(longitude, rectangle.getMaxx()) == 1) || (Double.compare(longitude, rectangle.getMinx()) == -1)) || ((Double.compare(latitude, rectangle.getMaxy()) == 1) || (Double.compare(latitude, rectangle.getMiny()) == -1))) {

                    if (recordParser.getDatasource() instanceof KafkaDatasource) {
                        lineMetaData = lineMetaData.substring(0, lineMetaData.lastIndexOf("."));
                    }

                    if (!spatioTemporalInformationOutOfRange.contains(lineMetaData)) {
                        spatioTemporalInformationOutOfRange.add(lineMetaData);
                    }
                    continue;
                }

                if (Double.compare(maxx, longitude) == -1) {
                    maxx = longitude;
                }
                if (Double.compare(minx, longitude) == 1) {
                    minx = longitude;
                }
                if (Double.compare(maxy, latitude) == -1) {
                    maxy = latitude;
                }
                if (Double.compare(miny, latitude) == 1) {
                    miny = latitude;
                }

                if (maxDate.compareTo(d) < 0) {
                    maxDate = d;
                }
                if (minDate.compareTo(d) > 0) {
                    minDate = d;
                }

                numberOfRecords++;


            } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {

                if ((e instanceof NumberFormatException)) {
                    logger.warn("Spatial information of record can not be parsed {} \nLine {}", e, record.getMetadata());
                } else {
                    logger.warn("Record is incorrect {} \nLine {}", e, record.getMetadata());
                }

                if (recordParser.getDatasource() instanceof KafkaDatasource) {
                    lineMetaData = lineMetaData.substring(0, lineMetaData.lastIndexOf("."));
                }

                if (!errorLines.contains(lineMetaData)) {
                    errorLines.add(lineMetaData);
                }
            }

        }

        String fileName = "Spatio-temporal-Info.txt";

        fileOutput.out("Lines error at: ", fileName);
        errorLines.forEach((s) -> fileOutput.out(s, fileName));
        fileOutput.out("\r\n", fileName);

        fileOutput.out("Spatial Information out of range at: ", fileName);
        spatioTemporalInformationOutOfRange.forEach((s) -> fileOutput.out(s, fileName));
        fileOutput.out("\r\n", fileName);

        fileOutput.out("Formed Spatial Box: ", fileName);
        fileOutput.out("Max Longitude: " + maxx, fileName);
        fileOutput.out("Min Longitude: " + minx, fileName);
        fileOutput.out("Max Latitude: " + maxy, fileName);
        fileOutput.out("Min Latitude: " + miny, fileName);

        if(recordParser.getDateFormat().equals("unixTimestamp")){
            fileOutput.out("Max Date: " + maxDate.getTime(), fileName);
            fileOutput.out("Min Date: " + minDate.getTime(), fileName);
        }
        else{
            DateFormat dateFormat = new SimpleDateFormat(recordParser.getDateFormat());
            fileOutput.out("Max Date: " + dateFormat.format(maxDate), fileName);
            fileOutput.out("Min Date: " + dateFormat.format(minDate), fileName);
        }

        fileOutput.out("\r\n", fileName);
        fileOutput.out("All of the records are " + numberOfRecords, fileName);

        fileOutput.close();

    }

    public static class Builder {

        private final RecordParser recordParser;

        private Rectangle rectangle = Rectangle.newRectangle(-180, -90, 180, 90);

        public Builder(RecordParser recordParser) throws Exception {
            this.recordParser = recordParser;
            this.rectangle = rectangle;
        }

        public Builder filter(Rectangle rectangle) {
            this.rectangle = rectangle;
            return this;
        }

        public CheckSpatioTemporalInfo build() {
            return new CheckSpatioTemporalInfo(this);
        }

    }

}
