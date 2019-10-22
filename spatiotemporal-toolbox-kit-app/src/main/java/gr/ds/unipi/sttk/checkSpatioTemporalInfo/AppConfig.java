package gr.ds.unipi.sttk.checkSpatioTemporalInfo;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import gr.ds.unipi.stpin.Rectangle;
import gr.ds.unipi.stpin.datasources.Datasource;
import gr.ds.unipi.stpin.datasources.FileDatasource;
import gr.ds.unipi.stpin.datasources.KafkaDatasource;
import gr.ds.unipi.stpin.outputs.FileOutput;
import gr.ds.unipi.stpin.parsers.CsvRecordParser;
import gr.ds.unipi.stpin.parsers.JsonRecordParser;
import gr.ds.unipi.stpin.parsers.RecordParser;

import java.io.File;

public class AppConfig {
    private final Config config;

    private AppConfig(String pathOfConfigFile){
        config = ConfigFactory.parseFile(new File(pathOfConfigFile));
    }

    public static AppConfig newAppConfig(String pathOfConfigFile){
        return new AppConfig(pathOfConfigFile);
    }

    public Datasource getDataSource() throws Exception {
        Config datasource = config.getConfig("datasource");
        Datasource ds;

        if(datasource.getString("type").equals("files")){
            Config files = datasource.getConfig("files");
            ds = FileDatasource.newFileDatasource(files.getString("filesPath"), files.getString("filesExtension"));
        }
        else if(datasource.getString("type").equals("kafka")) {
            Config kafka = datasource.getConfig("kafka");
            ds = KafkaDatasource.newKafkaDatasource(kafka.getString("consumerPropertiesPath"), kafka.getString("consumerTopic"), kafka.getInt("poll"));
        }
        else{
            throw new Exception("datasource type is not set correctly");
        }

        return ds;
    }

    public RecordParser getRecordParser(Datasource datasource) throws Exception {
        Config parser = config.getConfig("parser");
        RecordParser rp;

        if(parser.getString("type").equals("csv")){
            Config csv = parser.getConfig("csv");
                rp = new CsvRecordParser(datasource, csv.getString("separator"), csv.getInt("numberOfColumnLongitude"), csv.getInt("numberOfColumnLatitude"), csv.getInt("numberOfColumnDate"), parser.getString("dateFormat"));
        }
        else if(parser.getString("type").equals("json")){
            Config json = parser.getConfig("json");
                rp = new JsonRecordParser(datasource, json.getString("longitudeFieldName"), json.getString("latitudeFieldName"), json.getString("dateFieldName"), parser.getString("dateFormat"));
        }
        else{
            throw new Exception("parser type is not set correctly");
        }

        return rp;
    }


    public void getCheckSpatioTemporalInfo(RecordParser recordParser) throws Exception {
        Config check = config.getConfig("checkSpatioTemporalInfo");
        Config filter = config.getConfig("filter");

        CheckSpatioTemporalInfo.Builder checkSpatioTemporalInfo = CheckSpatioTemporalInfo.newCheckSpatioTemporalInfo(recordParser);

                if(check.getBoolean("filter")){
                    checkSpatioTemporalInfo.filter(Rectangle.newRectangle(filter.getDouble("minLon"), filter.getDouble("minLat"), filter.getDouble("maxLon"), filter.getDouble("maxLat")));
                }
                checkSpatioTemporalInfo.build().exportInfo(FileOutput.newFileOutput(check.getString("filesOutputPath"), check.getBoolean("deleteOutputDirectoryIfExists")));
    }

}
