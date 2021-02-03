package Receiver;
import Moleds.ResultsFF;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class EntryResultsFF {
	
    public EntryResultsFF() { }

    private static SparkSession sparkSession(){
        return SparkSession
                .builder()
                .appName("Application with Spark Streaming")
                .master("local[*]")
                .getOrCreate();
    }
    
    private static StructType customSchema(){
        return new StructType(new StructField[] {
                new StructField("date", DataTypes.StringType, true, Metadata.empty()),
                new StructField("home_team", DataTypes.StringType, true, Metadata.empty()),
                new StructField("away_team", DataTypes.StringType, true, Metadata.empty()),
                new StructField("home_score", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("away_score", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("tournament", DataTypes.StringType, true, Metadata.empty()),
                new StructField("city", DataTypes.StringType, true, Metadata.empty()),
                new StructField("country", DataTypes.StringType, true, Metadata.empty()),
                new StructField("neutral", DataTypes.BooleanType, true, Metadata.empty()),
                
        });
    }
    public static Dataset<ResultsFF> getDataset(){
        Encoder<ResultsFF> resultEncoder = Encoders.bean(ResultsFF.class);

        return sparkSession()
                .readStream()
                .option("header","true")
                .option("treatEmptyValuesAsNulls", "true")
                .option("dateFormat", "MM-dd-yyyy")
                .option("delimiter",",")
                .schema(customSchema())
                .csv("src/main/resources/data/results1*.csv")
                .as(resultEncoder);
    }

}
