package Receiver;
import model.Suicides;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class EntrySuicides {
	
    public EntrySuicides() { }

    private static SparkSession sparkSession(){
        return SparkSession
                .builder()
                .appName("Application with Spark Streaming")
                .master("local[*]")
                .getOrCreate();
    }
    
    private static StructType customSchema(){
        return new StructType(new StructField[] {
                new StructField("state", DataTypes.StringType, true, Metadata.empty()),
                new StructField("year", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("type_major", DataTypes.StringType, true, Metadata.empty()),
                new StructField("type_minor", DataTypes.StringType, true, Metadata.empty()),
                new StructField("gender", DataTypes.StringType, true, Metadata.empty()),
                new StructField("age_group", DataTypes.StringType, true, Metadata.empty()),
                new StructField("total", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("country", DataTypes.StringType, true, Metadata.empty()),
                new StructField("neutral", DataTypes.BooleanType, true, Metadata.empty()),
                
        });
    }
    public static Dataset<Suicides> getDataset(){
        Encoder<Suicides> resultEncoder = Encoders.bean(Suicides.class);

        return sparkSession()
                .readStream()
                .option("header","true")
                .option("treatEmptyValuesAsNulls", "true")
                .option("dateFormat", "MM-dd-yyyy")
                .option("delimiter",",")
                .schema(customSchema())
                .csv("src/main/resources/data/Suicides_in_India*.csv")
                .as(resultEncoder);
    }

}
