package Receiver;
import model.Suicides;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class EndSuicides {

    public EndSuicides(){}

    public static void displayDatasetWithRows(Dataset<Row> dataset, int numberRows , String outputMode) throws StreamingQueryException {
        dataset.writeStream()
                .format("console")
                .outputMode(outputMode)
                .option("numRows",numberRows)
                .start()
                .awaitTermination();
    }
    public static void displayDatasetWithOrders(Dataset<Suicides> dataset, int numberRows) throws StreamingQueryException {
        dataset.writeStream()
                .format("console")
                .outputMode("append")
                .option("numRows",numberRows)
                .start()
                .awaitTermination();
    }

}

