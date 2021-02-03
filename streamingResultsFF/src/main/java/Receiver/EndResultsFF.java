package Receiver;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQueryException;

import Moleds.ResultsFF;

public class EndResultsFF {

    public EndResultsFF(){}

    public static void displayDatasetWithRows(Dataset<Row> dataset, int numberRows , String outputMode) throws StreamingQueryException {
        dataset.writeStream()
                .format("console")
                .outputMode(outputMode)
                .option("numRows",numberRows)
                .start()
                .awaitTermination();
    }
    public static void displayDatasetWithOrders(Dataset<ResultsFF> dataset, int numberRows) throws StreamingQueryException {
        dataset.writeStream()
                .format("console")
                .outputMode("append")
                .option("numRows",numberRows)
                .start()
                .awaitTermination();
    }

}
