package SparkStreaming;

import model.Suicides;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQueryException;

import static Receiver.EntrySuicides.getDataset;
import static Receiver.EndSuicides.*;

public class sparkSuicides {

	public void listSuicides(int numberRows) throws StreamingQueryException {
		displayDatasetWithOrders(getDataset(), numberRows);
	}

	public void listSuicidesByType_major(String type_major, int numberRows) throws StreamingQueryException {
		Dataset<Suicides> suicides = getDataset()
				.filter((FilterFunction<Suicides>) suicide -> suicide.getType_major().equals(type_major));
		displayDatasetWithOrders(suicides, numberRows);
	}

	public void listSuicidesByStateAndAge_Groupe(String state, String age_group) throws StreamingQueryException {
		Dataset<Row> results = getDataset().filter((FilterFunction<Suicides>) result -> result.getState().equals(state))
				.select("state", "year", "age_group").where("age_group == \"" + age_group + "\"");
		displayDatasetWithRows(results, 8, "append");
	}

	public void listSuicidesByType_minorSortedByYear(String type_minor) throws StreamingQueryException {
		Dataset<Row> results = getDataset()
				.filter((FilterFunction<Suicides>) result -> result.getType_minor().equals(type_minor))
				.groupBy("state", "year", "type_major", "type_minor").count().sort("year");
		displayDatasetWithRows(results, 50, "complete");
	}

	public void listSuicidesSorted(String gender) throws StreamingQueryException {
		Dataset<Row> results = getDataset()
				.filter((FilterFunction<Suicides>) result -> result.getGender().equals(gender))
				.groupBy("year", "type_major", "type_minor", "gender", "total").count().sort("total");
		displayDatasetWithRows(results, 50, "complete");
	}
	
	public void listSuicidesCount(int total) throws StreamingQueryException {
		Dataset<Row> results = getDataset()
				.filter((FilterFunction<Suicides>) result -> result.getTotal()==total)
				.groupBy("total").count();
		displayDatasetWithRows(results, 50, "complete");
	}
}
