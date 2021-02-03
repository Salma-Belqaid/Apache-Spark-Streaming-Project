package SparkStreaming;

import static Receiver.EndResultsFF.displayDatasetWithOrders;
import static Receiver.EndResultsFF.displayDatasetWithRows;
import static Receiver.EntryResultsFF.getDataset;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQueryException;

import Moleds.ResultsFF;

public class ResultsFFStreaming {

	public void listResultsRR(int numberRows) throws StreamingQueryException {
		displayDatasetWithOrders(getDataset(), numberRows);
	}

	public void listResultsRRByAwayTeam(String away_team) throws StreamingQueryException {
		Dataset<ResultsFF> results = getDataset()
				.filter((FilterFunction<ResultsFF>) result -> result.getAway_team().equals(away_team));
		displayDatasetWithOrders(results, 8);
	}

	public void listResultsRRByHomeTeamandDate(String home_team, String date) throws StreamingQueryException {
		Dataset<Row> results = getDataset()
				.filter((FilterFunction<ResultsFF>) result -> result.getHome_team().equals(home_team))
				.select("date", "home_team", "away_score", "city").where("date == \"" + date + "\"");
		displayDatasetWithRows(results, 1008, "append");
	}

	public void listResultsRRByCountrySortedbyHomeScore(String country) throws StreamingQueryException {
		Dataset<Row> results = getDataset()
				.filter((FilterFunction<ResultsFF>) result -> result.getCountry().equals(country))
				.groupBy("country", "date", "home_team", "home_score").count().sort("home_score");
		displayDatasetWithRows(results, 1008, "complete");
	}

	public void listResultByhome_teamSortedByHomeScore(String home_team) throws StreamingQueryException {
		Dataset<Row> results = getDataset()
				.filter((FilterFunction<ResultsFF>) result -> result.getHome_team().equals(home_team))
				.groupBy("home_team", "home_score", "neutral").count().sort("home_score");
		displayDatasetWithRows(results, 1008, "complete");
	}
}
