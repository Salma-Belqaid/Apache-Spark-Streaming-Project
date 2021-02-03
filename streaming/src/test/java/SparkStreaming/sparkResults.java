package SparkStreaming;

import model.Results;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQueryException;

import static Receiver.EntryResults.getDataset;
import static Receiver.EndResults.*;

public class sparkResults {
	

	public void listResults(int numberRows) throws StreamingQueryException {
		displayDatasetWithOrders(getDataset(), numberRows);
	}

	public void listResultsByHomeTeam(String home_team) throws StreamingQueryException {
		Dataset<Results> results = getDataset()
				.filter((FilterFunction<Results>) result -> result.getHome_team().equals(home_team));
		displayDatasetWithOrders(results, 8);
	}

	public void listResultsByAwayTeamandCity(String away_team, String city) throws StreamingQueryException {
		Dataset<Row> results = getDataset()
				.filter((FilterFunction<Results>) result -> result.getAway_team().equals(away_team))
				.select("away_team", "away_score", "city").where("city == \"" + city + "\"");
		displayDatasetWithRows(results, 8, "append");
	}
	
	public void listResultsCount(String tournament) throws StreamingQueryException {
		Dataset<Row> results = getDataset()
				.filter((FilterFunction<Results>) result -> result.getTournament().equals(tournament))
				.groupBy("tournament").count();
		displayDatasetWithRows(results, 50, "complete");
	}

	public void listResultsByCountrySortedbyHomeScore(String country) throws StreamingQueryException {
		Dataset<Row> results = getDataset()
				.filter((FilterFunction<Results>) result -> result.getCountry().equals(country))
				.groupBy("country", "date", "home_team", "home_score").count().sort("date");
		displayDatasetWithRows(results, 8, "complete");
	}

	public void listResultByhome_teamSortedByHomeScore(String home_team) throws StreamingQueryException {
		Dataset<Row> results = getDataset()
				.filter((FilterFunction<Results>) result -> result.getHome_team().equals(home_team))
				.groupBy("home_team", "home_score", "neutral").count().sort("home_score");
		displayDatasetWithRows(results, 8, "complete");
	}
}
