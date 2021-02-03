package main;

import java.util.Scanner;
import SparkStreaming.ResultsFFStreaming;

public class main {

	public static Scanner scanner = new Scanner(System.in);
	public static ResultsFFStreaming sparkResult = new ResultsFFStreaming();

	public static void main(String[] args) {

		try {
			int menuOption = 0;

			do {
				menuOption = showMenu();

				switch (menuOption) {

				case 1:
					sparkResult.listResultsRR(5);
					break;
				case 2:
					sparkResult.listResultsRRByAwayTeam("Italy");
					break;
				case 3:
					sparkResult.listResultsRRByCountrySortedbyHomeScore("Hong Kong");
					break;
				case 4:
					sparkResult.listResultsRRByHomeTeamandDate("United States", "2020-03-11");
					break;
				case 5:
					sparkResult.listResultByhome_teamSortedByHomeScore("Japan");
					break;
				case 6:
					System.out.println("Quitting Program...");
					break;
				default:
					System.out.println("Sorry, please enter valid Option");
				}

			} while (menuOption != 6);

			System.out.println("Good Bye :D ");

		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println("Sorry problem occured within Program");
			scanner.next();
		} finally {
			scanner.close();
		}
	}

	public static int showMenu() {
		int option = 0;

		System.out.println("********************************* Menu *************************************");
		System.out.println(" ");
		System.out.println("1. ************ Display the firste 5 results of dataset ************");
		System.out.println("2. ************ filter results by away_team = Italy ************");
		System.out.println(
				"3. ******************* Display the results of match ho hase country=Hong Kong, sorted by home_score **************************");
		System.out.println(
				"4. ************ filter results by date = 2020-03-11 and Home-team = United States ************");
		System.out.println(
				"5. ******************* Display the results of match ho hase home_team=Japan, sorted by home_score **************************");
		System.out.println("6. Finish Program");
		System.out.println(" ");

		System.out.println("Enter the number : ");
		option = scanner.nextInt();

		return option;
	}
}
