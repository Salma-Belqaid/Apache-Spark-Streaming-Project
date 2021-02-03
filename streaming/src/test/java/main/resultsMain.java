package main;
import java.util.Scanner;
import SparkStreaming.sparkResults;

public class resultsMain {

    public static Scanner scanner = new Scanner(System.in);
    public static sparkResults sparkResult = new sparkResults();

    public static void main(String[] args) {

        try {
            int menuOption = 0;
            do {
                menuOption = showMenu();

                switch (menuOption) {

                    case 1:
                        sparkResult.listResults(5);
                        break;
                    case 2:
                        sparkResult.listResultsByHomeTeam("Abkhazia");
                        break;
                    case 3:
                        sparkResult.listResultsCount("Friendly");
                        break;
                    case 4:
                        sparkResult.listResultsByAwayTeamandCity("Scotland","London");
                        break;
                    case 5:
                        sparkResult.listResultsByCountrySortedbyHomeScore("Wales");
                        break;
                    case 6:
                        sparkResult.listResultByhome_teamSortedByHomeScore("Abkhazia");
                        break;
                    case 7:
                        System.out.println("Quitting Program...");
                        break;
                    default:
                        System.out.println("Sorry, please enter valid Option");
                }

            } while (menuOption != 6);

            System.out.println("Thanks for using this Program...");

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
		System.out.println("2. ************ filter results by home_team = Abkhazia ************");
		System.out.println("3. ****************** count Results ho hase neutra(Whether the match took place at a neutral venue or not)=True ***************");
		System.out.println("4. ************ filter results by city = London and Away-team = Scotland************");
		System.out.println("5. ******************* Display the results of match ho hase country=Wales, sorted by date **************************");
		System.out.println("6. ******************* Display the results of match ho hase home_score=Abkhazia, sorted by home_score **************************");
        System.out.println("7. Finish Program");
        System.out.println(" ");

        System.out.println("Enter the number  : ");
        option = scanner.nextInt();

        return option;
    }
}


