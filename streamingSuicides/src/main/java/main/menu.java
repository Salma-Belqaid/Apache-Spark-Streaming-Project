4package main;


import SparkStreaming.sparkSuicides;

import java.util.Scanner;

public class menu {

    public static Scanner scanner = new Scanner(System.in);
    public static sparkSuicides sparkSuicides = new sparkSuicides();

    public static void main(String[] args) {

        try {
            int menuOption = 0;

            do {
                menuOption = showMenu();

                switch (menuOption) {

                    case 1:
                    	sparkSuicides.listSuicides(5);
                        break;
                    case 2:
                    	sparkSuicides.listSuicidesByType_major("Social_Status",8);
                        break;
                    case 3:
                    	sparkSuicides.listSuicidesByStateAndAge_Groupe("Nagaland","30-44");
                        break;
                    case 4:
                    	sparkSuicides.listSuicidesByType_minorSortedByYear("Family Problems");
                        break;
                    case 5:
                    	sparkSuicides.listSuicidesSorted("Female");
                        break;
                    case 6:
                    	sparkSuicides.listSuicidesCount(1);
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
		System.out.println("2. ************ filter suicides by type_major= Social_Status ************");
		System.out.println("3. ************ filter results by state = Nagaland and age_group = 30-44 ************");
		System.out.println("4. ******************* Display the results of match ho hase type_minor=Family Problems, sorted by year **************************");
		System.out.println("5. ******************* Display the results of match ho hase gender=Female, sorted by total **************************");
		System.out.println("6. ******************* Count the nomber of total=1 **************************");
        System.out.println("7. Finish Program");
        System.out.println(" ");

        System.out.println("Enter the number  : ");
        option = scanner.nextInt();

        return option;
    }
}

