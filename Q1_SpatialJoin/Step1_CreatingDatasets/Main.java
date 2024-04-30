import java.io.*;

// DS503 Project 2, Question 1 (Step 1): Spatial Join (Creating the Datasets)

public class Main {

    public static void main(String[] args) {
        Main fileMaker = new Main();

        // Create the Points dataset
        fileMaker.createPoints();

        // Create the Rectangles dataset
        fileMaker.createRectangles();
    }

    int max_x = 10000;
    int max_y = 10000;
    int min = 0;

    // Create set of 2D points with random integer values for x and y coordinates
    public void createPoints() {
        // Save dataset in Points.txt file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("Points.txt"))) {

            // Create 10 coordinate points -- change to whatever is above 100MB
            for (int i = 1; i <= 10000000; i++) {

                // x coordinate: random int between 0 and 10 -- change to 10,000
                int x = getRandomInteger(min, max_x - 1);

                // y coordinate: random int between 0 and 10 -- change to 10,000
                int y = getRandomInteger(min, max_y - 1);

                // Write point to file: fields separated by commas & each point on new line
                writer.write(x + "," + y);
                writer.newLine();

            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Create set of 2D rectangles with random integer values for bottom left x and y coordinates as well as height and width
    public void createRectangles() {
        // Save dataset in Transactions.txt file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("Rectangles.txt"))) {

            // Create 10 rectangles -- change to whatever is above 100MB
            for (int i = 1; i <= 5500000; i++) {

                // bottom x coordinate: random int between 0 and 10 -- change to 10,000
                int bottom_x = getRandomInteger(min, max_x - 1);

                // bottom y coordinate: random int between 0 and 10 -- change to 10,000
                int bottom_y = getRandomInteger(min, max_y - 1);

                // height: random int between 0 and 10 -- change to 10,000
                int height = fixHeight(bottom_y, getRandomInteger(min + 1, max_y), max_y);

                // width: random int between 0 and 10 -- change to 10,000
                int width = fixWidth(bottom_x, getRandomInteger(min + 1, max_x), max_x);

                // Writing transactions to file: fields separated by commas & each transaction on new line
                writer.write(bottom_x + "," + bottom_y + "," + height + "," + width);
                writer.newLine();

            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Get a random integer
    private int getRandomInteger(int minNum, int maxNum) {
        // Create and return a random integer between min number and max number
        return (int) (Math.random() * (maxNum - minNum + 1)) + minNum;
    }

    private int fixWidth(int x, int width, int max_x) {
        int new_width;

        if (x + width > max_x) {
            new_width = getRandomInteger(1, max_x - x);
        } else {
            new_width = width;
        }

        return new_width;
    }

    private int fixHeight(int y, int height, int max_y) {
        int new_height;

        if (y + height > max_y) {
            new_height = getRandomInteger(1, max_y - y);
        } else {
            new_height = height;
        }

        return new_height;
    }

}
