import java.awt.Point;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

import javax.naming.Context;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// DS503 Project 2, Question 2 (Step 2): Outlier Detection
public class OutlierDetection {

    // Take in point dataset and define current and neighboring grid cells
    public static class PointMapper extends Mapper<Object, Text, Text, Text> {

        // Define r - radius for distance function
        private int r;

        // Define max x & y for grid
        private int max;

        // Set values for r & max
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            r = Integer.parseInt(conf.get("r"));
            max = 10000;
        }
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Takes in a single point "x,y" and turns it into a string
            String record = value.toString();

            // Divides record string into parts based on commas
            String[] parts = record.split(",");

            // Set values of point: parts[0] = x, parts[1] = y
            int x = Integer.parseInt(parts[0]);
            int y = Integer.parseInt(parts[1]);

            // Define current grid cell within the space based on r
            int gridX = (int) Math.ceil((double) x / r);
            int gridY = (int) Math.ceil((double) y / r);

            // Check to see if current grid is valid (not outside of space boundaries) before printing output
            if (isValidGrid(gridX, max, r) && isValidGrid(gridY, max, r)) {
                // Print output for the current grid:
                // key = gridX, gridY
                // value = x, y
                context.write(new Text(gridX + "," + gridY), new Text(x + "," + y));

                // Print output for neighboring grids
                printNeighbor(context, gridX + 1, gridY, x, y); // above
                printNeighbor(context, gridX - 1, gridY, x, y); // below
                printNeighbor(context, gridX, gridY - 1, x, y); // left
                printNeighbor(context, gridX, gridY + 1, x, y); // right
                printNeighbor(context, gridX - 1, gridY - 1, x, y); // above-left
                printNeighbor(context, gridX - 1, gridY + 1, x, y); // above-right
                printNeighbor(context, gridX + 1, gridY - 1, x, y); // below-left
                printNeighbor(context, gridX + 1, gridY + 1, x, y); // below-right
            }
        }

        // Print output for neighboring grids
        private void printNeighbor(Context context, int gridX, int gridY, int x, int y)
                throws IOException, InterruptedException {
            // Check that neighbor is within the space boundaries (AKA if grid is valid)
            if (isValidGrid(gridX, max, r) && isValidGrid(gridY, max, r)) {
                // If yes, print
                context.write(new Text(gridX + "," + gridY), new Text(x + "," + y));
            }
            // If not, ignore
        }

        // Check that grid is within the space boundaries
        private boolean isValidGrid(int grid, int max, int r) {
            // No grid spaces below 1 or above max / r
            return grid > 0 && grid <= max / r;
        }
    }

    // Input: Groups by grid (includes neighboring grids)
    // Count neighbors for each point by euclidean distance with r
    // Report outliers based on k
    public static class OutlierDetectionReducer extends Reducer<Text, Text, Text, Text> {

        // Define r (radius), k (# neighbors), and list of non-outliers
        private int r;
        private int k;
        private List<String> nonOutliers;

        // Set r, k, & create non-outliers
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            r = Integer.parseInt(conf.get("r"));
            k = Integer.parseInt(conf.get("k"));
            nonOutliers = new ArrayList<>();
        }

        // Keep track of all non-outliers
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Store non-outliers
            for (Text t : values) {
                nonOutliers.add(t.toString());
            }
        }

        // Count neighbors for each point by euclidean distance with r
        // Decide outlier or not based on k
        // Report all outliers at the end
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Identify outliers
            List<String> uniqueNonOutliers = new ArrayList<>(new HashSet<>(nonOutliers));
            for (String point : uniqueNonOutliers) {
                String[] parts = point.split(",");

                // Set x, y of current point
                int x1 = Integer.parseInt(parts[0]);
                int y1 = Integer.parseInt(parts[1]);

                // Init neighbors
                int neighbors = 0;

                // Iterate over each point and count neighbors within r
                for (String otherPoint : uniqueNonOutliers) {
                    if (!point.equals(otherPoint)) {
                        String[] otherParts = otherPoint.split(",");

                        // Set x,y of other point
                        int x2 = Integer.parseInt(otherParts[0]);
                        int y2 = Integer.parseInt(otherParts[1]);

                        // Calculate euclidean distance
                        double distance = calculateDistance(x1, y1, x2, y2);

                        // If distance within the radius, count point as a neighbor
                        if (distance <= r) {
                            neighbors++;
                        }
                    }
                }

                // If the number of neighbors is less than k, print the point as an outlier
                if (neighbors < k) {
                    context.write(null, new Text(point));
                }
            }
        }

        // Calculate euclidean distance of 2 points
        private double calculateDistance(int x1, int y1, int x2, int y2) {
            return Math.sqrt(Math.pow(x2 - x1, 2) + Math.pow(y2 - y1, 2));
        }
    }

    public static void main(String[] args) throws Exception {
        // If there are less than 4 parameters
        if (args.length < 4) {
            // Print out what the parameters should be
            System.err.println("Error: Please include values for input, output, r, and k");
            System.exit(1);
        }

        // Other configs
        Configuration conf = new Configuration();
        conf.set("r", args[2]);
        conf.set("k", args[3]);
        Job job = Job.getInstance(conf, "outlier detection");
        job.setJarByClass(OutlierDetection.class);
        job.setMapperClass(PointMapper.class);
        job.setReducerClass(OutlierDetectionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}