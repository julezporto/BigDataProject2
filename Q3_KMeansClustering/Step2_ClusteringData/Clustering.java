import java.awt.Point;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import javax.naming.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// DS503 Project 2, Question 3 (Step 2): Clustering
public class Clustering {

    // Take in point dataset and assign closest centroid
    public static class PointMapper extends Mapper<Object, Text, Text, Text> {

        // Define k (number of centroids) & list of centroids
        private int k;
        private List<Point> centroids;

        // Set k & list of centroids
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();

            // Set k
            String kString = conf.get("k");
            k = Integer.parseInt(kString);

            // Set list of k centroids
            centroids = generateRandomCentroids(k);
        }

        // Generate k random centroids
        private List<Point> generateRandomCentroids(int k) {
            Random random = new Random();
            List<Point> centroidList = new ArrayList<>();

            for (int i = 0; i < k; i++) {
                // Generate a random x & y
                int randomX = random.nextInt(10) + 1;
                int randomY = random.nextInt(10) + 1;

                // Add point to centroids list
                centroidList.add(new Point(randomX, randomY));
            }
            return centroidList;
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Takes in a single point "x,y" and turns it into a string
            String record = value.toString();

            // Divides record string into parts based on commas
            String[] parts = record.split(",");

            // Set values of point: parts[0] = x, parts[1] = y
            int x = Integer.parseInt(parts[0]);
            int y = Integer.parseInt(parts[1]);

            // Create current point
            Point currentPoint = new Point(x, y);

            // Find closest centroid to current point
            Point closestCentroid = findClosestCentroid(currentPoint);

            // Print output for the current point:
            // key = centroicX, centroidY
            // value = x, y
            context.write(new Text(closestCentroid.getX() + "," + closestCentroid.getY()),
                    new Text(x + "," + y));
        }

        // Find closest centroid to point
        private Point findClosestCentroid(Point currentPoint) {
            // Define min dist and closest centroid
            double minDistance = Double.MAX_VALUE;
            Point closestCentroid = null;

            // For each centroid...
            for (Point centroid : centroids) {
                // Calculate the distance to the current point
                double distance = calculateEuclideanDistance(
                        currentPoint.getX(), currentPoint.getY(),
                        centroid.getX(), centroid.getY());

                // If that distance is less than the min distance...
                if (distance < minDistance) {
                    // Set it to the min distance
                    minDistance = distance;
                    // Set the closest centroid to that centroid
                    closestCentroid = centroid;
                }
            }
            return closestCentroid;
        }

        // Calculate euclidean distance between 2 points
        private double calculateEuclideanDistance(double x1, double y1, double x2, double y2) {
            return Math.sqrt(Math.pow(x2 - x1, 2) + Math.pow(y2 - y1, 2));
        }
    }

    // Input: Groups by closest centroid (AKA cluster)
    // Compute new centroids
    public static class ClusteringReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // Set vars to re-compute centroid
            double totalX = 0;
            double totalY = 0;
            int num = 0;

            for (Text t : values) {
                // Separate fields by commas
                String parts[] = t.toString().split(",");

                // Set x & y
                int x = Integer.parseInt(parts[0]);
                int y = Integer.parseInt(parts[1]);

                // Add to totals
                totalX += x;
                totalY += y;

                // Increment counter
                num++;
            }

            // Calculate new centroid
            double averageX = totalX / num;
            double averageY = totalY / num;

            // Print output (new centroid) for current cluster:
            // key = null
            // value = centroicX, centroidY
            String result = String.format("%f, %f", averageX, averageY);
            context.write(null, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {

        // If incorrect number of parameters
        if (args.length != 3) {
            // Print out what the parameters should be
            System.err.println("Error: Please include values for input, output, and k");
            System.exit(1);
        }

        // Get k param
        Configuration conf = new Configuration();
        conf.set("k", args[2]);

        // Iteration control
        // Define iteration counter
        int iteration = 0;
        // Define centroids changed bool
        boolean centroidsChanged;

        // Run each iteration until the 6th or the centroids are all unchanged
        do {
            centroidsChanged = runIteration(conf, args[0], args[1], iteration);
            // Increment iteration counter
            iteration++;
        } while (iteration < 6 && centroidsChanged);

        System.exit(0);
    }

    // Run each iteration (map-reduce job to assign clusters and re-compute
    // centroids)
    private static boolean runIteration(Configuration conf, String inputPath, String outputPath, int iteration)
            throws IOException, ClassNotFoundException, InterruptedException {

        // One job per each iteration
        Job job = Job.getInstance(conf, "clustering-iteration-" + iteration);

        // other configs
        job.setJarByClass(Clustering.class);
        job.setMapperClass(PointMapper.class);
        job.setReducerClass(ClusteringReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        // Different outputs for each iteration
        FileOutputFormat.setOutputPath(job, new Path(outputPath + "/iteration_" + iteration));

        return job.waitForCompletion(true);
    }
}