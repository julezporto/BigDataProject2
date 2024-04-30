import java.io.IOException;
import javax.naming.Context;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// DS503 Project 2, Question 1 (Step 2): Spatial Join
public class SpatialJoin {

    // Take in point dataset and prep for join
    public static class PointMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Takes in a single point "x,y" and turns it into a string
            String record = value.toString();

            // Divides record string into pieces based on commas
            // Point record: [0] x, [1] y
            String[] parts = record.split(",");

            // Sends needed info through key-value pair:
            // key = [0] x, [1] y
            // value = "point", [0] x, [1] y
            context.write(new Text(parts[0] + "," + parts[1]), new Text("point" + "," + parts[0] + "," + parts[1]));
        }
    }

    // Take in rectangle dataset and prep for join
    public static class RectangleMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Takes in a single rectangle "bottomLeft_x,bottomleft_y,height,width" and turns it into a string
            String record = value.toString();

            // Divides record string into pieces based on commas
            // Rectangle record: [0] bottomLeft_x, [1] bottomleft_y, [2] height, [3] width
            String[] parts = record.split(",");

            // Set values of rectangle provided
            int bottomLeft_x = Integer.parseInt(parts[0]);
            int bottomleft_y = Integer.parseInt(parts[1]);
            int height = Integer.parseInt(parts[2]);
            int width = Integer.parseInt(parts[3]);

            // Get max x & y from provided values
            int max_x = bottomLeft_x + width;
            int max_y = bottomleft_y + height;

            // Loop through all points in rectangle based on min and max x & y values to produce key-value pairs
            for (int current_x = bottomLeft_x; current_x <= max_x; current_x++) {
                for (int current_y = bottomleft_y; current_y <= max_y; current_y++) {
                    // Sends each point in rectangle through key-value pairs:
                    // key = current_x, current_y
                    // value = "rectangle", [0] bottomLeft_x, [1] bottomleft_y, [2] height, [3] width
                    context.write(new Text(current_x + "," + current_y), new Text("rectangle" + "," + current_x + "," + current_y + "," + bottomLeft_x + "," + bottomleft_y + "," + height + "," + width));
                }
            }
        }
    }

    // Joins points and rectangles by overlapping points
    public static class SpatialJoinReducer extends Reducer<Text, Text, Text, Text> {

        // Create window variables
        private int windowX1, windowY1, windowX2, windowY2;

        // Set window based on optional W(x1,y1,x2,y2) parameter
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Get window parameters from config (strings)
            Configuration conf = context.getConfiguration();
            String windowX1Str = conf.get("windowX1");
            String windowY1Str = conf.get("windowY1");
            String windowX2Str = conf.get("windowX2");
            String windowY2Str = conf.get("windowY2");

            // Set window parameters (ints)
            windowX1 = Integer.parseInt(windowX1Str);
            windowY1 = Integer.parseInt(windowY1Str);
            windowX2 = Integer.parseInt(windowX2Str);
            windowY2 = Integer.parseInt(windowY2Str);
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // Create list for poitns & rectangles
            List<int[]> points = new ArrayList<>();
            List<int[]> rectangles = new ArrayList<>();

            // For each input...
            for (Text t : values) {
                // Separate fields by commas
                String parts[] = t.toString().split(",");

                // If point...
                if (parts[0].equals("point")) {
                    // Add point to point list
                    points.add(new int[] { Integer.parseInt(parts[1]), Integer.parseInt(parts[2]) });
                }
                // If rectangle...
                else if (parts[0].equals("rectangle")) {
                    // Add the rectangle to rectangle list
                    rectangles.add(new int[] {
                            Integer.parseInt(parts[1]),
                            Integer.parseInt(parts[2]),
                            Integer.parseInt(parts[3]),
                            Integer.parseInt(parts[4]),
                            Integer.parseInt(parts[5]),
                            Integer.parseInt(parts[6])
                    });
                }
            }

            // Loop over each point and rectangle pair to check for overlapping coordinate
            for (int[] point : points) {
                for (int[] rectangle : rectangles) {
                    // If same x & y value
                    if (point[0] == rectangle[0] && point[1] == rectangle[1]) {
                        // If within the specified window
                        if (point[0] >= windowX1 && point[0] <= windowX2 && point[1] >= windowY1 && point[1] <= windowY2) {
                            // Print output: bottom_x, bottom_y, height, width, (point_x, point_y)
                            String result = String.format("%d,%d,%d,%d,(%d,%d)", rectangle[2], rectangle[3], rectangle[4], rectangle[5], point[0], point[1]);
                            context.write(null, new Text(result));
                        }
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // If there are less than 3 or more than 4 parameters
        if (args.length < 3 || args.length > 4) {
            // Print out what the parameters should be
            System.err.println(
                    "Please include: <inputPoints> <inputRectangles> <output> <W(windowX1,windowY1,windowX2,windowY2)> with W() in quotes.");
            System.exit(1);
        }

        // Set default values for window parameters (AKA the whole space)
        conf.set("windowX1", "0");
        conf.set("windowY1", "0");
        conf.set("windowX2", "10000");
        conf.set("windowY2", "10000");

        // If there was a 4th parameter included...
        if (args.length == 4) {
            String windowParam = args[3];
            // If it is in the right W(windowX1,windowY1,windowX2,windowY2) format...
            if (windowParam.matches("^W\\(\\d+,\\d+,\\d+,\\d+\\)$")) {
                // Remove "W(" and ")" from it
                windowParam = windowParam.substring(2, windowParam.length() - 1);

                // Split the window parameters into their own individual parts
                String[] windowParams = windowParam.split(",");

                // Get and set all window parameters (strings)
                if (windowParams.length == 4) {
                    conf.set("windowX1", windowParams[0]);
                    conf.set("windowY1", windowParams[1]);
                    conf.set("windowX2", windowParams[2]);
                    conf.set("windowY2", windowParams[3]);
                } else {
                    System.err.println("Wrong number of window parameters");
                    System.exit(1);
                }
            } else {
                System.err.println("Wrong window parameter format");
                System.exit(1);
            }
        }

        // Other configs
        Job job = Job.getInstance(conf, "spatial join");
        job.setJarByClass(SpatialJoin.class);
        job.setReducerClass(SpatialJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PointMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RectangleMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}