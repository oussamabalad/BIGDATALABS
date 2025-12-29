package edu.supmti.hadoop;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class ReadHDFS {
    public static void main(String[] args) {
        Configuration conf = new Configuration();

        // Essayons avec try-with-resources
        try (FileSystem fs = FileSystem.get(conf);
             FSDataInputStream inStream = fs.open(new Path("/user/root/achats.txt"));
             BufferedReader br = new BufferedReader(new InputStreamReader(inStream))) {

            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }

        } catch (IOException e) {
            System.err.println("Error reading file from HDFS: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
