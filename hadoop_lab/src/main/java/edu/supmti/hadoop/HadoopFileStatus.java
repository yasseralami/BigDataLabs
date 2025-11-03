package edu.supmti.hadoop;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopFileStatus {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs =FileSystem.get(conf);
        if (args.length < 1) {
            System.err.println("Usage: HadoopFileStatus <file-path>");
            System.exit(1);
        }
        Path path = new Path(args[0]);//"/user/root/input/alice.txt"
        FileStatus status = fs.getFileStatus(path);
        System.out.println("File: " + status.getPath());
        System.out.println("Owner: " + status.getOwner());
        System.out.println("Group: " + status.getGroup());
        System.out.println("Size: " + status.getLen());
        System.out.println("Modification Time: " + status.getModificationTime());
        BlockLocation[] blockLocations = fs.getFileBlockLocations(status, 0, status.getLen());
        for (BlockLocation blockLocation : blockLocations) {
            System.out.println("Block Offset: " + blockLocation.getOffset());
            System.out.println("Block Length: " + blockLocation.getLength());
            System.out.println("Hosts: " + String.join(", ", blockLocation.getHosts()));
        }
    }

}
