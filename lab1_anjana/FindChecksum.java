
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;



public class FindChecksum {

	public static void main(String[] args)  throws Exception 
	{

		// The system configuration
		Configuration conf = new Configuration();
		// Get an instance of the Filesystem
		FileSystem fs = FileSystem.get(conf);
		// Specify the path to the bigdata file on the Hadoop FS
		String path_name = "/lab1/experiment2/bigdata";
		Path path = new Path(path_name);

		//Open the contents of the file
		FSDataInputStream file = fs.open(path);
		
	    //Since the total data to be stored is from 1000000000-1000000999, buffer size 1000
	    byte buffer[] = new byte[1000];
	    // This reads the file starting at a particular location, till the buffer ends
	    file.readFully(1000000000L,buffer);
	   

	    // Logic for finding checksum of all data in that range
	    int checksum = buffer[0];
	    for (int i = 1; i < buffer.length; i++)
	    {
	      checksum ^= buffer[i];
	    }    
	    
	    // Print the checksum value
	    System.out.println("The Checksum obtained is : " + checksum);    
		file.close();
		fs.close();
	}
		// TODO Auto-generated method stub

	

}

