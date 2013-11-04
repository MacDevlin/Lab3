import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class ComputeNode {
	
	public static Map<String,String> node_config;
	public static void loadComputeNodeConfig() {
		File f = new File("./node_config.conf");
		try {
			//TODO: This might be a shitty way to create a DataInputStream
			DataInputStream fi = new DataInputStream(new FileInputStream(f));
			node_config = new HashMap<String,String>();
			while(fi.available()>0) {
				//split on =
				String line = fi.readLine();
				String[] split = line.split("=");
				node_config.put(split[0], split[1]);
			}
		} catch (FileNotFoundException e) {
			System.out.println("Could not find node_config.conf, exiting");
			System.exit(1);
		} catch (IOException e) {
			System.out.println("Could not read node_config.conf, exiting");
			System.exit(1);
		}
	}
	
	public static void main(String[] args) {
		System.out.println("Loading configuration file");
		loadComputeNodeConfig();
		System.out.println("Connecting to master");
		
	}
}
