import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;


public class MasterNode {
	
	public static Map<String,String> master_config;
	public static Scheduler scheduler;
	
	public static void loadMasterNodeConfig() {
		File f = new File("./master_configuration.conf");
		try {
			//TODO: This might be a shitty way to create a DataInputStream
			DataInputStream fi = new DataInputStream(new FileInputStream(f));
			master_config = new HashMap<String,String>();
			while(fi.available()>0) {
				//split on =
				String line = fi.readLine();
				String[] split = line.split("=");
				master_config.put(split[0], split[1]);
			}
		} catch (FileNotFoundException e) {
			System.out.println("Could not find master_configuration.conf, exiting");
			System.exit(1);
		} catch (IOException e) {
			System.out.println("Could not read master_configuration.conf, exiting");
			System.exit(1);
		}
	}
	
	
	public static void main(String[] args) {
		System.out.println("Reading configuration data");
		loadMasterNodeConfig();
		System.out.println("Loading Scheduler");
		scheduler = new Scheduler();
	}
}
