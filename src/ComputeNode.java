import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;


public class ComputeNode {
	
	public static Map<String,String> node_config;
	public static CommunicationServer cServer;
	public static Thread cServerThread;
	public static Connection master;
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
		try {
			cServer = new CommunicationServer(Integer.parseInt(node_config.get("port")));
			cServerThread = new Thread(cServer);
			cServerThread.start();
		} catch (NumberFormatException e) {
			System.out.println("Invalid port number in config file");
			System.exit(1);
		} catch (IOException e) {
			System.out.println("Socket connection exception");
			System.exit(1);
		}
		try {
			master = new Connection(node_config.get("master_ip"), Integer.parseInt(node_config.get("master_port")));
		} catch (NumberFormatException e) {
			System.out.println("Bad port in config file");
			System.exit(1);
		} catch (UnknownHostException e) {
			System.out.println("Could not find master");
			System.exit(1);
		} catch (IOException e) {
			System.out.println("Socket exception connecting to master");
			System.exit(1);
		}
		
	}
}
