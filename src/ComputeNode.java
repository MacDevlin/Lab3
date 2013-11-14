import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
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
	
	public static void processConsole(String request) {
		if(request.startsWith("launch")) {
			String[] split = request.split(" ");
			String[] args = new String[split.length - 2];
			System.arraycopy(split, 2, args, 0, args.length);
			FourFortyMapReduce ffMR = new FourFortyMapReduce(cServer);
			//class file is split[1], arguments are the rest
			
			//TODO: why the hell is this required
			split[1] = split[1].substring(0,split[1].length()-1);
			
			try {
				Class<?> c = Class.forName(split[1]);
				Constructor<?> con = c.getConstructor(FourFortyMapReduce.class, String[].class);
				Runnable e = (Runnable)con.newInstance((Object)ffMR, (Object[])args);
				Thread th = new Thread(e);//requires test class to be a runnable
				th.start();//run the program
			} catch (ClassNotFoundException e) {
				System.out.println("Class could not be found: " + split[1] + "test");
			} catch (NoSuchMethodException e) {
				System.out.println("Class does not take string[] in constructor");
			} catch (SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void startShell() {
		String request = "";
		
		while(true) {
			try {
				if(System.in.available()>0) {
					char readByte = (char)System.in.read();
					if(readByte != '\n') {
						request = request + readByte;
					}
					else {
						//end of request
						processConsole(request);
						request = "";
					}
				}
			} catch (IOException e) {
				
			}
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
			cServer.addConnection(master);
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
		startShell();
	}
}
