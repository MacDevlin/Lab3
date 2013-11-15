import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class FileSystem implements Runnable{

	String[] filenames;
	List<File> files;
	String ip;
	int port;
	boolean isMaster;
	CommunicationServer cServer;
	public Map<String,String> filesystem_config;
	Map<String,Integer> fileLocations;
	Map<String,Integer> fileLines;
	public int repFactor;
	public String path = "/tmp/";
	public FileSystem(String ip, int port, CommunicationServer cServer, boolean isMaster) {
		files = new ArrayList<File>();
		this.ip = ip;
		this.port = port;
		this.cServer=cServer;
		this.isMaster = isMaster;
		filesystem_config = new HashMap<String,String>();
		fileLocations = new HashMap<String,Integer>();
		fileLines = new HashMap<String,Integer>();
		repFactor = -1;
	}
	
	
	
	public void addRecord(String filename, int c) {
		fileLocations.put(filename, c);
	}
	
	public int getFileLocation(String filename) {
		return fileLocations.get(filename);
	}
	
	public int getFileLocation(String filename, int part) {
		return fileLocations.get(filename + "_" + part);
	}
	
	public int getFileLineNum(String filename) {
		return fileLines.get(filename);
	}
	
	private void sendFileIncMessage(DistFile f, int c) throws IOException, InterruptedException {
		byte[] messageStr = ("FILESYSTEM FILE_INC " + f.filename + "," + f.getSize() + "\n").getBytes();
		int size = messageStr.length;
		byte[] message = new byte[size + f.getSize()];
		//System.out.println("Sending FILE INC message" + f.getSize());
		System.arraycopy(messageStr, 0, message, 0, messageStr.length);
		byte[] fileData = f.getBytes();
		System.arraycopy(fileData, 0, message, messageStr.length, fileData.length);
		cServer.sendMessage(c,message);
		addRecord(f.filename, c);
		
	}
	
	private void splitUpFiles(int rep, String[] filesStr) throws IOException, InterruptedException {
		int node = 0;
		for(int i=0; i < filesStr.length; i++) {
			String file = filesStr[i];
			FileInputStream fi = new FileInputStream(file);
			
			DataInputStream data = new DataInputStream(fi);
			List<String> lines = new ArrayList<String>();
			while(data.available()>0) {
				lines.add(data.readLine());
			}
			int numLines = lines.size();
			fileLines.put(file, numLines);
			for(int r=0; r<rep-1; r++)
			{

				DistFile f = new DistFile(path,file,r,lines,numLines*r/(rep),numLines/rep);
				sendFileIncMessage(f,node%cServer.participants());

				node++;
			}
			DistFile f = new DistFile(path,file,rep-1,lines,numLines*(rep-1)/rep,numLines/rep+numLines%rep);//account for a remainder
			sendFileIncMessage(f,node%cServer.participants());
			node++;
		}
	}
	
	public void loadConfig() {
		File f = new File("./filesystem_config.conf");
		try {
			//TODO: This might be a shitty way to create a DataInputStream
			DataInputStream fi = new DataInputStream(new FileInputStream(f));
			filesystem_config = new HashMap<String,String>();
			while(fi.available()>0) {
				//split on =
				String line = fi.readLine();
				String[] split = line.split("=");
				filesystem_config.put(split[0], split[1]);
				if(split[0].equals("files")) {
					filenames = split[1].split(",");
				}
			}

		} catch (FileNotFoundException e) {
			System.out.println("Could not find filesystem_config.conf, exiting");
			System.exit(1);
		} catch (IOException e) {
			System.out.println("Could not read filesystem_config.conf, exiting");
			System.exit(1);
		}
		repFactor = Integer.parseInt(filesystem_config.get("replication_factor"));
	}
	
	public void printLocations() {
		Iterator<String> locs = fileLocations.keySet().iterator();
		while(locs.hasNext()) {
			String loc = locs.next();
			System.out.println(String.format("File %s: %s", loc, (cServer.connections.get(fileLocations.get(loc))).asString()));
		}
	}
	
	public void run() {
		if(isMaster) {
			//wait for all participants, then initialize all files
			loadConfig();
			System.out.println("Waiting on all participants...");
			int participants = Integer.parseInt(filesystem_config.get("participants"));
			while (cServer.connections.size() < participants) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
			System.out.println("All participants connected, initializing file system");
			int rep = Integer.parseInt(filesystem_config.get("replication_factor"));
			try {
				splitUpFiles(rep,filenames);
			} catch (IOException e) {
				System.out.println("Could not split up files");
				System.exit(1);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
}
