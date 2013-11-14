import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class FileSystem implements Runnable{

	String[] filenames;
	List<File> files;
	String ip;
	int port;
	boolean isMaster;
	CommunicationServer cServer;
	public Map<String,String> filesystem_config;
	public FileSystem(String ip, int port, CommunicationServer cServer, boolean isMaster) {
		files = new ArrayList<File>();
		this.ip = ip;
		this.port = port;
		this.cServer=cServer;
		this.isMaster = isMaster;
		filesystem_config = new HashMap<String,String>();
	}
	
	public File createFile(String filename) {
		int id = files.size();
		File f = null;
		files.add(f);
		return f;
	}
	
	public DistFile makeLocal(File f) {
		return null;
	}
	
	private void sendFileIncMessage(DistFile f, int c) throws IOException, InterruptedException {
		System.out.println("Sending FILE INC message");
		byte[] messageStr = ("FILESYSTEM FILE_INC " + f.filename + "," + f.getSize() + "\n").getBytes();
		int size = messageStr.length;
		byte[] message = new byte[size + f.getSize()];
		ByteBuffer target = ByteBuffer.wrap(message);
		target.put(messageStr);
		target.put(f.getBytes());
		
		
		
		cServer.sendMessage(c,message);
		System.out.println("TEST");
		
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
			for(int r=0; r<rep-1; r++)
			{
				DistFile f = new DistFile(file,r,lines,numLines/rep*r,numLines/rep);
				sendFileIncMessage(f,node%cServer.participants());
				node++;
			}
			DistFile f = new DistFile(file,rep-1,lines,numLines/rep*(rep-1),numLines/rep+numLines%rep);
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
