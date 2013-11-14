import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;


public class CommunicationHandler implements Runnable {

	public CommunicationServer cServer;
	public volatile boolean running;
	public CommunicationHandler(CommunicationServer cServer) {
		this.cServer = cServer;
		running=false;
	}
	
	public void ack(int c) {
		cServer.connections.get(c).networkOut.print("ACK\n");
	}
	
	public void processRequest(String request, Connection con) throws IOException {
		if(request.startsWith("MAP")) {
			System.out.println("MAP REQUEST");
			
		}
		else if(request.startsWith("REDUCE")) {
			System.out.println("REDUCE REQUEST");
		}
		else if(request.startsWith("FILESYSTEM")) {
			if(request.startsWith("FILESYSTEM FILE_INC")) {
				//format: FILESYSTEM FILE_INC [filename],[size as int]\n{data}
				String[] split = request.split(" ");
				String filename = split[2].split(",")[0];
				int filesize = Integer.parseInt(split[2].split(",")[1]);
				byte[] data = new byte[filesize];
				try {
					con.networkIn.readFully(data);
				} catch (IOException e) {
					//Connection dun goofed
				}
				File f = new File("/tmp/" + filename);
				FileOutputStream ps = new FileOutputStream(f);
				ps.write(data);
				ps.close();
				
				
			}
		}
	}
	
	public void run() {
		running = true;
		
		while(running) {
			int c = cServer.hasData();
			if(c!=-1) {
				//handle communication with connection c
				Connection con = cServer.connections.get(c);
				String line;
				try {
					line = con.getLine();
				} catch (IOException e) {
					continue; //this is weird.
				}
				try {
					processRequest(line,con);
				} catch (IOException e) {
					//couldn't save file or other things
				}
			}
		}
	}
}
