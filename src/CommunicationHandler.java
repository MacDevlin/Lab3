import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;


public class CommunicationHandler implements Runnable {

	public CommunicationServer cServer;
	public volatile boolean running;
	public CommunicationHandler(CommunicationServer cServer) {
		this.cServer = cServer;
		running=false;
	}
	
	public void sendMessage(Connection c, byte[] message) throws IOException {
		c.networkOut.write(message);
	}
	
	public void ack(Connection c) {
		c.networkOut.print("ACK\n");
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
				System.out.println(String.format("Creating local copy of %s [%d]", filename, filesize));
				byte[] data = new byte[filesize];
				try {
					con.networkIn.readFully(data);
				} catch (IOException e) {
					//Connection dun goofed
				}
				System.out.println("TEST1");
				File f = new File(filename);
				FileOutputStream ps = new FileOutputStream(f);
				System.out.println("TEST2");
				ps.write(data);
				ps.close();
				System.out.println("TEST3");
				
				
			}
		}
		if(request.equals("ACK") == false) {
			ack(con);
		} else {
			con.acked = true;
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
			try {
				Thread.currentThread().sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
