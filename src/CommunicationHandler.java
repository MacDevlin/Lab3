import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintStream;


public class CommunicationHandler implements Runnable {

	public CommunicationServer cServer;
	public volatile boolean running;
	public Scheduler scheduler;
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
	
	public void map(String filename, int start, int recNum, IFunction iFun) throws IOException {
		File f = new File(filename);
		File f2 = new File(filename+"_map");
		DataInputStream fi = new DataInputStream(new FileInputStream(f));
		PrintStream ps = new PrintStream(f2);
		//TODO: stop at end
		int lineNum = 0;
		while(fi.available()>0) {
			String line = fi.readLine();
			if(lineNum >= start && lineNum <=start+recNum) {
				String newLine = iFun.execute(line);
				ps.println(newLine);
			}
			lineNum++;
		}
		fi.close();
		ps.close();
		System.out.println("Map Complete");
		
	}
	
	public void processRequest(String request, Connection con) throws IOException, InterruptedException {
		if(request.startsWith("MAP REQUEST")) {
			System.out.println("MAP REQUEST");
			String[] split = request.split(",");
			String filename = split[1];
			Integer startRec = Integer.parseInt(split[2]);
			Integer recNum = Integer.parseInt(split[3]);
			Integer size = Integer.parseInt(split[4]);
			byte[] funcData = new byte[size];
			con.networkIn.readFully(funcData);//read in the IFunction
			//test reconstitute
			try {
				IFunction iFun = reconstitute(funcData);
			} catch (ClassNotFoundException e) {
				System.out.println("Could not reconstitute function to map");
			}
			if(scheduler != null) {
				scheduler.schedule(con,filename,startRec,recNum,funcData);
			}
		}
		else if(request.startsWith("MAP ASSIGN")) {
			
			String[] split = request.split(",");
			String filename = split[1];
			Integer startRec = Integer.parseInt(split[2]);
			Integer recNum = Integer.parseInt(split[3]);
			Integer size = Integer.parseInt(split[4]);
			System.out.println("MAP ASSIGN: " + filename + " (" + startRec + " - " + recNum + ")");
			byte[] funcData = new byte[size];
			con.networkIn.readFully(funcData);//read in the IFunction
			IFunction iFun = null;
			try {
				iFun = reconstitute(funcData);
			} catch (ClassNotFoundException e) {
				System.out.println("Could not reconstitute function to map");
			}
			//ack the assignment
			ack(con);
			//apply the function...
			map(filename,startRec,recNum,iFun);
			return;
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
				}
				File f = new File("/tmp/" + filename);
				FileOutputStream ps = new FileOutputStream(f);
				ps.write(data);
				ps.close();
				
				
			}
		}
		if(request.equals("ACK") == false) {
			ack(con);
		} else {
			con.acked = true;
		}
	}
	
	private IFunction reconstitute(byte[] data) throws IOException, ClassNotFoundException {
        File f = new File("reconstituting.ser");
        if(f.exists()) {
                f.delete();
        }
        FileOutputStream fout = new FileOutputStream("reconstituting.ser");
        fout.write(data);
        FileInputStream fin = new FileInputStream("reconstituting.ser");
        
        ObjectInputStream in = new ObjectInputStream(fin);
        IFunction iFun = (IFunction)in.readObject();
        return iFun;
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
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
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
