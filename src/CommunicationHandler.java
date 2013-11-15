import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintStream;
import java.util.LinkedList;
import java.util.List;


public class CommunicationHandler implements Runnable {

	public CommunicationServer cServer;
	public volatile boolean running;
	public Scheduler scheduler;

	public List<FourFortyMapReduce> runningPrograms;
	
	public CommunicationHandler(CommunicationServer cServer) {
		this.cServer = cServer;
		running=false;
		runningPrograms = new LinkedList<FourFortyMapReduce>();
	}
	
	public void addRunningProgram(FourFortyMapReduce mr) {
		runningPrograms.add(mr);
	}
	
	public void sendMessage(Connection c, byte[] message) throws IOException {
		c.networkOut.write(message);
	}
	
	public void ack(Connection c) {
		c.networkOut.print("ACK\n");
	}
	
	public void map(String filename, int start, int recNum, Map iFun) throws IOException, InterruptedException {
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
		byte[] messageStr = ("MAP RESULT," + filename + "\n").getBytes();
		cServer.sendMessage(0, messageStr);
		System.out.println("Map Complete");
		
	}
	
	public void processRequest(String request, Connection con) throws IOException, InterruptedException {
		if(request.startsWith("MAP REQUEST")) {
			System.out.println("MAP REQUEST");
			String[] split = request.split(",");
			int id = Integer.parseInt(split[1]);
			String filename = split[2];
			Integer startRec = Integer.parseInt(split[3]);
			Integer recNum = Integer.parseInt(split[4]);
			Integer size = Integer.parseInt(split[5]);
			byte[] funcData = new byte[size];
			con.networkIn.readFully(funcData);//read in the IFunction
			if(scheduler != null) {
				scheduler.schedule(true, con, id, filename,startRec,recNum,funcData);
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
			Map iFun = null;
			try {
				iFun = reconstituteMap(funcData);
			} catch (ClassNotFoundException e) {
				System.out.println("Could not reconstitute function to map");
			}
			//ack the assignment
			ack(con);
			//apply the function...
			Mapper m = new Mapper(cServer, filename,startRec,recNum,iFun);
			Thread t = new Thread(m);
			t.start();
			//map(filename,startRec,recNum,iFun);
			return;
		}
		else if(request.startsWith("REDUCE REQUEST")) {
			System.out.println("REDUCE REQUEST");
			String[] split = request.split(",");
			Integer id = Integer.parseInt(split[1]);
			String filename = split[2];
			Integer startRec = Integer.parseInt(split[3]);
			Integer recNum = Integer.parseInt(split[4]);
			Integer size = Integer.parseInt(split[5]);
			byte[] funcData = new byte[size];
			con.networkIn.readFully(funcData);//read in the IFunction
			if(scheduler != null) {
				scheduler.schedule(false, con,id,filename,startRec,recNum,funcData);
			}
			
		}
		else if(request.startsWith("REDUCE ASSIGN")) {
			
			String[] split = request.split(",");
			String filename = split[1];
			Integer startRec = Integer.parseInt(split[2]);
			Integer recNum = Integer.parseInt(split[3]);
			Integer size = Integer.parseInt(split[4]);
			System.out.println("REDUCE ASSIGN: " + filename + " (" + startRec + " - " + recNum + ")");
			byte[] funcData = new byte[size];
			con.networkIn.readFully(funcData);//read in the IFunction
			Reduce iFun = null;
			try {
				iFun = reconstituteReduce(funcData);
			} catch (ClassNotFoundException e) {
				System.out.println("Could not reconstitute function to map");
			}
			//ack the assignment
			ack(con);
			//apply the function...
			Reducer red = new Reducer(cServer, filename, startRec, recNum, iFun);
			Thread t = new Thread(red);
			t.start();
			//reduce(filename,startRec,recNum,iFun);
			return;
		}
		else if(request.startsWith("REDUCE RESULT")) {
			System.out.println("REDUCE RESULT");
			String[] split = request.split(",");
			String filename = split[1];
			String result = con.networkIn.readLine();
			scheduler.logResult(false, con,filename,result);
		}
		else if(request.startsWith("MAP RESULT")) {
			System.out.println("MAP RESULT");
			String[] split = request.split(",");
			String filename = split[1];
			ack(con);
			scheduler.logResult(true, con,filename,"");
			return;
		}
		else if(request.startsWith("REDUCE COMPLETE")) {
			System.out.println("REDUCE COMPLETE");
			String[] split = request.split(",");
			Integer id = Integer.parseInt(split[1]);
			String result = split[2];
			System.out.println(id + ": " + result);
			//NEED TO RETURN THIS TO THE CALLER
			runningPrograms.get(id).reduceComplete=true;
			runningPrograms.get(id).reduceResult=result;
		}
		else if(request.startsWith("MAP COMPLETE")) {
			System.out.println("MAP COMPLETE");
			String[] split = request.split(",");
			Integer id = Integer.parseInt(split[1]);
			runningPrograms.get(id).mapComplete=true;
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
	
	private Map reconstituteMap(byte[] data) throws IOException, ClassNotFoundException {
        File f = new File("reconstituting.ser");
        if(f.exists()) {
                f.delete();
        }
        FileOutputStream fout = new FileOutputStream("reconstituting.ser");
        fout.write(data);
        FileInputStream fin = new FileInputStream("reconstituting.ser");
        
        ObjectInputStream in = new ObjectInputStream(fin);
        Map iFun = (Map)in.readObject();
        return iFun;
	}
	
	public Reduce reconstituteReduce(byte[] data) throws IOException, ClassNotFoundException {
        File f = new File("reconstituting.ser");
        if(f.exists()) {
                f.delete();
        }
        FileOutputStream fout = new FileOutputStream("reconstituting.ser");
        fout.write(data);
        FileInputStream fin = new FileInputStream("reconstituting.ser");
        
        ObjectInputStream in = new ObjectInputStream(fin);
        Reduce iFun = (Reduce)in.readObject();
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
