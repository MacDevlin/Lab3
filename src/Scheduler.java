import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class Scheduler implements Runnable {
	
	public List<MapRequest> requests;
	public FileSystem fSystem;
	public CommunicationServer cServer;
	public Scheduler(FileSystem fSystem, CommunicationServer cServer) {
		this.fSystem = fSystem;
		this.cServer = cServer;
		requests = new LinkedList<MapRequest>();
	}
	
	public void schedule(Connection con, String filename, int startRecord, int recNum, byte[] funcData) throws IOException, InterruptedException {
		MapRequest mr = new MapRequest(con,filename,startRecord,recNum,funcData);
		requests.add(mr);
	}
	
	public void doRequest() throws IOException, InterruptedException {
		
		//for now, just assign work to the nodes with the files needed
		int rep = Integer.parseInt(fSystem.filesystem_config.get("replication_factor"));
		MapRequest mr = requests.get(0);
		
		int fileLines = fSystem.getFileLineNum(mr.filename);
		int startPart = ((mr.startRec)*rep+1)/fileLines;
		int endPart = (int)Math.ceil((float)(mr.startRec+mr.recNum+1)*rep/fileLines);
		
		
		//int endPart = (int)Math.ceil((float)(mr.startRec+mr.recNum-1)*rep/fileLines);
		int partLen = fileLines/rep;
		
		
		//tell each compute node to map
		for(int i=startPart; i<rep; i++) { //endPart; i++) {
			int c = fSystem.getFileLocation(mr.filename,i);
			System.out.println("Assigning part " + i + " to " + c);
			int partStart = i*partLen;
			int relStart = Math.max(mr.startRec - partStart, 0);
			int relLen = (mr.startRec+mr.recNum - partStart-1);
			if(relLen < 0) continue;
			if(i != rep-1) {
				//not in last part
				if(relLen > partLen-1) {
					relLen = partLen-1;
				}
			}
			byte[] messageStr = ("MAP ASSIGN," + fSystem.path + mr.filename + "_" + i + "," + relStart + "," + relLen + "," + mr.funcData.length + "\n").getBytes();
			byte[] message = new byte[messageStr.length + mr.funcData.length];
			System.arraycopy(messageStr, 0, message, 0, messageStr.length);
			System.arraycopy(mr.funcData, 0, message, messageStr.length, mr.funcData.length);
			cServer.sendMessage(c, message);
		}
		System.out.println("schedule complete");
		requests.remove(0);
	}
	
	public void run() {
		while(true) {
			if(requests.size() > 0) {
				try {
					doRequest();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			try {
				Thread.currentThread().sleep(25);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
}
