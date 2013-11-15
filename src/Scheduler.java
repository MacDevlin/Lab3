import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


public class Scheduler implements Runnable {
	
	public List<MapRequest> requests;
	public FileSystem fSystem;
	public CommunicationServer cServer;
	public Map<String,ReduceRequest> activeReduces;
	public Map<String,MapRequest> activeMaps;
	public Scheduler(FileSystem fSystem, CommunicationServer cServer) {
		this.fSystem = fSystem;
		this.cServer = cServer;
		requests = new LinkedList<MapRequest>();
		activeReduces = new HashMap<String,ReduceRequest>();
		activeMaps = new HashMap<String,MapRequest>();
		
	}
	
	public synchronized void logResult(boolean isMap, Connection con, String filename, String result) {
		int part = Integer.parseInt(filename.substring(filename.lastIndexOf("_") + 1));
		String baseName = filename.substring(0,filename.lastIndexOf("_")).substring(filename.lastIndexOf("/")+1);
		if(!isMap) {
			ReduceRequest rr = activeReduces.get(baseName);
			rr.results[part-rr.startPart] = result;
			rr.resultNum++;
		} else {
			MapRequest mr = activeMaps.get(baseName);
			mr.completeParts++;
		}
	}
	
	public void schedule(boolean isMap, Connection con, int id, String filename, int startRecord, int recNum, byte[] funcData) throws IOException, InterruptedException {
		MapRequest mr = new MapRequest(isMap, con, id, filename,startRecord,recNum,-1,funcData);
		if(isMap) {
			activeMaps.put(filename, mr);
		}
		requests.add(mr);
	}
	
	public void doRequest() throws IOException, InterruptedException, ClassNotFoundException {
		
		//for now, just assign work to the nodes with the files needed
		int rep = Integer.parseInt(fSystem.filesystem_config.get("replication_factor"));
		MapRequest mr = requests.get(0);
		
		int fileLines = fSystem.getFileLineNum(mr.filename);
		int startPart = ((mr.startRec)*rep+1)/fileLines;
		//endPart is the last part of the file that needs to be mapped/reduced
		int linesPerPart = fileLines/rep;
		int endPart = (mr.startRec+mr.recNum-1)/linesPerPart;
		
		
		if(!mr.isMap) {
			ReduceRequest rr = new ReduceRequest(cServer.getNumber(mr.requester), mr.id, endPart - startPart+1, startPart, cServer.cHandler.reconstituteReduce(mr.funcData));
			activeReduces.put(mr.filename, rr);
		} else {
			mr.totalParts = endPart - startPart + 1;
		}
		
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
				if(relLen+relStart > partLen-1) {
					relLen = partLen-1-relStart;
				}
			}
			if(!mr.isMap) {
				relStart = 0; //accounted for by the map not including those lines
			}
			byte[] messageStr = null;
			if(mr.isMap){ 
				messageStr = ("MAP ASSIGN," + fSystem.path + mr.filename + "_" + i + "," + relStart + "," + relLen + "," + mr.funcData.length + "\n").getBytes();
			} else {
				messageStr = ("REDUCE ASSIGN," + fSystem.path + mr.filename + "_" + i + "," + relStart + "," + relLen + "," + mr.funcData.length + "\n").getBytes();
			}
			byte[] message = new byte[messageStr.length + mr.funcData.length];
			System.arraycopy(messageStr, 0, message, 0, messageStr.length);
			System.arraycopy(mr.funcData, 0, message, messageStr.length, mr.funcData.length);
			cServer.sendMessage(c, message);
		}
		System.out.println("schedule complete");
		requests.remove(0);
	}
	
	public void checkResults() throws IOException, InterruptedException {
		//check for completed reduces
		Iterator<Entry<String,ReduceRequest>> it = activeReduces.entrySet().iterator();
		while(it.hasNext()) {
			Entry<String,ReduceRequest> e = it.next();
			ReduceRequest rr = e.getValue();
			String filename = e.getKey();
			if(rr.resultNum == rr.results.length) {
				//done
				String answer = rr.results[0];
				for(int r=1; r<rr.results.length; r++) {
					answer = rr.func.execute(answer,rr.results[r]);
				}
				
				//send message
				byte[] messageStr = ("REDUCE COMPLETE," + rr.id + "," + answer + "\n").getBytes();
				cServer.sendMessage(rr.caller, messageStr);
				activeReduces.remove(filename);
			}
		}
		Iterator<Entry<String,MapRequest>> it2 = activeMaps.entrySet().iterator();
		while(it2.hasNext()) {
			Entry<String, MapRequest> e = it2.next();
			MapRequest mr = e.getValue();
			String filename = e.getKey();
			if(mr.completeParts == mr.totalParts) {
				
				//send message
				byte[] messageStr = ("MAP COMPLETE," + mr.id + "\n").getBytes();
				cServer.sendMessage(cServer.getNumber(mr.requester), messageStr);
				activeMaps.remove(filename);
			}
		}
		
	}
	
	public void run() {
		while(true) {

			try {
				checkResults();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			if(requests.size() > 0) {
				try {
					doRequest();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
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
