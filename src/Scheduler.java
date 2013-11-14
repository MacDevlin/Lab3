import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class Scheduler {
	
	public FileSystem fSystem;
	public CommunicationServer cServer;
	public Scheduler(FileSystem fSystem, CommunicationServer cServer) {
		this.fSystem = fSystem;
		this.cServer = cServer;
	}
	
	public void schedule(Connection con, String filename, int startRecord, int endRecord, byte[] funcData) throws IOException, InterruptedException {
		
		//for now, just assign work to the nodes with the files needed
		int rep = Integer.parseInt(fSystem.filesystem_config.get("replication_factor"));
		int fileLines = fSystem.getFileLineNum(filename);
		int startPart = startRecord*rep/fileLines;
		int endPart = (int)Math.ceil((float)endRecord*rep/fileLines);
		int partLen = fileLines/rep;
		
		
		//tell each compute node to map
		for(int i=startPart; i<=endPart; i++) {
			int c = fSystem.getFileLocation(filename,i);
			int relStart = Math.max(startRecord - i*partLen, 0);
			int relEnd = Math.min((endRecord - i*partLen),2*partLen);//due to the last part file possibly being larger, 2*partLen covers it. Maps will stop at eof anyway 
			byte[] messageStr = ("MAP ASSIGN," + filename + "_" + i + "," + relStart + "," + relEnd + "," + funcData.length + "\n").getBytes();
			byte[] message = new byte[messageStr.length + funcData.length];
			System.arraycopy(messageStr, 0, message, 0, messageStr.length);
			System.arraycopy(funcData, 0, message, messageStr.length, funcData.length);
			cServer.sendMessage(c, message);
		}
	}
	
}
