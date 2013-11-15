import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;


public class Mapper implements Runnable {

	public CommunicationServer cServer;
	public String filename;
	public int start;
	public int recNum;
	public Map iFun;
	
	
	public Mapper(CommunicationServer cServer, String filename, int start, int recNum, Map iFun) {
		this.cServer = cServer;
		this.filename = filename;
		this.start = start;
		this.recNum = recNum;
		this.iFun = iFun;
	}
	
	public void map() throws IOException, InterruptedException {
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
	
	public void run() {
		try {
			map();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
