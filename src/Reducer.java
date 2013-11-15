import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;


public class Reducer implements Runnable {
	
	CommunicationServer cServer;
	String filename;
	int start;
	int recNum;
	Reduce iFun;
	
	public Reducer(CommunicationServer cServer, String f, int s, int r, Reduce i) {
		this.cServer = cServer;
		filename = f;
		start = s;
		recNum = r;
		iFun = i;
	}
	
	public void reduce() throws IOException, InterruptedException {
		File f = new File(filename+"_map");
		File f2 = new File(filename+"_reduce");
		DataInputStream fi = new DataInputStream(new FileInputStream(f));
		PrintStream ps = new PrintStream(f2);
		//skip to the start
		for(int i=0; i<start; i++) {
			fi.readLine();
		}
		String answer = fi.readLine();
		for(int i=0; i<recNum; i++) {
			String line = fi.readLine();
			answer = iFun.execute(answer, line);
		}
		ps.println(answer);
		fi.close();
		ps.close();
		byte[] messageStr = ("REDUCE RESULT," + filename + "," + "\n"+answer+"\n").getBytes();
		//byte[] message = new byte[messageStr.length + answer.getBytes().length];
		//System.arraycopy(messageStr, 0, message, 0, messageStr.length);
		//System.arraycopy(answer.getBytes(), 0, message, messageStr.length, answer.getBytes().length);
		cServer.sendMessage(0, messageStr);
		
		System.out.println("Reduce Complete");
		
	}
	
	public void run() {
		try {
			reduce();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
