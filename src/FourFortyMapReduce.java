import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;


public class FourFortyMapReduce {

	CommunicationServer cServer;
	public int id;
	public boolean reduceComplete = false;
	public String reduceResult = "";
	public boolean mapComplete = false;
	public FourFortyMapReduce(int id, CommunicationServer cServer) {
		this.cServer = cServer;
		this.id = id;
	}
	
	public void map(String filename, int recordStart, int recordNum, Map function) throws IOException, InterruptedException {
		//talk to master
		byte[] fData = serialize(function);
		byte[] messageStr = ("MAP REQUEST" + "," + id + "," + filename + "," + recordStart + "," + recordNum + "," + fData.length + "\n").getBytes();
		byte[] message = new byte[messageStr.length + fData.length];
		System.arraycopy(messageStr, 0, message, 0, messageStr.length);
		System.arraycopy(fData, 0, message, messageStr.length, fData.length);
		
		cServer.sendMessage(0, message);//0 is always master on a slave
		while(mapComplete==false) {
			Thread.currentThread().sleep(25);
		}
		mapComplete=false;
		return;
	}
	
	
	public String reduce(String filename, int recordStart, int recordNum, Reduce function) throws IOException, InterruptedException {
		byte[] fData = serialize(function);
		byte[] messageStr = ("REDUCE REQUEST"+ "," + id + "," + filename + "," + recordStart + "," + recordNum + "," + fData.length + "\n").getBytes();
		byte[] message = new byte[messageStr.length + fData.length];
		System.arraycopy(messageStr, 0, message, 0, messageStr.length);
		System.arraycopy(fData, 0, message, messageStr.length, fData.length);
		
		cServer.sendMessage(0, message);//0 is always master on a slave
		
		while(reduceComplete==false) {
			Thread.currentThread().sleep(25);
		}
		reduceComplete=false;
		return reduceResult;
		
	}
	
	public byte[] serialize(Object ifun) throws IOException {
        ObjectOutputStream out;
        File f = new File("serializing.ser");
        if(f.exists()) {
                f.delete();
        }
        //serialize it
        //System.out.println("Serializing function");
        FileOutputStream fout = new FileOutputStream("serializing.ser");
        out = new ObjectOutputStream(fout);
        //System.out.println(String.format("writing %s", ifun.toString()));
        out.writeObject(ifun);
        //System.out.println("Reading Object file into byte array");
        FileInputStream fin = new FileInputStream("serializing.ser");
        byte[] data = new byte[fin.available()];
        fin.read(data);
        fin.close();
        return data;
}
	
}
