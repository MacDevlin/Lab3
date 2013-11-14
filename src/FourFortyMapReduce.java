import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;


public class FourFortyMapReduce {

	CommunicationServer cServer;
	public FourFortyMapReduce(CommunicationServer cServer) {
		this.cServer = cServer;
	}
	
	public void map(String filename, int recordStart, int recordEnd, IFunction function) throws IOException, InterruptedException {
		//talk to master
		byte[] fData = serialize(function);
		byte[] messageStr = ("MAP REQUEST," + filename + "," + recordStart + "," + recordEnd + "," + fData.length + "\n").getBytes();
		byte[] message = new byte[messageStr.length + fData.length];
		System.arraycopy(messageStr, 0, message, 0, messageStr.length);
		System.arraycopy(fData, 0, message, messageStr.length, fData.length);
		
		cServer.sendMessage(0, message);//0 is always master on a slave
		
	}
	
	
	public byte[] serialize(IFunction ifun) throws IOException {
        ObjectOutputStream out;
        File f = new File("serializing.ser");
        if(f.exists()) {
                f.delete();
        }
        //serialize it
        System.out.println("Serializing function");
        FileOutputStream fout = new FileOutputStream("serializing.ser");
        out = new ObjectOutputStream(fout);
        System.out.println(String.format("writing %s", ifun.toString()));
        out.writeObject(ifun);
        System.out.println("Reading Object file into byte array");
        FileInputStream fin = new FileInputStream("serializing.ser");
        byte[] data = new byte[fin.available()];
        fin.read(data);
        fin.close();
        return data;
}
	
}
