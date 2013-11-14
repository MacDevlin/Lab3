import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;


public class CommunicationServer implements Runnable {

	public Integer port;
	public InetAddress host;
	public ServerSocket serverSocket;
	List<Connection> connections;
	CommunicationHandler cHandler;
	public CommunicationServer(int port) throws IOException {
		connections = new LinkedList<Connection>();

		host = InetAddress.getLocalHost();
		serverSocket = new ServerSocket(port);
		this.port = serverSocket.getLocalPort();
		cHandler = new CommunicationHandler(this);
		Thread cHandlerThread = new Thread(cHandler);
		cHandlerThread.start();
	}
	
	public int participants() {
		return connections.size();
	}
	
	//message assumed to end in newline
	public void sendMessage(int c, byte[] message) throws IOException, InterruptedException {
		Connection con = connections.get(c);
		con.networkOut.write(message);
		
		//wait for ACK
		while(true) {
			if(con.acked == true) {
				con.acked=false;
				break;
			}
			Thread.sleep(25);
		}
	}
	
	public int hasData() {
		for(int i=0; i<connections.size(); i++) {
			try {
				if(connections.get(i).networkIn.available()>0) {
					return i;
				}
			} catch (IOException e) {
				//connection is invalid
			}
		}
		return -1;
	}
	
	public void addConnection(Connection c) {
		connections.add(c);
	}
	
	public void run() {
		System.out.println(String.format("Communication Server starting on %s:%d", host.getHostAddress(), port));
		while(true) {
			Socket clientSock;
			try {
				clientSock = serverSocket.accept();
				clientSock.setSoTimeout(10000);//10 second timeout
				Connection c = new Connection(clientSock);
				connections.add(c);
				System.out.println("New Connection Accepted");
			} catch (IOException e) {
			}
		
		}
	}
}
