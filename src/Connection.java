import java.io.DataInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.Socket;
import java.net.UnknownHostException;


public class Connection {
	public DataInputStream networkIn;
	public PrintStream networkOut;
	public Socket sock;
	public String remoteIp;
	public int remotePort;
	public boolean acked;
	
	public Connection(Socket sock) throws IOException {
		networkIn = new DataInputStream(sock.getInputStream());
		networkOut = new PrintStream(sock.getOutputStream());
		remotePort = sock.getPort();
		remoteIp = sock.getInetAddress().getHostAddress();
		acked=false;
	}
	
	public Connection(String ip, int port) throws UnknownHostException, IOException {
		sock = new Socket(ip,port);
		networkIn = new DataInputStream(sock.getInputStream());
		networkOut = new PrintStream(sock.getOutputStream());
		remotePort = sock.getPort();
		remoteIp = sock.getInetAddress().getHostAddress();
		acked=false;
	}
	
	public String getLine() throws IOException {
		return networkIn.readLine();
	}
	
	public String asString() {
		return remoteIp + ":" + remotePort;
	}
	
}
