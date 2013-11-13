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
	
	public Connection(Socket sock) throws IOException {
		networkIn = new DataInputStream(sock.getInputStream());
		networkOut = new PrintStream(sock.getOutputStream());
		remotePort = sock.getPort();
		remoteIp = sock.getInetAddress().getHostAddress();
	}
	
	public Connection(String ip, int port) throws UnknownHostException, IOException {
		sock = new Socket(ip,port);
		networkIn = new DataInputStream(sock.getInputStream());
		networkOut = new PrintStream(sock.getOutputStream());
		remotePort = sock.getPort();
		remoteIp = sock.getInetAddress().getHostAddress();
	}
	
	public String getLine() throws IOException {
		return networkIn.readLine();
	}
}
