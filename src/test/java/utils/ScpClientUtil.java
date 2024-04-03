package utils;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.SCPClient;

import java.io.IOException;

public class ScpClientUtil {
    private String ip;
    private int port;
    private String username;
    private String password;
    
    static private ScpClientUtil instance;
    public ScpClientUtil(String ip, int port, String username, String password) {
        this.ip = ip;
        this.port = port;
        this.username = username;
        this.password = password;
    }
    
    static synchronized public ScpClientUtil getInstance(String ip, int port, String username, String password) {
        if (instance == null) {
            instance = new ScpClientUtil(ip, port, username, password);
        }
        return instance;
    }
    
    public void getFile (String remoteFile, String localTargetDirectory) {
        Connection conn = new Connection(ip, port);
        try {
            conn.connect();
            boolean isAuthenticated = conn.authenticateWithPassword(username, password);
            if (!isAuthenticated) {
                System.err.println("authentication failed");
            }
            SCPClient client = new SCPClient(conn);
            client.get(remoteFile, localTargetDirectory);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            conn.close();
        }
    }
    
    public void putFile(String localFile, String remoteTargetDirectory) {
        putFile(localFile, null, remoteTargetDirectory);
    }
    
    public void putFile(String localFile, String remoteFileName, String remoteTargetDirectory) {
        putFile(localFile, remoteFileName, remoteTargetDirectory, null);
    }
    
    public void putFile(String localFile, String remoteFileName, String remoteTargetDirectory, String mode) {
        Connection conn = new Connection(ip, port);
        try {
            conn.connect();
            boolean isAuthenticated = conn.authenticateWithPassword(username, password);
            if (!isAuthenticated) {
                System.err.println("authentication failed");
            }
            SCPClient client = new SCPClient(conn);
            if ((mode == null) || (mode.length() == 0)) {
                mode = "0600";
            }
            if (remoteFileName == null) {
                client.put(localFile, remoteTargetDirectory);
            } else {
                client.put(localFile, remoteFileName, remoteTargetDirectory, mode);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            conn.close();
        }
    }

//    public static void main(String[] args) {
//        ScpClientUtil scpClient = ScpClientUtil.getInstance("172.20.3.13", 22, "root", "@WSX3edc");
//        scpClient.getFile("/home/v8/hatest/start_one_coordinator.sh", "src/main/resources/");
//        scpClient.putFile("src/main/resources/stop_one_coordinator.sh", "/home/liwt/");
//    }
    
}
