package utils;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class RemoteRunSH {
    public static void execShell (String user, String password, String host, int port, String scriptTargetDirectory, String shellName, String stopNode) {
        String command = "bash " + scriptTargetDirectory + "/" + shellName + " " + stopNode + " " + scriptTargetDirectory;

        try {
            JSch jSch = new JSch();
            Session session = jSch.getSession(user, host, port);
            session.setPassword(password);
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect();
            
            // 打开一个远程shell
            Channel channel = session.openChannel("exec");
            ((ChannelExec) channel).setCommand(command);
            channel.setInputStream(null);
            ((ChannelExec) channel).setErrStream(System.err);

            BufferedReader in = new BufferedReader(new InputStreamReader(channel.getInputStream()));
            channel.connect();

            String line;
            while ((line = in.readLine()) != null) {
                System.out.println(line);
            }

            in.close();
            channel.disconnect();
            session.disconnect();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void cleanShell (String user, String password, String host, int port, String scriptTargetDirectory, String shellName) {
        String command = "rm -f " + scriptTargetDirectory + "/" + shellName;

        try {
            JSch jSch = new JSch();
            Session session = jSch.getSession(user, host, port);
            session.setPassword(password);
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect();

            // 打开一个远程shell
            Channel channel = session.openChannel("exec");
            ((ChannelExec) channel).setCommand(command);
            channel.setInputStream(null);
            ((ChannelExec) channel).setErrStream(System.err);

            BufferedReader in = new BufferedReader(new InputStreamReader(channel.getInputStream()));
            channel.connect();

            String line;
            while ((line = in.readLine()) != null) {
                System.out.println(line);
            }

            in.close();
            channel.disconnect();
            session.disconnect();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
