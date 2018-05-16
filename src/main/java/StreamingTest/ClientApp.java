package StreamingTest;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class ClientApp {
    public static void main(String[] args) {
        // main方法
        try {
            System.out.println("Defining new socket");
            ServerSocket soc = new ServerSocket(9999);
            System.out.println("Waiting for Incoming Connection");
            Socket clientSocket = soc.accept();

            System.out.println("Connect received");
            OutputStream outputStream = clientSocket.getOutputStream();

            // 循环获取数据, 并发送到套接字上
            Boolean flag = true;
            while (flag) {
                PrintWriter out = new PrintWriter(outputStream, true);
                BufferedReader read = new BufferedReader(new InputStreamReader(System.in));
                System.out.println("Wait for user to input some data");
                String data = read.readLine();
                System.out.println("Data received and now writing it to socket");
                out.println(data);
                if (data.equals("exit")) flag = false;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
