package com.vinay.spark.streaming.text;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class LocalSocketServer {

	@SuppressWarnings("unused")
	public static void main(String[] args) {
		ServerSocket socketServerObj = null;
		Socket clientSocketObj = null;
		DataInputStream dataInputStreamObj = null;
		PrintStream printStreamObj = null;

		Path path = FileSystems.getDefault().getPath(CommonUtils.DATA_DIR, "StreamingTweets.txt");
		List<String> dataLines = null;
		try {

			// Getting all lines from files for streaming...
			dataLines = Files.readAllLines(path, StandardCharsets.UTF_8);

			// Opening the Server Socket at respective port
			socketServerObj = new ServerSocket(CommonUtils.SERVER_PORT);
			System.out.println("Sever Socket Opened : " + CommonUtils.SERVER_PORT);
			System.out.println("Total Records Read  : " + dataLines.size());

			// Getting the reference for client Socket
			clientSocketObj = socketServerObj.accept();
			System.out.println("Accepted Client request from : " + clientSocketObj.getInetAddress());
			Thread.sleep((long) (Math.random() * 4000));

			// Opening the Input Stream from client Socket
			dataInputStreamObj = new DataInputStream(clientSocketObj.getInputStream());
			// Opening the output Stream from client Socket
			printStreamObj = new PrintStream(clientSocketObj.getOutputStream());

			while (true) {
				// Pick a Random Line to publish...
				int randomDataLine = (int) (Math.random() * dataLines.size());
				System.out.println("Publishing " + (randomDataLine + 1) + " record : " + dataLines.get(randomDataLine));
				printStreamObj.println(dataLines.get(randomDataLine));
				printStreamObj.flush();
				// Randomly sleeping the main thread for 1-3 seconds
				Thread.sleep((long) (Math.random() * 3000));
			}

		} catch (IOException ioException) {
			ioException.printStackTrace();
		} catch (InterruptedException interruptedException) {
			interruptedException.printStackTrace();
		}
	}
}
