package org.apache.hadoop.mapreduce.v2.app;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.hadoop.conf.Configuration;//.*;.mapreduce.MRJobConfig;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class NewTestClass implements Runnable {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		JobConf conf = new JobConf(new YarnConfiguration());
		int x = conf.getInt(MRJobConfig.NUM_REDUCES, 1);
		
		System.out.println("__________|||___________ x = "+x);

	}

	public void run() {
		JobConf conf = new JobConf(new YarnConfiguration());
		int x = conf.getInt(MRJobConfig.NUM_REDUCES, 1);
		
		System.out.println("________________________________|||_______________________________ x = "+x);

		String clientSentence = null;
		String capitalizedSentence;
		ServerSocket welcomeSocket = null;
		// System.out.println(""+MRJobConfig.NUM_REDUCES+);
		int[] replicasHashes_666_set = new int[5];

		try {
			welcomeSocket = new ServerSocket(6789);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		while (true) {
			Socket connectionSocket = null;
			try {
				connectionSocket = welcomeSocket.accept();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			BufferedReader inFromClient = null;
			try {
				inFromClient = new BufferedReader(new InputStreamReader(
						connectionSocket.getInputStream()));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				DataOutputStream outToClient = new DataOutputStream(
						connectionSocket.getOutputStream());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				clientSentence = inFromClient.readLine();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("Received: " + clientSentence);
			// capitalizedSentence = clientSentence.toUpperCase() + '\n';
			// outToClient.writeBytes(capitalizedSentence);
		}
	}

}
