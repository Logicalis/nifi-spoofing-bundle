/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.*;
import java.util.Arrays;
import java.util.Enumeration;

import static org.junit.Assert.assertEquals;


public class PutSpoofedUDPTest {

    private TestRunner testRunner;
    private static InetAddress src;
    private static InetAddress dst;
    private static int srcPort;
    private static int dstPort;
    private static String niface;
    private static DatagramSocket dstHost;
    private static DatagramPacket pkt;
    private static byte[] buff;
    private static String msg;

    @BeforeClass
    public static void initClass(){
        // message to be send
        msg = "This is SPARTA!!!!";

        // get a source port
        srcPort = 4242;
        dstPort = -1; // temporary value

        // just making java happy here :)
        niface = "";
        src = null;
        dst = null;
        dstHost = null;

        // fill static stuff
        try {
            // get src/dst address and network interface
            src = InetAddress.getLocalHost();
            for(Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();e.hasMoreElements();){
                NetworkInterface ni = e.nextElement();
                if(ni.isUp() && !ni.isLoopback() && ni.getInetAddresses().hasMoreElements()){
                    dst = ni.getInetAddresses().nextElement();
                    niface = ni.getName();
                    break;
                }
            }

            // setup dst host
            dstHost = new DatagramSocket(0,dst);
            dstPort = dstHost.getLocalPort();

            // get a datagram packet
            buff = new byte[64*1024]; // get 64k of memory
            pkt = new DatagramPacket(buff, buff.length);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (SocketException e) {
            e.printStackTrace();
        }
    }


    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(PutSpoofedUDP.class);

        // setup processor
        testRunner.setProperty(PutSpoofedUDP.PROPERTY_SRC_HOSTNAME,src.getHostName());
        testRunner.setProperty(PutSpoofedUDP.PROPERTY_DEST_HOSTNAME,dst.getHostName());
        testRunner.setProperty(PutSpoofedUDP.PROPERTY_SRC_PORT,String.valueOf(srcPort));
        testRunner.setProperty(PutSpoofedUDP.PROPERTY_DEST_PORT,String.valueOf(dstPort));
        testRunner.setProperty(PutSpoofedUDP.PROPERTY_INTERFACE, niface);

        // clear buffer
        Arrays.fill(buff, (byte) 0);

    }

    @Test(timeout = 5000L)
    public void happyFlow() {
        // print debug info
        System.out.println("HAPPY FLOW: using " + niface + " to " + src.getHostName() + ":" + srcPort + " -> " + dst.getHostName() + ":" + dstPort);
        System.out.flush();

        // setup host
        Thread host = new Thread(() -> {
            try {
                System.out.println("HOST: waiting for packets...");
                System.out.flush();
                dstHost.receive(pkt);
                System.out.println("HOST: packet received.");
                System.out.flush();
                assertEquals("packet source address: ", src, pkt.getAddress());
                assertEquals("packet source port: ", srcPort, pkt.getPort());
                assertEquals("packet content: ", msg, new String(pkt.getData(),0,pkt.getLength()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        host.start();

        // add message and run
        testRunner.enqueue(msg.getBytes());
        testRunner.run();

        // wait host receive message
        try {
            host.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void noFlowfile() {
        testRunner.run();
        assertEquals("repassed flowfile:", 0, testRunner.getFlowFilesForRelationship(PutSpoofedUDP.RELATIONSHIP_REPASS).size());
    }

    @Test
    public void invalidNetworkInterface(){
        testRunner.setProperty(PutSpoofedUDP.PROPERTY_INTERFACE, "");
        testRunner.assertNotValid();
        testRunner.setProperty(PutSpoofedUDP.PROPERTY_INTERFACE, "                ");
        testRunner.assertNotValid();
        testRunner.setProperty(PutSpoofedUDP.PROPERTY_INTERFACE, " a645sd0 fd5640 fd4 dfd4");
        testRunner.assertNotValid();
        testRunner.setProperty(PutSpoofedUDP.PROPERTY_INTERFACE, "lols");
        testRunner.assertNotValid();

    }

}
