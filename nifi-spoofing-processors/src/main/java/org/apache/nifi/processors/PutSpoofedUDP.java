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

import com.savarese.rocksaw.net.RawSocket;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.savarese.vserv.tcpip.OctetConverter;
import org.savarese.vserv.tcpip.UDPPacket;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <h1>WARNING: this processor ONLY works if nifi is running with ROOT PRIVILEGES</h1>
 * <p>
 * This processor binds to a network interface, defined via configuration, and every
 * time a flowfile is received a UDP packet is fabricated using the source/destination
 * hostname and port, set on its properties, and the flowfile content. Such packet is
 * then send via the bound network interface. After all the processing the received
 * flowfile is re-passed unchanged using the Repass relationship.
 * </p>
 * <p>
 * The following properties are required for this processor to work:
 * <ul>
 *     <li>
 *         "Destination Hostname": hostname/IP address to send the fabricated messages
 *     </li>
 *     <li>
 *         "Destination Port": destination host port
 *     </li>
 *     <li>
 *         "Spoofed Source Hostname": fake hostname/IP address source
 *     </li>
 *     <li>
 *         "Spoofed Source Port": fake source host port
 *     </li>
 *     <li>
 *         "Network interface": interface used to bind and send the packets
 *     </li>
 * </ul>
 *
 */
@Tags({"udp", "relay", "spoofing", "put"})
@CapabilityDescription("Transparent relay for syslog messages received through flowfiles")
public class PutSpoofedUDP extends AbstractProcessor {

    public static final PropertyDescriptor PROPERTY_DEST_HOSTNAME = new PropertyDescriptor
            .Builder().name("Destination Hostname")
            .description("host name of the destination")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor PROPERTY_DEST_PORT = new PropertyDescriptor
            .Builder().name("Destination Port")
            .description("Port number of the destination")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor PROPERTY_SRC_HOSTNAME = new PropertyDescriptor
            .Builder().name("Spoofed Source Hostname")
            .description("Host name used as source in the spoofed packet")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor PROPERTY_SRC_PORT = new PropertyDescriptor
            .Builder().name("Spoofed Source Port")
            .description("Source port number used int the spoofed packet")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();


    public static final PropertyDescriptor PROPERTY_INTERFACE = new PropertyDescriptor
            .Builder().name("Network interface")
            .description("Interface used to send the spoofed messages")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator((subject, input, context) -> {
                try {
                    if (NetworkInterface.getByName(input) == null){
                        return new ValidationResult.Builder()
                                .valid(false)
                                .subject(subject)
                                .explanation("input is not a network interface")
                                .build();
                    }
                } catch (SocketException e) {
                    e.printStackTrace();
                }

                return new ValidationResult.Builder().valid(true).build();
            })
            .build();

    public static final Relationship RELATIONSHIP_REPASS = new Relationship.Builder()
            .name("success")
            .description("Flowfile content successfully spoofed")
            .build();

    /**
     * max size for a ip packet including header
     */
    public static final int MAX_IP_PKT_SIZE = 65535;

    private AtomicInteger ipId = new AtomicInteger(0);

    /**
     * Raw socket to send udp packet with ip headers
     */
    private RawSocket rs;

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(RELATIONSHIP_REPASS);
        return Collections.unmodifiableSet(relationships);
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(PROPERTY_INTERFACE);
        descriptors.add(PROPERTY_SRC_HOSTNAME);
        descriptors.add(PROPERTY_SRC_PORT);
        descriptors.add(PROPERTY_DEST_HOSTNAME);
        descriptors.add(PROPERTY_DEST_PORT);
        return Collections.unmodifiableList(descriptors);
    }

    /**
     * Pre-fill spoofed packet and bind socket to an interface
     *
     * @param context processor's context
     */
    @OnScheduled
    public void onScheduled(final ProcessContext context) throws ProcessException {

        // create and open raw socket
        rs = new RawSocket();
        try {
            rs.open(RawSocket.PF_INET, RawSocket.getProtocolByName("UDP"));
            rs.setIPHeaderInclude(true);
        } catch (IOException e) {
            throw new ProcessException("Failed to create rawsocket", e);
        }

        // bind to interface
        try {
            rs.bindDevice(context.getProperty(PROPERTY_INTERFACE).getValue());
        } catch (IOException e) {
            throw new ProcessException("Failed to bind rawsocket to interface", e);
        }

    }

    @OnUnscheduled
    public void onUnscheduled() {
        try {
            this.rs.close();
        } catch (IOException e) {
            // already closed
        }
    }

    /**
     * Read a flow file, create a spoofed udp packed with its content and send it,
     * based on this processor properties.
     *
     * @param context Processor's context
     * @param session Current session
     * @throws ProcessException related to Super Class method signature
     */
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // get next flow file
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        InetAddress srcAddr;

        // allocate memory for the spoofed udp datagram
        final byte[] buffer = new byte[MAX_IP_PKT_SIZE];
        final UDPPacket udpPacket = new UDPPacket(1); // 1 here is just to create the object, the buffer itself is set below
        udpPacket.setData(buffer);

        // configure ip pkt
        udpPacket.setIPVersion(4);
        udpPacket.setIPHeaderLength(5);
        udpPacket.setTypeOfService(0);
        udpPacket.setIdentification(ipId.get());
        udpPacket.setIPFlags(0);
        udpPacket.setFragmentOffset(0);
        udpPacket.setTTL(255);
        udpPacket.setProtocol(RawSocket.getProtocolByName("UDP"));

        InetAddress destAddr;
        try {
            destAddr = InetAddress.getByName(context.getProperty(PROPERTY_DEST_HOSTNAME).evaluateAttributeExpressions(flowFile).getValue());
            srcAddr = InetAddress.getByName(context.getProperty(PROPERTY_SRC_HOSTNAME).evaluateAttributeExpressions(flowFile).getValue());
        } catch (UnknownHostException e) {
            throw new ProcessException("Could not parse destination hostname");
        }
        udpPacket.setSourceAsWord(convertAddrToWord(srcAddr.getAddress()));
        udpPacket.setDestinationAsWord(convertAddrToWord(destAddr.getAddress()));

        // configure udp pkt
        int srcPort = Integer.parseInt(context.getProperty(PROPERTY_SRC_PORT).evaluateAttributeExpressions(flowFile).getValue());
        int destPort = Integer.parseInt(context.getProperty(PROPERTY_DEST_PORT).evaluateAttributeExpressions(flowFile).getValue());
        udpPacket.setSourcePort(srcPort);
        udpPacket.setDestinationPort(destPort);

        getLogger().debug(srcAddr.getHostAddress() + ":" + srcPort + " -> " + destAddr.getHostAddress() + ":" + destPort);

        // fill packet with message and set message size
        int firstByteOfData = udpPacket.getCombinedHeaderByteLength();
        session.read(flowFile, in -> {
            int qtd = in.read(buffer, firstByteOfData, MAX_IP_PKT_SIZE - firstByteOfData);
            getLogger().debug(qtd + " bytes received");
            udpPacket.setUDPDataByteLength(qtd);
            udpPacket.setUDPPacketLength(UDPPacket.LENGTH_UDP_HEADER + qtd);
        });

        // finishes and send spoofed message
        try {
            // set ip id field
            udpPacket.setIdentification(ipId.getAndAdd(2));

            // generate checksums
            udpPacket.computeUDPChecksum();
            udpPacket.computeIPChecksum();

            // generate and display debug information
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 28; i++){
                sb.append(String.format("%02x ", (int) buffer[i] & 0xff));
                if ((i+1) % 4 == 0){
                    sb.append("| " );
                }
            }
            getLogger().debug("pkt header: " + sb.toString());

            // send packet
            rs.write(destAddr, buffer, 0, udpPacket.getIPPacketLength());
            getLogger().debug("pkt sent");

        } catch (Throwable e) {
            throw new ProcessException(e);
        }

        session.transfer(flowFile, RELATIONSHIP_REPASS);
    }

    /**
     * Convert a IPv4 address in the format a byte array to a work,
     * using network byte ordering.
     *
     * @param addr IPv4 address to be converted
     * @return the address as a word int network byte ordering
     */
    private int convertAddrToWord(byte[] addr){
        /*int word = 0;
        for(int i : addr){
            word = (word << 8) + i;
        }
        return word;*/
        return OctetConverter.octetsToInt(addr);
    }
}

