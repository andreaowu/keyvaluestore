/**
 * XML Parsing library for the key-value store
 * 
 * @author Mosharaf Chowdhury (http://www.mosharaf.com)
 * @author Prashanth Mohan (http://www.cs.berkeley.edu/~prmohan)
 * 
 * Copyright (c) 2012, University of California at Berkeley
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *  * Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of University of California, Berkeley nor the
 *    names of its contributors may be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *    
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 *  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 *  DISCLAIMED. IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY
 *  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 *  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 *  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 *  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package edu.berkeley.cs162;

import java.io.BufferedReader;
import java.io.FilterInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.IOException;
import java.net.Socket;


/**
 * This is the object that is used to generate messages the XML based messages 
 * for communication between clients and servers. 
 */
public class KVMessage {
	private String msgType = null;
	private String key = null;
	private String value = null;
	private String status = null; //TODO: what is status?
	private String message = null;
	
	public final String getKey() {
		return key;
	}

	public final void setKey(String key) {
		this.key = key;
	}

	public final String getValue() {
		return value;
	}

	public final void setValue(String value) {
		this.value = value;
	}

	public final String getStatus() {
		return status;
	}

	public final void setStatus(String status) {
		this.status = status;
	}

	public final String getMessage() {
		return message;
	}

	public final void setMessage(String message) {
		this.message = message;
	}

	public String getMsgType() {
		return msgType;
	}

	/* Solution from http://weblogs.java.net/blog/kohsuke/archive/2005/07/socket_xml_pitf.html */
	private class NoCloseInputStream extends FilterInputStream {
	    public NoCloseInputStream(InputStream in) {
	        super(in);
	    }
	    
	    public void close() {} // ignore close
	}
	
	/***
	 * 
	 * @param msgType
	 * @throws KVException of type "resp" with message "Message format incorrect" if msgType is unknown
	 */
	public KVMessage(String msgType) throws KVException {
		System.out.println("sup" + msgType);
		if (msgType.equals("resp") || msgType.equals("delreq") || msgType.equals("putreq") || msgType.equals("getreq")) {
			this.msgType = msgType;
	    } else {
	    	this.msgType = "resp";
	    	setMessage("Message format incorrect");
	    	throw new KVException(this);
	    }
	}
	
	public KVMessage(String msgType, String message) throws KVException { //TODO: can the message be null or 0 length?
		if (msgType.equals("resp") || msgType.equals("delreq") || msgType.equals("putreq") || msgType.equals("getreq")) {
        	this.msgType = msgType;
        	setMessage(message);
        } else {
        	this.msgType = "resp";
        	setMessage("Message format incorrect");
        	throw new KVException(this);
        }
	}
	
	 /***
     * Parse KVMessage from incoming network connection
     * @param sock
     * @throws KVException if there is an error in parsing the message. The exception should be of type "resp" and message should be:
     * a. "XML Error: Received unparseable message" - if the received message is not valid XML.
     * b. "Network Error: Could not receive data" - if there is a network error causing an incomplete parsing of the message.
     * c. "Message format incorrect" - if there message does not conform to the required specifications. Examples include incorrect message type. 
     */
	public KVMessage(InputStream input) throws KVException {
    	//read stream with BufferedReader
    	BufferedReader br = new BufferedReader(new InputStreamReader(input));
    	int counter = -1;
    	try {
	    	String line;
	    	boolean unfinished = true;
	    	int begin;
	    	int end;
	    	while (unfinished) {
	    		line = br.readLine();
	    		counter++;
	    		System.out.println(line);
	    		switch(counter) {
	        	case 0:
	        		if (!line.equals("<?xml version=\"1.0\" encoding=\"UTF-8\"?>")) {
	        			setMessage("XML Error: Received unparseable message");
	        			this.msgType = "resp";
	        			throw new KVException(this);
	        		}
	        		break;
	        	case 1:
	        		begin = line.indexOf("<KVMessage type=");
	        		end = line.indexOf("\">");
	        		if (begin != 0 || end < 0) {
	        			setMessage("Message format incorrect");
	        			this.msgType = "resp";
	        			throw new KVException(this);
	        		}
	        		msgType = line.substring(begin + "<KVMessage type=".length() + 1, end);
	        		if (!msgType.equals("resp") && !msgType.equals("delreq") && !msgType.equals("putreq") && !msgType.equals("getreq")) {
	        	    	this.msgType = "resp";
	        	    	setMessage("Message format incorrect");
	        	    	throw new KVException(this);
	        	    }
	        		break;
	        	case 2: 
	        		if (line.indexOf("<Key>") == 0 && line.contains("</Key>")) {
		        		begin = line.indexOf("<Key>") + "<Key>".length();
		        		end = line.indexOf("</Key>");
		        		key = line.substring(begin, end);
		        	} else if (msgType.equals("resp") && key == null) {
	        			begin = line.indexOf("<Message>");
		        		end = line.indexOf("</Message>");
		        		if (begin < 0 || end < 0) {
		        			setMessage("Message format incorrect");
		        			this.msgType = "resp";
			        		throw new KVException(this);
		        		}
		        		message = line.substring(begin + "<Message>".length(), end);
		        	} else {
		        		setMessage("Message format incorrect");
		        		this.msgType = "resp";
		        		throw new KVException(this);
		        	}
	        		break;
	        	case 3:
	        		if ((msgType.equals("getreq") || msgType.equals("delreq") || (msgType.equals("resp") && key == null && value == null))) { //TODO: line.equals("</KVMessage>                                                                                                                                                     ") bugged
	        			unfinished = false;
	        		} else if (key != null) { 
	        			begin = line.indexOf("<Value>");
	            		end = line.indexOf("</Value>");
	            		if (begin < 0 || end < 0) {
	            			setMessage("Message format incorrect");
	            			this.msgType = "resp";
			        		throw new KVException(this);
	            		}
	            		value = line.substring(begin + "<Value>".length(), end);
	        		}
	        		break;
	        	case 4:
	        		if ((msgType.equals("putreq") || msgType.equals("resp")) && key != null && value != null) { //TODO: line.equals("</KVMessage>                                                                                                                                                     ") bugged
	        			unfinished = false;
	        		} else {
	        			setMessage("Message format incorrect");
	        			this.msgType = "resp";
		        		throw new KVException(this);
	        		}
	        		break;
	        	case 5:
	        		setMessage("Message format incorrect");
	        		this.msgType = "resp";
	        		throw new KVException(this);
	        	}
	    	}
	    	//br.close();
    	} catch (IOException e) {
    		setMessage("Network Error: Could not receive data");
			throw new KVException(this);
    	}
    	
	}
	
	/**
	 * Generate the XML representation for this message.
	 * @return the XML String
	 * @throws KVException if not enough data is available to generate a valid KV XML message
	 * 
	 */
	public String toXML() throws KVException {
		if ( (msgType.equals("putreq") && (key == null || key.length() == 0 || value == null || value.length() == 0))
				|| (msgType.equals("resp") && ((key == null || key.length() == 0 || value == null || value.length() == 0)
						&& (message == null) )))
			throw new KVException(this);
		String answer = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<KVMessage type=\"";
		String keyBegin = "\">\n<Key>";
		String keyClose = "</Key>\n";
		String valueBegin = "<Value>";
		String valueEnd = "</Value>\n";
		String messageBegin = "<Message>";
		String messageEnd = "</Message>";
		String kvMsg = "</KVMessage>";
		if (msgType.equals("getreq") || msgType.equals("delreq")) {
			return answer.concat(msgType + keyBegin + key + keyClose + kvMsg);
		} else if (msgType.equals("putreq") || (msgType.equals("resp") && key != null && value != null)) {
			return answer.concat(msgType + keyBegin + key + keyClose + valueBegin + value + valueEnd + kvMsg);
		} else if (msgType.equals("resp") && message.equals("Success")) {
			return answer.concat(msgType + "\">\n" + messageBegin + "Success" + messageEnd + "\n" + kvMsg);
		} else if (msgType.equals("resp")) {
			return answer.concat(msgType + "\">\n" + messageBegin + "Error Message" + messageEnd + kvMsg);
		} else {
			setMessage("Message format incorrect");
    		this.msgType = "resp";
    		throw new KVException(this);
		}
	}
	
	public void sendMessage(Socket sock) throws KVException {
	    String sendString = toXML();
	    System.out.println("sendMessage: \n" + sendString);
	    byte[] send = new byte[sendString.length()];
	    for (int i = 0; i < sendString.length(); i++)
	    	send[i] = (byte) sendString.charAt(i);
	    try {
	    	OutputStream out = sock.getOutputStream();
	    	out.write(send);
	    	out.flush();
	    	sock.shutdownOutput();
	    } catch (IOException e) {
	    	setMessage("Network Error: Could not send data");
	    	e.printStackTrace();
			throw new KVException(this);
	    }
	}
}
