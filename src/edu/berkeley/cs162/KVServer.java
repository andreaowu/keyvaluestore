/**
 * Slave Server component of a KeyValue store
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

/**
 * This class defines the slave key value servers. Each individual KVServer
 * would be a fully functioning Key-Value server. For Project 3, you would
 * implement this class. For Project 4, you will have a Master Key-Value server
 * and multiple of these slave Key-Value servers, each of them catering to a
 * different part of the key namespace.
 * 
 */
public class KVServer implements KeyValueInterface {
	private KVStore dataStore = null;
	private KVCache dataCache = null;

	private static final int MAX_KEY_SIZE = 256;
	private static final int MAX_VAL_SIZE = 256 * 1024;

	/**
	 * @param numSets
	 *            number of sets in the data Cache.
	 */
	public KVServer(int numSets, int maxElemsPerSet) {
		dataStore = new KVStore();
		dataCache = new KVCache(numSets, maxElemsPerSet);

		AutoGrader.registerKVServer(dataStore, dataCache);
	}

	public boolean put(String key, String value) throws KVException {
		// Must be called before anything else
		AutoGrader.agKVServerPutStarted(key, value);
		System.out.println("put in KVServer");
		if (key.length() > MAX_KEY_SIZE) {
			KVMessage kmsg = new KVMessage("Oversized key");
			// Must be called before returning
			AutoGrader.agKVServerPutFinished(key, value);
			throw new KVException(kmsg);
		} else if (value.length() > MAX_VAL_SIZE) {
			KVMessage kmsg = new KVMessage("Oversized value");
			// Must be called before returning
			AutoGrader.agKVServerPutFinished(key, value);
			throw new KVException(kmsg);
		}
		dataCache.getWriteLock(key);
		boolean cache = dataCache.put(key, value);

		boolean store;
		try {
			store = dataStore.put(key, value);
		} catch (KVException e) {
			KVMessage kmsg = new KVMessage("I/O Error");
			// Must be called before returning
			AutoGrader.agKVServerGetFinished(key);
			throw new KVException(kmsg);
		}		
		
		if (!cache) {
			// Must be called before returning
			AutoGrader.agKVServerPutFinished(key, value);
			return false;
		}
		// Must be called before returning
		AutoGrader.agKVServerPutFinished(key, value);
		return store;
	}

	public String get(String key) throws KVException {
		// Must be called before anything else
		AutoGrader.agKVServerGetStarted(key);

		if (key.length() > MAX_KEY_SIZE) {
			KVMessage kmsg = new KVMessage("Oversized key");
			// Must be called before returning
			AutoGrader.agKVServerGetFinished(key);
			throw new KVException(kmsg);
		}
		dataCache.getWriteLock(key);
		String store = dataStore.get(key);
		if (store != null) {
			// Must be called before returning
			AutoGrader.agKVServerGetFinished(key);
			return store;
		}
		String cache = dataCache.get(key);
		if (cache == null) {
			KVMessage kmsg = new KVMessage("Does not exist");
			// Must be called before returning
			AutoGrader.agKVServerGetFinished(key);
			throw new KVException(kmsg);
		}

		// Must be called before returning
		AutoGrader.agKVServerGetFinished(key);
		return cache;
	}

	public void del(String key) throws KVException {
		// Must be called before anything else
		AutoGrader.agKVServerDelStarted(key);

		if (key.length() > MAX_KEY_SIZE) {
			KVMessage kmsg = new KVMessage("Oversized key");
			// Must be called before returning
			AutoGrader.agKVServerDelFinished(key);
			throw new KVException(kmsg);
		}

		try {
			dataStore.del(key);
			dataCache.del(key);
		} catch (KVException e) {
			KVMessage kmsg = new KVMessage("Does not exist");
			// Must be called before returning
			AutoGrader.agKVServerDelFinished(key);
			throw new KVException(kmsg);
		}
		// Must be called before returning
		AutoGrader.agKVServerDelFinished(key);
	}
}
