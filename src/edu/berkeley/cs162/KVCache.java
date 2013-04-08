/**
 * Implementation of a set-associative cache.
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

import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.HashMap;
import java.lang.String;
import java.util.ArrayList;

/**
 * A set-associate cache which has a fixed maximum number of sets (numSets).
 * Each set has a maximum number of elements (MAX_ELEMS_PER_SET). If a set is
 * full and another entry is added, an entry is dropped based on the eviction
 * policy.
 */
public class KVCache implements KeyValueInterface {
	private int numSets = 100;
	private int maxElemsPerSet = 10;
	private HashMap<Integer, ArrayList<String[]>> setToElem;
	private HashMap<Integer, WriteLock> locks; // true for lock available

	/**
	 * Creates a new LRU cache.
	 * 
	 * @param cacheSize
	 *            the maximum number of entries that will be kept in this cache.
	 */
	public KVCache(int numSets, int maxElemsPerSet) {
		this.numSets = numSets;
		this.maxElemsPerSet = maxElemsPerSet;
		setToElem = new HashMap<Integer, ArrayList<String[]>>();
		locks = new HashMap<Integer, WriteLock>();
		for (int i = 0; i < numSets; i++) {
			setToElem.put(i, new ArrayList<String[]>()); // String[0]: key 	String[1]: value 	String[2]: use bit
			locks.put(i, new ReentrantReadWriteLock().writeLock());
		}
	}

	/**
	 * Retrieves an entry from the cache. Assumes the corresponding set has
	 * already been locked for writing.
	 * 
	 * @param key
	 *            the key whose associated value is to be returned.
	 * @return the value associated to this key, or null if no value with this
	 *         key exists in the cache.
	 */
	public String get(String key) {
		// Must be called before anything else
		AutoGrader.agCacheGetStarted(key);
		AutoGrader.agCacheGetDelay();

		ArrayList<String[]> keyVal = setToElem.get(getSetId(key));
		for (int i = 0; i < keyVal.size(); i++) {
			if (keyVal.get(i)[0].equals(key)) {
				keyVal.get(i)[2] = "1";

				// Must be called before returning
				AutoGrader.agCacheGetFinished(key);
				return keyVal.get(i)[1];
			}
		}

		// Must be called before returning
		AutoGrader.agCacheGetFinished(key);
		return null;
	}

	/**
	 * Adds an entry to this cache. If an entry with the specified key already
	 * exists in the cache, it is replaced by the new entry. If the cache is
	 * full, an entry is removed from the cache based on the eviction policy
	 * Assumes the corresponding set has already been locked for writing.
	 * 
	 * @param key
	 *            the key with which the specified value is to be associated.
	 * @param value
	 *            a value to be associated with the specified key.
	 * @return true is something has been overwritten
	 */
	public boolean put(String key, String value) {
		// Must be called before anything else
		AutoGrader.agCachePutStarted(key, value);
		AutoGrader.agCachePutDelay();

		ArrayList<String[]> keyVal = setToElem.get(getSetId(key));
		// check whether element is already in set
		for (int i = 0; i < keyVal.size(); i++) {
			if (keyVal.get(i)[0].equals(key)) {
				String[] changeThis = keyVal.get(i);
				changeThis[1] = value;
				changeThis[2] = "0"; // TODO: if put in a new one over an old
										// one, reference bit is 0?
				keyVal.remove(i);
				keyVal.add(i, changeThis);
				setToElem.put(getSetId(key), keyVal);
				// Must be called before returning
				AutoGrader.agCacheGetFinished(key);
				return true;
			}
		}

		String[] put = new String[3];
		put[0] = key;
		put[1] = value;
		put[2] = "0";
		keyVal.add(put);

		if (keyVal.size() < maxElemsPerSet) {
			// insert new key value pair
			setToElem.put(getSetId(key), keyVal);
			// Must be called before returning
			AutoGrader.agCacheGetFinished(key);
			return false;
		}

		String[] check = keyVal.get(0);
		// overwrite value
		while (check[3] != "0") {
			check[3] = "0";
			keyVal.remove(0);
			keyVal.add(check);
			check = keyVal.get(0);
		}
		keyVal.remove(0);
		keyVal.add(put);
		setToElem.put(getSetId(key), keyVal);
		// Must be called before returning
		AutoGrader.agCachePutFinished(key, value);
		return true;
	}

	/**
	 * Removes an entry from this cache. Assumes the corresponding set has
	 * already been locked for writing.
	 * 
	 * @param key
	 *            the key with which the specified value is to be associated.
	 */
	public void del(String key) {
		// Must be called before anything else
		AutoGrader.agCacheGetStarted(key);
		AutoGrader.agCacheDelDelay();

		ArrayList<String[]> keyVal = setToElem.get(getSetId(key));
		for (int i = 0; i < keyVal.size(); i++) {
			if (keyVal.get(i)[0].equals(key)) {
				keyVal.remove(i);
				break;
			}
		}

		// Must be called before returning
		AutoGrader.agCacheDelFinished(key);
	}

	/**
	 * @param key
	 * @return the write lock of the set that contains key.
	 */
	public WriteLock getWriteLock(String key) {
		return locks.get(getSetId(key));
	}

	/**
	 * 
	 * @param key
	 * @return set of the key
	 */
	private int getSetId(String key) {
		return Math.abs(key.hashCode()) % numSets;
	}

	public String toXML() {
		String finalString = "";
		String header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>/n<KVCache>/n<Set ID=\"";
		String setIDend = "\">/n";
		String ref = "<CacheEntry isReferenced=\"";
		String valid = " isValid=\"";
		String validEnd = "\">/n<Key>";
		String keyEnd = "</Key>/n<Value>";
		String closing = "</Value>/n</CacheEntry>/n</Set>/n</KVCache>/n";
		for (int i = 0; i < numSets; i++) {
			ArrayList<String[]> keyVal = setToElem.get(i);
			int j = 0;
			for (; j < keyVal.size(); j++) {
				finalString.concat(header + i + setIDend + ref
						+ keyVal.get(j)[2] + valid + "true" + validEnd
						+ keyVal.get(j)[0] + keyEnd + keyVal.get(j)[1]
						+ closing);
			}
			while (j != maxElemsPerSet) {
				finalString.concat(header + i + setIDend + ref
						+ keyVal.get(j)[2] + valid + "false" + validEnd + 0
						+ keyEnd + 0 + closing); // TODO: if not valid, what's the key/value?
				j++;
			}

		}
		return finalString;
	}
}
