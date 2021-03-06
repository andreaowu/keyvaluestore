/**
 * A simple thread pool implementation
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

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ThreadPool {
	/**
	 * Set of threads in the threadpool
	 */
	protected Thread threads[] = null;
	protected Queue<Runnable> tasks = null;
	protected static Lock taskLock;
	protected static Condition hasTask;

	/**
	 * Initialize the number of threads required in the threadpool.
	 * 
	 * @param size How many threads in the thread pool.
	 */
	public ThreadPool(int size) {
		this.threads = new Thread[size];
		this.tasks = new LinkedList<Runnable>();
		taskLock = new ReentrantLock();
		hasTask = taskLock.newCondition();
		for (int i = 0; i < size; i++) {
			Thread nThread = new WorkerThread(this);
			threads[i] = nThread;
			threads[i].start();
		}

	}

	/*
	 * public Thread[] getThreadGroup(){ return threads; }
	 */

	/**
	 * Add a job to the queue of tasks that has to be executed. As soon as a
	 * thread is available, it will retrieve tasks from this queue and start
	 * processing.
	 * 
	 * @param r job that has to be executed asynchronously
	 * @throws InterruptedException
	 */
	public void addToQueue(Runnable r) throws InterruptedException {
		taskLock.lock();
		synchronized (this.tasks) {
			this.tasks.add(r);
		}
		hasTask.signal();
		taskLock.unlock();
		// System.out.println("added");
	}

	/**
	 * Block until a job is available in the queue and retrieve the job
	 * 
	 * @return A runnable task that has to be executed
	 * @throws InterruptedException
	 */
	public synchronized Runnable getJob() throws InterruptedException {

		// System.out.println("getting");
		taskLock.lock();
		if (this.tasks.peek() == null)
			hasTask.await();
		synchronized (this.tasks) {
			try {
				taskLock.unlock();
			} catch (IllegalMonitorStateException e) {
				// do nothing??
			}
			// System.out.println("gotten");
			return this.tasks.poll();
		}
	}
}

/**
 * The worker threads that make up the thread pool.
 */
class WorkerThread extends Thread {
	/**
	 * The constructor.
	 * 
	 * @param o
	 *            the thread pool
	 */
	protected ThreadPool threadPool;

	WorkerThread(ThreadPool o) {
		super();
		this.threadPool = o;
	}

	/**
	 * Scan for and execute tasks.
	 */
	public void run() {
		Runnable task = null;
		while (true) {
			try {
				task = threadPool.getJob();
				task.run();
			} catch (InterruptedException e) {
			}
		}
	}
}