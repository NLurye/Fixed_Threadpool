package test;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.function.Consumer;

//ParallelStreamer (Fixed Threadpool) creates n queues with given capacity and a consumer that accepts <E> and open new thread for each queue.

public class ParallelStreamer<E> {
	int num_of_queues, round;
	volatile boolean stop;
	Thread[] threads;
	ArrayList<BlockingQueue<E>> queues;

	public ParallelStreamer(int n, int capacity, Consumer<E> consumer) {
		num_of_queues = n;
		stop = false;
		round = 0;
		queues = new ArrayList<>();
		threads = new Thread[n];
		for (int i = 0; i < n; i++) {
			BlockingQueue<E> q = new ArrayBlockingQueue<>(capacity);
			queues.add(q);
			threads[i] = new Thread(
					()->{
						while (!stop){
							try {
								consumer.accept(q.take());//Run function with next E from queue
							} catch (InterruptedException e) {}
						}
					}
			);
			threads[i].start();
		}

	}
		
	// tasks added evenly in each queue every round
	public void add(E e) throws InterruptedException{
	while (!stop){
			queues.get(round).put(e);
			round=(round+1)%num_of_queues;
		}
	}
	
	public void endOfInput(){
		stop = true;
		for(int i=0;i<threads.length;i++)
			threads[i].interrupt();
	}

}
