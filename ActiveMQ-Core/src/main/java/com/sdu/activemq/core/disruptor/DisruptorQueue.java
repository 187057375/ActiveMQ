package com.sdu.activemq.core.disruptor;

import com.lmax.disruptor.*;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.ProducerType;
import com.sdu.activemq.core.MQConfig;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Disruptor队列[消息队列]
 *
 * @apiNote :
 *
 *  消息投递两种方式:
 *
 *     1: 消息投递直接投递到RingBuffer中
 *
 *     2: 消息投递到每个线程的缓冲中, 定时将消息投递到RingBuffer中
 *
 * @author hanhan.zhang
 * */
public class DisruptorQueue {

    private static final Logger LOGGER = LoggerFactory.getLogger(DisruptorQueue.class);

    private static final String PREFIX = "disruptor-";

    // 队列名称
    private static final String MQ_DISRUPTOR_QUEUE_NAME = "mq.disruptor.queue.name";

    // 环形Ring Buffer容量
    private static final String MQ_DISRUPTOR_RING_BUFFER_SIZE = "mq.disruptor.ring.buffer.size";

    // Ring Buffer读超时
    private static final String MQ_DISRUPTOR_RING_READ_TIMEOUT = "mq.disruptor.ring.buffer.read.timeout";

    // 消息缓冲容量
    private static final String MQ_DISRUPTOR_CACHE_SIZE = "mq.disruptor.cache.size";

    // 消息缓冲区Flush间隔
    private static final String MQ_DISRUPTOR_CACHE_FLUSH_INTERVAL = "mq.disruptor.cache.flush.interval";

    private static final String MQ_DISRUPTOR_CACHE_FLUSH_THREAD = "mq.disruptor.cache.flush.thread";

    @Getter
    private String queueName;

    // Disruptor队列
    private RingBuffer<AtomicReference<Object>> buffer;

    // consume
    private Sequence consume;
    private SequenceBarrier barrier;


    // batch size
    private int inputBatchSize;
    // key = threadId, _value = thread event batch
    private final ConcurrentHashMap<Long, ThreadLocalInsert> threadBatch = new ConcurrentHashMap<Long, ThreadLocalInsert>();

    // flush thread pool executor
    private ScheduledExecutorService flushExecutor;

    private EventHandlerImpl eventHandler;

    public DisruptorQueue(ProducerType type, MessageHandler messageHandler, MQConfig mqConfig) {
        this.queueName = PREFIX + mqConfig.getString(MQ_DISRUPTOR_QUEUE_NAME, "msg-queue");
        this.eventHandler = new EventHandlerImpl(messageHandler);
        this.inputBatchSize = mqConfig.getInt(MQ_DISRUPTOR_CACHE_SIZE, 128);

        WaitStrategy waitStrategy;
        int readTimeout = mqConfig.getInt(MQ_DISRUPTOR_RING_READ_TIMEOUT, 0);
        if (readTimeout < 0) {
            waitStrategy = new LiteBlockingWaitStrategy();
        } else {
            waitStrategy = new TimeoutBlockingWaitStrategy(readTimeout, TimeUnit.MICROSECONDS);
        }

        // 创建环形Ring Buffer[队列]
        EventFactory<AtomicReference<Object>> eventFactory = new EventFactoryImpl();
        this.buffer = RingBuffer.create(type, eventFactory, mqConfig.getInt(MQ_DISRUPTOR_RING_BUFFER_SIZE, 1024), waitStrategy);

        // 消费者控制
        this.barrier = this.buffer.newBarrier();
        this.consume = new Sequence();
        this.buffer.addGatingSequences(this.consume);


        // 定时剔除投递到RingBuffer中的事件
        int flushInterval = mqConfig.getInt(MQ_DISRUPTOR_CACHE_FLUSH_INTERVAL, 1);
        this.flushExecutor = Executors.newScheduledThreadPool(mqConfig.getInt(MQ_DISRUPTOR_CACHE_FLUSH_THREAD, 1));
        this.flushExecutor.scheduleWithFixedDelay(() ->
            threadBatch.forEach((threadId, threadLocalInsert) -> {
                threadLocalInsert.forceBatch();
                threadLocalInsert.flush(true);
            }), flushInterval, flushInterval, TimeUnit.SECONDS);
    }

    // consume
    private void consumeBatch(EventHandler<Object> eventHandler) {
        try {
            final long nextSequence = this.consume.get() + 1;
            final long availableSequence = this.barrier.waitFor(nextSequence);

            if (availableSequence > nextSequence) {
                this.consumeBatchToCursor(availableSequence, eventHandler);
            }
        } catch (AlertException e) {
            LOGGER.error("disruptor queue occur alter exception", e);
        } catch (InterruptedException e) {
            LOGGER.error("disruptor queue occur alter interrupt exception", e);
        } catch (TimeoutException e) {
            LOGGER.error("disruptor queue occur alter timeout exception", e);
        }
    }

    private void consumeBatchToCursor(long cursor, EventHandler<Object> eventHandler) {
        for (long cur = this.consume.get() + 1; cur <= cursor ; ++cur) {
            AtomicReference<Object> atomicReference = this.buffer.get(cur);
            try {
                eventHandler.onEvent(atomicReference.get(), cur, cur == cursor);
            } catch (Exception e) {
                LOGGER.error("disruptor queue consumer event occur exception !", e);
            } finally {
                this.consume.set(cur);
            }
        }
    }

    private void publishDirect(ArrayList<Object> objects, boolean lock) throws InsufficientCapacityException {
        int size = objects.size();
        long endSequence;
        if (lock) {
            endSequence = this.buffer.next(size);
        } else {
            endSequence = this.buffer.tryNext(size);
        }

        long beginSequence = endSequence - (size -1);
        long index = beginSequence;
        for (Object obj : objects) {
            AtomicReference<Object> atomicReference = buffer.get(index++);
            atomicReference.set(obj);
        }
        this.buffer.publish(beginSequence, endSequence);

        // 消费投递的事件
        this.consumeBatch(eventHandler);
    }

    // producer
    public void publish(Object object) {
        long threadId = Thread.currentThread().getId();
        ThreadLocalInsert threadLocalInsert = this.threadBatch.get(threadId);
        if (threadLocalInsert == null) {
            threadLocalInsert = new ThreadLocalBatch();
            this.threadBatch.put(threadId, threadLocalInsert);
        }
        threadLocalInsert.add(object);
    }

    private class EventFactoryImpl implements EventFactory<AtomicReference<Object>> {
        @Override
        public AtomicReference<Object> newInstance() {
            return new AtomicReference<>();
        }
    }

    private class EventHandlerImpl implements EventHandler {

        private MessageHandler messageHandler;

        EventHandlerImpl(MessageHandler messageHandler) {
            this.messageHandler = messageHandler;
        }

        @Override
        public void onEvent(Object object, long l, boolean b) throws Exception {
            messageHandler.handle(object);
        }
    }

    private interface ThreadLocalInsert {
        public void add(Object obj);
        public void forceBatch();
        public void flush(boolean block);
    }

    // Batch for every thread
    private class ThreadLocalBatch implements ThreadLocalInsert {

        private ReentrantLock _flushLock;

        private ArrayList<Object> _objectBatch;

        private ConcurrentLinkedQueue<ArrayList<Object>> _overflowBatch;

        ThreadLocalBatch() {
            this._flushLock = new ReentrantLock();
            this._overflowBatch = new ConcurrentLinkedQueue<>();
            this._objectBatch = new ArrayList<>(inputBatchSize);
        }

        @Override
        public void add(Object obj) {
            this._objectBatch.add(obj);
            // overflow
            if (this._objectBatch.size() >= inputBatchSize) {
                boolean flush = false;
                if (this._overflowBatch.isEmpty()) {
                    try {
                        publishDirect(this._objectBatch, false);
                        this._objectBatch.clear();
                        flush = true;
                    } catch (InsufficientCapacityException e) {

                    }
                }

                if (!flush) {
                    this._overflowBatch.add(this._objectBatch);
                    this._objectBatch = new ArrayList<>(inputBatchSize);
                }
                this._overflowBatch.add(this._objectBatch);
                this._objectBatch = new ArrayList<>(inputBatchSize);

            }
        }

        @Override
        public void forceBatch() {
            if (this._objectBatch.isEmpty()) {
                return;
            }
            this._overflowBatch.add(this._objectBatch);
            this._objectBatch = new ArrayList<>(inputBatchSize);
        }

        @Override
        public void flush(boolean block) {
            if (block) {
                this._flushLock.lock();
            } else if (this._flushLock.tryLock()) {
                return;
            }

            LOGGER.info("flush thread {} event batch !", Thread.currentThread().getId());
            try {
                while (!this._overflowBatch.isEmpty()) {
                    publishDirect(this._overflowBatch.peek(), block);
                }
            } catch (InsufficientCapacityException e) {

            } finally {
                _flushLock.unlock();
            }
        }
    }
}
