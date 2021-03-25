package io.opencmw.client;

import static org.junit.jupiter.api.Assertions.fail;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.awaitility.Awaitility;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import io.opencmw.EventStore;
import io.opencmw.RingBufferEvent;
import io.opencmw.filter.EvtTypeFilter;
import io.opencmw.filter.TimingCtx;
import io.opencmw.serialiser.IoClassSerialiser;
import io.opencmw.serialiser.IoSerialiser;
import io.opencmw.serialiser.spi.BinarySerialiser;
import io.opencmw.serialiser.spi.FastByteBuffer;
import io.opencmw.utils.NoDuplicatesList;

import com.lmax.disruptor.RingBuffer;

@Timeout(30)
class DataSourcePublisherBurstTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataSourcePublisherBurstTest.class);
    private static final List<DnsResolver> RESOLVERS = Collections.synchronizedList(new NoDuplicatesList<>());
    private static class TestDataSource extends DataSource {
        public static AtomicReference<TestDataSource> lastInstance = new AtomicReference<>();
        public static final Factory FACTORY = new Factory() {
            @Override
            public List<String> getApplicableSchemes() {
                return List.of("test");
            }

            @Override
            public Class<? extends IoSerialiser> getMatchingSerialiserType(final @NotNull URI endpoint) {
                return BinarySerialiser.class;
            }

            @Override
            public DataSource newInstance(final ZContext context, final @NotNull URI endpoint, final @NotNull Duration timeout, final @NotNull ExecutorService executorService, final @NotNull String clientId) {
                return new DataSourcePublisherBurstTest.TestDataSource(context, endpoint);
            }

            @Override
            public List<DnsResolver> getRegisteredDnsResolver() {
                return RESOLVERS;
            }
        };
        private final static String INPROC = "inproc://testDataSource";
        private final ZContext context;
        private final ZMQ.Socket socket;
        private ZMQ.Socket internalSocket;
        private final IoClassSerialiser ioClassSerialiser = new IoClassSerialiser(new FastByteBuffer(2000));
        private final Map<String, URI> subscriptions = new HashMap<>();

        public TestDataSource(final ZContext context, final URI endpoint) {
            super(endpoint);
            this.context = context;
            this.socket = context.createSocket(SocketType.DEALER);
            this.socket.bind(INPROC);
            lastInstance.set(this);
        }

        public void notify(final String reqId, final URI endpoint, final TestObject object) {
            if (internalSocket == null) {
                internalSocket = context.createSocket(SocketType.DEALER);
                internalSocket.connect(INPROC);
            }
            final ZMsg msg = new ZMsg();
            msg.add(reqId);
            msg.add(endpoint.toString());
            ioClassSerialiser.getDataBuffer().reset();
            ioClassSerialiser.serialiseObject(object);
            msg.add(Arrays.copyOfRange(ioClassSerialiser.getDataBuffer().elements(), 0, ioClassSerialiser.getDataBuffer().position()));
            msg.add(new byte[0]); // exception
            msg.send(internalSocket);
        }

        @Override
        public long housekeeping() {
            return Long.MAX_VALUE; // no housekeeping needed
        }

        @Override
        public void get(final String requestId, final URI endpoint, final byte[] data, final byte[] rbacToken) {
            throw new UnsupportedOperationException("cannot perform gett");
        }

        @Override
        public void set(final String requestId, final URI endpoint, final byte[] data, final byte[] rbacToken) {
            throw new UnsupportedOperationException("cannot perform set");
        }

        @Override
        public ZMQ.Socket getSocket() {
            return socket;
        }

        @Override
        public void close() {
            socket.close();
            lastInstance.set(null);
        }

        @Override
        protected Factory getFactory() {
            return FACTORY;
        }

        @Override
        public ZMsg getMessage() {
            return ZMsg.recvMsg(socket, ZMQ.DONTWAIT);
        }

        @Override
        public void subscribe(final String reqId, final URI endpoint, final byte[] rbacToken) {
            subscriptions.put(reqId, endpoint);
        }

        @Override
        public void unsubscribe(final String reqId) {
            subscriptions.remove(reqId);
        }
    }

    public static class TestContext {
        public final String ctx;
        public final String filter;

        public TestContext(final String ctx, final String filter) {
            this.ctx = ctx;
            this.filter = filter;
        }

        public TestContext() {
            this.ctx = "";
            this.filter = "";
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            final TestContext that = (TestContext) o;
            return Objects.equals(ctx, that.ctx) && Objects.equals(filter, that.filter);
        }

        @Override
        public int hashCode() {
            return Objects.hash(ctx, filter);
        }
    }

    public static class TestObject {
        private final String foo;
        private final double bar;
        private final long creationTime;

        public TestObject(final String foo, final double bar) {
            this.foo = foo;
            this.bar = bar;
            this.creationTime = System.nanoTime();
        }

        public TestObject() {
            this.foo = "";
            this.bar = Double.NaN;
            this.creationTime = System.nanoTime();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (!(o instanceof TestObject))
                return false;
            final TestObject that = (TestObject) o;
            return bar == that.bar && Objects.equals(foo, that.foo);
        }

        @Override
        public int hashCode() {
            return Objects.hash(foo, bar);
        }

        @Override
        public String
        toString() {
            return "TestObject{foo='" + foo + '\'' + ", bar=" + bar + '}';
        }
    }

    @BeforeAll
    @Timeout(10)
    static void registerDataSource() {
        DataSource.register(TestDataSource.FACTORY);
    }

    /**
     * Simple publisher test with two subscriptions. Notify one endpoint with a constant low event rate and process it
     * with a fast event listener. An additional burst of rapid events consumed by a artificially slowed down consumer
     * simulates a processing heavy property. evaluate processing latency on the ping events and ensure that the
     * processing logic does not block.
     */
    @Test
    void testSubscriptionWithParallelBurst() throws URISyntaxException {
        final AtomicInteger pingReceived = new AtomicInteger(0);
        int pingSent = 0;
        final AtomicInteger burstPacketsReceived = new AtomicInteger(0);
        int burstSent = 0;

        final EventStore eventStore = EventStore.getFactory().setFilterConfig(TimingCtx.class, EvtTypeFilter.class).build();
        eventStore.register((event, sequence, endOfBatch) -> {
            final TestObject payload = event.payload.get(TestObject.class);
            if (payload == null) {
                fail("unexpected event: payload is null or not TestObject");
            }
            if (payload.foo.equals("ping")) {
                pingReceived.incrementAndGet();
                LOGGER.atInfo().addArgument(Duration.ofNanos(System.nanoTime() - payload.creationTime).toNanos() * 1e-6).log("received ping: latency: {}ms");
            } else if (!payload.foo.equals("burst")) {
                fail("unexpected event payload");
            }
        });
        eventStore.register((event, sequence, endOfBatch) -> {
            final TestObject payload = event.payload.get(TestObject.class);
            if (payload == null) {
                fail("unexpected event: payload is null or not TestObject");
            }
            if (payload.foo.equals("burst")) {
                LockSupport.parkNanos(Duration.ofMillis(40).toNanos());
                burstPacketsReceived.incrementAndGet();
            } else if (!payload.foo.equals("ping")) {
                fail("unexpected event payload");
            }
        });

        final DataSourcePublisher dataSourcePublisher = new DataSourcePublisher(null, eventStore, null, null, "testSubPublisher");
        eventStore.start();
        new Thread(dataSourcePublisher).start();

        final URI uriPing = new URI("test://foobar/testDevice/ping?ctx=FAIR.SELECTOR.ALL&filter=foobar");
        final URI uriData = new URI("test://foobar/testDevice/data?ctx=FAIR.SELECTOR.ALL&filter=foobar");
        String reqIdPing;
        String reqIdData;
        try (final DataSourcePublisher.Client client = dataSourcePublisher.getClient()) {
            reqIdPing = client.subscribe(uriPing, TestObject.class);
            reqIdData = client.subscribe(uriData, TestObject.class);
        }
        Awaitility.waitAtMost(Duration.ofSeconds(1)).until(() -> TestDataSource.lastInstance.get() != null);
        final TestDataSource testDataSource = TestDataSource.lastInstance.get();

        // ping every 200ms, burst every 20ms for 200200msg
        long start = System.nanoTime();
        long nextMsg = System.nanoTime();
        for (int i = 0; i < 100; ++i) {
            LockSupport.parkNanos(nextMsg - System.nanoTime());
            nextMsg = System.nanoTime() + Duration.ofMillis(5).toNanos();
            if (i % 10 == 0) {
                testDataSource.notify(reqIdPing, uriPing, new TestObject("ping", i));
                pingSent++;
            }
            if (i > 15 && i < 85) {
                testDataSource.notify(reqIdData, uriData, new TestObject("burst", i));
                burstSent++;
            }
            // assert that pings still arrive on time
            // assertTrue(pingReceived.get() + 1 >= pingSent, "pings sent/received: " + pingSent + '/' + pingReceived.get());
        }
        // wait for the internal raw data event buffer to be empty
        final RingBuffer<RingBufferEvent> rawRingBuffer = dataSourcePublisher.getRawDataEventStore().getRingBuffer();
        Awaitility.await().atMost(Duration.ofSeconds(9)).until(() -> rawRingBuffer.getCursor() == rawRingBuffer.getMinimumGatingSequence() && eventStore.getRingBuffer().getCursor() == eventStore.getRingBuffer().getMinimumGatingSequence());
        LOGGER.atInfo().addArgument(dataSourcePublisher.droppedEvents).log("processed all messages, dropped {} mesages");
        LOGGER.atInfo().addArgument(pingReceived.get()).addArgument(pingSent).addArgument(burstPacketsReceived.get()).addArgument(burstSent).log("pings: {}/{}, bursts: {}/{}");

        eventStore.stop();
        dataSourcePublisher.stop();
    }
}
