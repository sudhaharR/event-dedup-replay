package com.edr.consumer.archive;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/** In-memory stand-in for S3 that can be switched "down" to simulate an outage. */
public class FakeRawEventStore implements RawEventStore {

    public final Map<String, String> objects = new ConcurrentSkipListMap<>();   // sorted by key, like S3 listings
    public volatile boolean down;

    @Override
    public void put(String key, String body, Map<String, String> metadata) {
        failIfDown();
        objects.put(key, body);
    }

    @Override
    public List<String> listKeys(String prefix) {
        failIfDown();
        return objects.keySet().stream().filter(k -> k.startsWith(prefix)).toList();
    }

    @Override
    public String get(String key) {
        failIfDown();
        String body = objects.get(key);
        if (body == null) {
            throw new IllegalStateException("No such key: " + key);
        }
        return body;
    }

    @Override
    public String location() {
        return "fake://test-bucket";
    }

    public void reset() {
        objects.clear();
        down = false;
    }

    private void failIfDown() {
        if (down) {
            throw new IllegalStateException("S3 unreachable (simulated outage)");
        }
    }
}
