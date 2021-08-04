package com.begor.kc.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SessionFactory {

    static CqlSession createSession(CassandraSinkConnectorConfig config) {
        return CqlSession.builder()
                .addContactPoints(getContactPoints(Arrays.asList(config.contactPoints), config.port))
                .withLocalDatacenter(config.localDataCenter)
                .build();
    }

    private static List<InetSocketAddress> getContactPoints(final List<String> contactPoints, final int port) {
        final List<InetSocketAddress> inetSocketAddresses = new ArrayList<InetSocketAddress>();
        for (final String contactPoint : contactPoints) {
            inetSocketAddresses.add(new InetSocketAddress(contactPoint, port));
        }
        return inetSocketAddresses;
    }
}
