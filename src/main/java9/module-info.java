module r2dbc.singlestore {
    requires transitive r2dbc.spi;
    requires transitive reactor.core;
    requires transitive io.netty.buffer;
    requires transitive io.netty.handler;
    requires transitive io.netty.transport.unix.common;
    requires transitive io.netty.common;
    requires transitive io.netty.transport;
    requires transitive io.netty.codec;
    requires transitive org.reactivestreams;
    requires transitive reactor.netty.core;
    requires transitive java.naming;

    exports com.singlestore.r2dbc;
    exports com.singlestore.r2dbc.api;
    exports com.singlestore.r2dbc.authentication;
    exports com.singlestore.r2dbc.message;

    uses com.singlestore.r2dbc.authentication.AuthenticationPlugin;
    uses io.r2dbc.spi.ConnectionFactoryProvider;

    provides io.r2dbc.spi.ConnectionFactoryProvider with
            com.singlestore.r2dbc.SingleStoreConnectionFactoryProvider;
    provides com.singlestore.r2dbc.authentication.AuthenticationPlugin with
            com.singlestore.r2dbc.authentication.standard.NativePasswordPluginFlow,
            com.singlestore.r2dbc.authentication.addon.ClearPasswordPluginFlow,
            com.singlestore.r2dbc.authentication.standard.PamPluginFlow;
}
