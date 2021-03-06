package io.druid.embedded.app;

import io.druid.embedded.resource.DruidResource;
import io.druid.segment.QueryableIndex;
import io.swagger.jaxrs.config.BeanConfig;
import io.swagger.jaxrs.listing.ApiListingResource;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import javax.servlet.DispatcherType;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Starts a standalone instance of Druid
 */
public class DruidRunner {
    public static final Map<Integer, QueryableIndex> INDEX_MAP = new HashMap<>();
    private Server server;
    private QueryableIndex index;
    private int port;

    /**
     * Create a new DruidRunner to start and stop an embedded Druid instance
     *
     * @param port  Port to listen on
     * @param index Index to make available
     */
    public DruidRunner(int port, QueryableIndex index) {
        this.port = port;
        this.index = index;
    }

    /**
     * Starts the Druid instance and returns the server it is running on
     *
     * @return server hosting Druid
     * @throws Exception
     */
    public Server run() throws Exception {
        if (server == null) {
            server = new Server(port);
            String basePath = "/druid";
            buildSwagger(basePath);
            HandlerList handlers = new HandlerList();
            handlers.addHandler(buildContext(basePath));
            server.setHandler(handlers);
            server.start();
            INDEX_MAP.put(port, index);
        } else {
            throw new IllegalStateException("Server already running");
        }
        return server;
    }

    /**
     * Builds the Swagger documentation
     *
     * @param basePath Base path where the resources reside
     */
    private static void buildSwagger(String basePath) {
        // This configures Swagger
        BeanConfig beanConfig = new BeanConfig();
        beanConfig.setVersion("1.0.0");
        beanConfig.setResourcePackage(DruidResource.class.getPackage().getName());
        beanConfig.setBasePath(basePath);
        beanConfig.setDescription("Embedded Druid");
        beanConfig.setTitle("Embedded Druid");
        beanConfig.setScan(true);
    }

    /**
     * Adds resources to the context to be run on the server
     *
     * @param basePath Path to add the resources on
     * @return ContextHandler to add on the server
     */
    private static ContextHandler buildContext(String basePath) {
        ResourceConfig resourceConfig = new ResourceConfig();
        resourceConfig.packages(DruidResource.class.getPackage().getName(), ApiListingResource.class.getPackage().getName());
        ServletContainer servletContainer = new ServletContainer(resourceConfig);
        ServletHolder entityBrowser = new ServletHolder(servletContainer);
        ServletContextHandler entityBrowserContext = new ServletContextHandler(ServletContextHandler.SESSIONS);
        entityBrowserContext.setContextPath(basePath);
        entityBrowserContext.addServlet(entityBrowser, "/*");

        FilterHolder corsFilter = new FilterHolder(CrossOriginFilter.class);
        corsFilter.setAsyncSupported(true);
        corsFilter.setInitParameter(CrossOriginFilter.ALLOWED_ORIGINS_PARAM, "*");
        corsFilter.setInitParameter(CrossOriginFilter.ALLOWED_HEADERS_PARAM, "*");
        corsFilter.setInitParameter(CrossOriginFilter.ACCESS_CONTROL_ALLOW_ORIGIN_HEADER, "*");
        corsFilter.setInitParameter(CrossOriginFilter.ALLOWED_METHODS_PARAM, "GET,POST,HEAD,DELETE,PUT,OPTIONS");

        entityBrowserContext.addFilter(corsFilter, "/*", EnumSet.allOf(DispatcherType.class));
        return entityBrowserContext;
    }

    /**
     * Runs Druid and blocks until the server is stopped
     *
     * @throws Exception
     */
    public void runBlocking() throws Exception {
        run().join();
    }

    /**
     * Stops the server and removes the index
     *
     * @throws Exception
     */
    public void stop() throws Exception {
        INDEX_MAP.remove(port);
        server.stop();
    }

    /**
     * Starts the server from the command line
     * Currently indexes nothing, must be modified to index from custom locations
     *
     * @param args Command line arguments
     * @throws Exception Thrown by the server on failure
     */
    public static void main(String[] args) throws Exception {
        // TODO Accept command line parameters to start up Druid
        new DruidRunner(37843, null).run();
    }
}
