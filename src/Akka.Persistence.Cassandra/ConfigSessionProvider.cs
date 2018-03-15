using System;
using System.Collections.Generic;
using System.Net;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Cassandra;

namespace Akka.Persistence.Cassandra
{
    /// <summary>
    /// Default implementation of the <see cref="ISessionProvider"/> that is used for creating the
    /// Cassandra Session. This class is building the Cluster from configuration
    /// properties.
    /// 
    /// You may create a subclass of this that performs lookup the contact points
    /// of the Cassandra cluster asynchronously instead of reading them in the
    /// configuration. Such a subclass should override the <see cref="LookupContactPoints"/>
    /// method.
    /// 
    /// The implementation is defined in configuration `session-provider` property.
    /// The config parameter is the config section of the plugin.
    /// </summary>
    public class ConfigSessionProvider : ISessionProvider
    {
        private static int clusterIdentifier = 0;

        private readonly ExtendedActorSystem system;
        private readonly Config config;

        public ConfigSessionProvider(ExtendedActorSystem system, Config config)
        {
            this.system = system;
            this.config = config;

            Port = config.GetInt("port");
            PageSize = config.GetInt("max-result-size");

            switch (config.GetString("protocol-version"))
            {
                case null: case "": ProtocolVersion = null; break;
                case "1": ProtocolVersion = global::Cassandra.ProtocolVersion.V1; break;
                case "2": ProtocolVersion = global::Cassandra.ProtocolVersion.V2; break;
                case "3": ProtocolVersion = global::Cassandra.ProtocolVersion.V3; break;
                case "4": ProtocolVersion = global::Cassandra.ProtocolVersion.V4; break;
                case "5": ProtocolVersion = global::Cassandra.ProtocolVersion.V5; break;
            }

            ConnectionPoolConfig = config.GetConfig("connection-pool");
            PoolingOptions = new PoolingOptions()
                .SetCoreConnectionsPerHost(HostDistance.Local, ConnectionPoolConfig.GetInt("connections-per-host-core-local"))
                .SetCoreConnectionsPerHost(HostDistance.Remote, ConnectionPoolConfig.GetInt("connections-per-host-core-remote"))
                .SetMaxConnectionsPerHost(HostDistance.Local, ConnectionPoolConfig.GetInt("connections-per-host-max-local"))
                .SetMaxConnectionsPerHost(HostDistance.Remote, ConnectionPoolConfig.GetInt("connections-per-host-max-remote"))
                .SetMaxSimultaneousRequestsPerConnectionTreshold(HostDistance.Local, ConnectionPoolConfig.GetInt("max-requests-per-connection-local"))
                .SetMaxSimultaneousRequestsPerConnectionTreshold(HostDistance.Remote, ConnectionPoolConfig.GetInt("max-requests-per-connection-remote"))
                .SetMinSimultaneousRequestsPerConnectionTreshold(HostDistance.Local, ConnectionPoolConfig.GetInt("min-requests-per-connection-local"))
                .SetMinSimultaneousRequestsPerConnectionTreshold(HostDistance.Remote, ConnectionPoolConfig.GetInt("min-requests-per-connection-remote"))
                .SetHeartBeatInterval(ConnectionPoolConfig.GetInt("pool-timeout-millis"));

            ReconnectMaxDelay = config.GetTimeSpan("reconnect-max-delay");

            var maxSpeculativeExecutions = config.GetInt("speculative-execution");
            switch (maxSpeculativeExecutions)
            {
                case 0: SpeculativeExecution = null; break;
                default:
                    var delay = config.GetTimeSpan("speculative-executions-delay");
                    SpeculativeExecution = new ConstantSpeculativeExecutionPolicy((long)delay.TotalMilliseconds, maxSpeculativeExecutions);
                    break;
            }
        }

        public int Port { get; }
        public int PageSize { get; }
        public ProtocolVersion? ProtocolVersion { get; }
        public Config ConnectionPoolConfig { get; }
        public PoolingOptions PoolingOptions { get; }
        public TimeSpan ReconnectMaxDelay { get; }
        public ISpeculativeExecutionPolicy SpeculativeExecution { get; }

        /// <summary>
        /// Subclass may override this method to perform lookup the contact points
        /// of the Cassandra cluster asynchronously instead of reading them from the
        /// configuration.
        /// </summary>
        /// <param name="clusterId">The configured `cluster-id` to lookup</param>
        public async Task<IEnumerable<IPEndPoint>> LookupContactPoints(string clusterId)
        {
            var contactPoints = config.GetStringList("contact-points");
            return BuildContactPoints(contactPoints, Port);
        }

        public async Task<ISession> ConnectAsync()
        {
            var clusterId = config.GetString("cluster-id");
            var builder = await ClusterBuilder(clusterId);
            var cluster = builder.Build();

            return await cluster.ConnectAsync();
        }
        
        protected async Task<Builder> ClusterBuilder(string clusterId)
        {
            var addresses = await LookupContactPoints(clusterId);

            var b = new Builder()
                .AddContactPoints(addresses)
                .WithPoolingOptions(PoolingOptions)
                .WithReconnectionPolicy(
                    new ExponentialReconnectionPolicy(1000, (long) ReconnectMaxDelay.TotalMilliseconds))
                .WithQueryOptions(new QueryOptions().SetPageSize(PageSize))
                .WithPort(Port);

            if (SpeculativeExecution != null)
                b.WithSpeculativeExecutionPolicy(SpeculativeExecution);

            if (ProtocolVersion.HasValue)
                b.WithMaxProtocolVersion(ProtocolVersion.Value);

            var userName = config.GetString("authentication.username");
            if (!string.IsNullOrEmpty(userName))
                b.WithCredentials(userName, config.GetString("authentication.password"));

            var localDatacenter = config.GetString("local-datacenter");
            if (!string.IsNullOrEmpty(localDatacenter))
            {
                var userHostsPerRemoteDc = config.GetInt("used-hosts-per-remote-dc");
                b.WithLoadBalancingPolicy(new TokenAwarePolicy(
                    new DCAwareRoundRobinPolicy(localDatacenter, userHostsPerRemoteDc)));
            }

            var sslConfig = config.GetConfig("ssl");
            if (sslConfig != null)
            {
                var certPath = sslConfig.GetString("certificate-path");
                var certPassword = sslConfig.GetString("certificate-password");
                var sslOptions = new SSLOptions()
                    .SetCertificateCollection(new X509CertificateCollection
                    {
                        new X509Certificate2(certPath, certPassword)
                    });
                b.WithSSL(sslOptions);
            }

            var socketConfig = config.GetConfig("socket");
            var socketOptions = new SocketOptions()
                .SetConnectTimeoutMillis((int) socketConfig.GetTimeSpan("connection-timeout").TotalMilliseconds)
                .SetReadTimeoutMillis((int) socketConfig.GetTimeSpan("read-timeout").TotalMilliseconds);

            var sendBufferSize = socketConfig.GetInt("send-buffer-size");
            var receiveBufferSize = socketConfig.GetInt("receive-buffer-size");

            if (sendBufferSize > 0) socketOptions.SetSendBufferSize(sendBufferSize);
            if (receiveBufferSize > 0) socketOptions.SetReceiveBufferSize(receiveBufferSize);

            b.WithSocketOptions(socketOptions);
            return b;
        }

        /// <summary>
        /// Builds list of IP addresses out of host:port pairs or host entries + given port parameter.
        /// </summary>
        protected IEnumerable<IPEndPoint> BuildContactPoints(IList<string> contactPoints, int port)
        {
            if (contactPoints == null || contactPoints.Count == 0)
                throw new ArgumentException("A contact point list cannot be empty.", nameof(contactPoints));

            foreach (var contactPoint in contactPoints)
            {
                var split = contactPoint.Split(':');
                if (split.Length == 1 && IPAddress.TryParse(split[0], out var ip))
                    yield return new IPEndPoint(ip, port);
                else if (split.Length == 2 && int.TryParse(split[1], out var p) && IPAddress.TryParse(split[0], out ip))
                    yield return new IPEndPoint(ip, p);
                else throw new ArgumentException($"A contact point should have the form [ip:port] or [ip] but was: {contactPoint}.");
            }
        }
        
    }
}