using System;
using Akka.Configuration;
using Cassandra;

namespace Akka.Persistence.Cassandra.Session
{
    public class CassandraSessionSettings
    {
        public CassandraSessionSettings(Config config) : this(
            pageSize: config.GetInt("max-result-size"),
            readConsistentcy: (ConsistencyLevel)Enum.Parse(typeof(ConsistencyLevel), config.GetString("read-consistency"), true),
            writeConsistentcy: (ConsistencyLevel)Enum.Parse(typeof(ConsistencyLevel), config.GetString("write-consistency"), true),
            connectionRetries: config.GetInt("connect-retries"),
            connectionRetryDelay: config.GetTimeSpan("connect-retry-delay"))
        {
        }

        public CassandraSessionSettings(int pageSize, ConsistencyLevel readConsistentcy, ConsistencyLevel writeConsistentcy, int connectionRetries, TimeSpan connectionRetryDelay)
        {
            PageSize = pageSize;
            ReadConsistentcy = readConsistentcy;
            WriteConsistentcy = writeConsistentcy;
            ConnectionRetries = connectionRetries;
            ConnectionRetryDelay = connectionRetryDelay;
        }

        public int PageSize { get; set; }

        public ConsistencyLevel ReadConsistentcy { get; set; }

        public ConsistencyLevel WriteConsistentcy { get; set; }

        public int ConnectionRetries { get; set; }

        public TimeSpan ConnectionRetryDelay { get; set; }
    }
}