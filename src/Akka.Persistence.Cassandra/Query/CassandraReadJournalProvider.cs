using Akka.Persistence.Query;

namespace Akka.Persistence.Cassandra.Query
{
    public class CassandraReadJournalProvider : IReadJournalProvider
    {
        public IReadJournal GetReadJournal()
        {
            throw new System.NotImplementedException();
        }
    }
}