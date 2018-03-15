using Cassandra;

namespace Akka.Persistence.Cassandra
{
    public sealed class PreparedStatementEnvelope
    {
        public ISession Session { get; }
        public PreparedStatement PreparedStatement { get; }

        public PreparedStatementEnvelope(ISession session, PreparedStatement preparedStatement)
        {
            Session = session;
            PreparedStatement = preparedStatement;
        }
    }
}