using System;
using System.Threading.Tasks;
using Akka.Streams;
using Akka.Streams.Stage;

namespace Akka.Persistence.Cassandra.Query
{
    public class EventsByPersistenceIdStage<T> : GraphStageWithMaterializedValue<SourceShape<T>, IEventsByPersistenceIdController>
    {
        public override ILogicAndMaterializedValue<IEventsByPersistenceIdController> CreateLogicAndMaterializedValue(Attributes inheritedAttributes)
        {
            throw new System.NotImplementedException();
        }

        public override SourceShape<T> Shape { get; }
    }

    public interface IEventsByPersistenceIdController : IDisposable
    {
        void Poll(long knownSequenceNr);
        void FastForward(long nextSequenceNr);
        Task CompletionTask { get; }
    }
}