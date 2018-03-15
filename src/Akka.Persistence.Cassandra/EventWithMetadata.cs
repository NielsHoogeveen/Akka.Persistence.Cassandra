using System;

namespace Akka.Persistence.Cassandra
{
    /// <summary>
    /// If the event is wrapped in this class the <see cref="Metadata"/> will
    /// be serialized and stored in the `meta` column. This can be used by event
    /// adapters or other tools to store additional meta data without altering
    /// the actual domain event.
    /// </summary>
    public sealed class EventWithMetadata
    {
        public object Event { get; }
        public object Metadata { get; }

        public EventWithMetadata(object @event, object metadata)
        {
            Event = @event;
            Metadata = metadata;
        }
    }

    /// <summary>
    /// If the event is wrapped in this class the <see cref="Metadata"/> will
    /// be serialized and stored in the `meta` column. This can be used by event
    /// adapters or other tools to store additional meta data without altering
    /// the actual domain event.
    /// </summary>
    public sealed class SnapshotWithMetadata
    {
        public object Event { get; }
        public object Metadata { get; }

        public SnapshotWithMetadata(object @event, object metadata)
        {
            Event = @event;
            Metadata = metadata;
        }
    }

    /// <summary>
    /// If meta data could not be deserialized it will not fail the replay/query.
    /// The "invalid" meta data is represented with this <see cref="UnknownMetadata"/> and
    /// it and the event will be wrapped in <see cref="EventWithMetadata"/> or <see cref="SnapshotWithMetadata"/>.
    /// 
    /// The reason for not failing the replay/query is that meta data should be
    /// optional, e.g. the tool that wrote the meta data has been removed. This
    /// is typically because the serializer for the meta data has been removed
    /// from the class path (or configuration).
    /// </summary>
    public sealed class UnknownMetadata : IEquatable<UnknownMetadata>
    {
        public int SerializerId { get; }
        public string Manifest { get; }

        public UnknownMetadata(int serializerId, string manifest)
        {
            SerializerId = serializerId;
            Manifest = manifest;
        }

        public bool Equals(UnknownMetadata other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return SerializerId == other.SerializerId && string.Equals(Manifest, other.Manifest);
        }

        public override bool Equals(object obj) => obj is UnknownMetadata metadata && Equals(metadata);

        public override int GetHashCode()
        {
            unchecked
            {
                return (SerializerId * 397) ^ (Manifest != null ? Manifest.GetHashCode() : 0);
            }
        }

        public override string ToString() => $"UnknownMetadata(serializerId: {SerializerId}, manifest: [{Manifest}])";
    }
}