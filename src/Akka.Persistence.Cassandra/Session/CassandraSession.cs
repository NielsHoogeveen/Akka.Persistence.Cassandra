using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Akka.Pattern;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Stage;
using Akka.Util;
using Cassandra;

namespace Akka.Persistence.Cassandra.Session
{
    /// <summary>
    /// Data Access Object for Cassandra. The statements are expressed in
    /// <a href="http://docs.datastax.com/en/cql/3.3/cql/cqlIntro.html">Cassandra Query Language</a>
    /// (CQL) syntax.
    /// 
    /// The `init` hook is called before the underlying session is used by other methods,
    /// so it can be used for things like creating the keyspace and tables.
    /// 
    /// All methods are non-blocking.
    /// </summary>
    internal sealed class CassandraSession
    {
        private readonly ActorSystem system;
        private readonly ISessionProvider sessionProvider;
        private readonly CassandraSessionSettings settings;
        private readonly ILoggingAdapter log;
        private readonly Func<ISession, Task> init;
        private readonly Lazy<IMaterializer> materializer;

        // cache of PreparedStatement (PreparedStatement should only be prepared once)
        private readonly ConcurrentDictionary<string, Task<PreparedStatement>> preparedStatements = new ConcurrentDictionary<string, Task<PreparedStatement>>();
        private readonly Func<string, Task<PreparedStatement>> computePreparedStatement;
        private readonly AtomicReference<ISession> underlyingSession = new AtomicReference<ISession>();

        public CassandraSession(ActorSystem system, ISessionProvider sessionProvider, CassandraSessionSettings settings, ILoggingAdapter log, Func<ISession, Task> init)
        {
            this.system = system;
            this.sessionProvider = sessionProvider;
            this.settings = settings;
            this.log = log;
            this.init = init;
            this.materializer = new Lazy<IMaterializer>(() => this.system.Materializer());
            this.computePreparedStatement = async key =>
            {
                var session = await UnderlyingAsync();
                try
                {
                    return await session.PrepareAsync(key);
                }
                catch (Exception)
                {
                    preparedStatements.TryRemove(key, out _);
                    throw;
                }
            };
        }

        public ProtocolVersion? ProtocolVersion
        {
            get
            {
                var task = UnderlyingAsync();
                if (task.IsCompleted)
                {
                    var p = task.Result.Cluster.Configuration.ProtocolOptions.MaxProtocolVersion;
                    if (p.HasValue) return (ProtocolVersion) p.Value;
                    else return default(ProtocolVersion?);
                }

                throw new IllegalStateException("protocolVersion can only be accessed after successful init");
            }
        }

        /// <summary>
        /// The <see cref="ISession"/> of the underlying
        /// <a href="http://datastax.github.io/java-driver/">Datastax Java Driver</a>.
        /// Can be used in case you need to do something that is not provided by the
        /// API exposed by this class. Be careful to not use blocking calls.
        /// </summary>
        public async Task<ISession> UnderlyingAsync()
        {
            var existing = underlyingSession.Value;
            if (existing != null) return existing;

            var session = await sessionProvider.ConnectAsync();
            try
            {
                if (underlyingSession.CompareAndSet(null, session))
                {
                    system.RegisterOnTermination(session.Dispose);
                    return session;
                }
                else
                {
                    session.Dispose();
                    return underlyingSession.Value;
                }
            }
            catch (Exception)
            {
                underlyingSession.CompareAndSet(session, null);
                session.Dispose();
                throw;
            }
        }

        /// <summary>
        /// See <a href="http://docs.datastax.com/en/cql/3.3/cql/cql_using/useCreateTableTOC.html">Creating a table</a>.
        /// 
        /// The returned `Future` is completed when the table has been created,
        /// or if the statement fails.
        /// </summary>
        public async Task ExecuteCreateTableAsync(IStatement statement)
        {
            var session = await UnderlyingAsync();
            await session.ExecuteAsync(statement);
        }

        /// <summary>
        /// Create a `PreparedStatement` that can be bound and used in
        /// `executeWrite` or `select` multiple times.
        /// </summary>
        public async Task<PreparedStatement> PrepareAsync(string statement)
        {
            var session = await UnderlyingAsync();
            return await preparedStatements.GetOrAdd(statement, computePreparedStatement);
        }

        /// <summary>
        /// Execute several statements in a batch. First you must <see cref="PrepareAsync"/> the
        /// statements and bind its parameters.
        /// 
        /// See <a href="http://docs.datastax.com/en/cql/3.3/cql/cql_using/useBatchTOC.html">Batching data insertion and updates</a>.
        /// 
        /// The configured write consistency level is used if a specific consistency
        /// level has not been set on the <see cref="BatchStatement"/>.
        /// 
        /// The returned task is completed when the batch has been
        /// successfully executed, or if it fails.
        /// </summary>
        public async Task ExecuteWriteBatchAsync(BatchStatement batch)
        {
            await ExecuteWriteAsync(batch);
        }

        /// <summary>
        /// Execute one statement. First you must <see cref="PrepareAsync"/> the
        /// statement and bind its parameters.
        /// 
        /// See <a href="http://docs.datastax.com/en/cql/3.3/cql/cql_using/useInsertDataTOC.html">Inserting and updating data</a>.
        /// 
        /// The configured write consistency level is used if a specific consistency
        /// level has not been set on the <see cref="IStatement"/>.
        /// 
        /// The returned task is completed when the statement has been
        /// successfully executed, or if it fails.
        /// </summary>
        public async Task ExecuteWriteAsync(IStatement statement)
        {
            if (statement.ConsistencyLevel == null)
                statement.SetConsistencyLevel(settings.WriteConsistentcy);

            var session = await UnderlyingAsync();
            await session.ExecuteAsync(statement);
        }

        /// <summary>
        /// Prepare, bind and execute one statement in one go.
        /// 
        /// See <a href="http://docs.datastax.com/en/cql/3.3/cql/cql_using/useInsertDataTOC.html">Inserting and updating data</a>.
        /// 
        /// The configured write consistency level is used.
        /// 
        /// The returned task is completed when the statement has been
        /// successfully executed, or if it fails.
        /// </summary>
        public async Task ExecuteWriteAsync(string statement, params object[] bindValues)
        {
            var prepared = await PrepareAsync(statement);
            var bound = prepared.Bind(bindValues);
            await ExecuteWriteAsync(bound);
        }

        internal async Task<RowSet> SelectRowSetAsync(IStatement statement)
        {
            if (statement.ConsistencyLevel == null)
                statement.SetConsistencyLevel(settings.ReadConsistentcy);

            var session = await UnderlyingAsync();
            return await session.ExecuteAsync(statement);
        }

        /// <summary>
        /// Execute a select statement. First you must <see cref="PrepareAsync"/> the
        /// statement and bind its parameters.
        /// 
        /// See <a href="http://docs.datastax.com/en/cql/3.3/cql/cql_using/useQueryDataTOC.html">Querying tables</a>.
        /// 
        /// The configured read consistency level is used if a specific consistency
        /// level has not been set on the <see cref="IStatement"/>.
        /// 
        /// Note that you have to connect a <see cref="Sink{TIn,TMat}"/> that consumes the messages from
        /// this <see cref="Source{TOut,TMat}"/> and then `run` the stream.
        /// </summary>
        public Source<Row, NotUsed> Select(IStatement statement)
        {
            if (statement.ConsistencyLevel == null)
                statement.SetConsistencyLevel(settings.ReadConsistentcy);

            return Source.FromGraph(new SelectSource(this, Task.FromResult(statement)));
        }

        /// <summary>
        /// Prepare, bind and execute a select statement in one go.
        /// 
        /// See <a href="http://docs.datastax.com/en/cql/3.3/cql/cql_using/useQueryDataTOC.html">Querying tables</a>.
        /// 
        /// The configured read consistency level is used.
        /// 
        /// Note that you have to connect a <see cref="Sink{TIn,TMat}"/> that consumes the messages from
        /// this <see cref="Source{TOut,TMat}"/> and then `run` the stream.
        /// </summary>
        public Source<Row, NotUsed> Select(string statement, params object[] bindValues)
        {
            async Task<IStatement> BindAsync()
            {
                var prepared = await PrepareAsync(statement);
                var bound = prepared.Bind(bindValues);
                bound.SetConsistencyLevel(settings.ReadConsistentcy);
                return bound;
            }

            return Source.FromGraph(new SelectSource(this, BindAsync()));
        }

        public async Task<IEnumerable<Row>> SelectAllAsync(IStatement statement)
        {
            if (statement.ConsistencyLevel == null)
                statement.SetConsistencyLevel(settings.ReadConsistentcy);

            return await FetchAllAsync(statement);
        }

        public async Task<IEnumerable<Row>> SelectAllAsync(string statement, params object[] bindValues)
        {
            var prepared = await PrepareAsync(statement);
            var bound = prepared.Bind(bindValues);
            bound.SetConsistencyLevel(settings.ReadConsistentcy);

            return await FetchAllAsync(bound);
        }

        private async Task<IEnumerable<Row>> FetchAllAsync(IStatement statement)
        {
            var session = await UnderlyingAsync();
            var rowSet = await session.ExecuteAsync(statement);
            var result = new List<Row>();

            result.AddRange(rowSet.GetRows());
            while (!rowSet.IsExhausted())
            {
                await rowSet.FetchMoreResultsAsync();
                result.AddRange(rowSet.GetRows());
            }

            return result;
        }

        public async Task<Row> SingleAsync(IStatement statement)
        {
            if (statement.ConsistencyLevel == null)
                statement.SetConsistencyLevel(settings.ReadConsistentcy);

            var session = await UnderlyingAsync();
            var rowSet = await session.ExecuteAsync(statement);

            return rowSet.FirstOrDefault();
        }

        public async Task<Row> SingleAsync(string statement, params object[] bindValues)
        {
            var prepared = await PrepareAsync(statement);
            var bound = prepared.Bind(bindValues);
            bound.SetConsistencyLevel(settings.ReadConsistentcy);

            var session = await UnderlyingAsync();
            var rowSet = await session.ExecuteAsync(bound);

            return rowSet.FirstOrDefault();
        }
    }

    internal sealed class SelectSource : GraphStage<SourceShape<Row>>
    {
        #region internal classes

        private sealed class Logic : GraphStageLogic
        {
            private readonly SelectSource stage;
            private Action<RowSet> asyncResult;
            private Action<Exception> asyncFailure;
            private RowSet rowSet;

            public Logic(SelectSource stage) : base(stage.Shape)
            {
                this.stage = stage;

                SetHandler(stage.Outlet, onPull: TryPushOne);
            }

            public override void PreStart()
            {
                asyncResult = GetAsyncCallback<RowSet>(rs =>
                {
                    rowSet = rs;
                    TryPushOne();
                });
                asyncFailure = GetAsyncCallback<Exception>(e =>
                {
                    Fail(stage.Outlet, e ?? new OperationCanceledException("Failed to fetch more results"));
                });

                stage.statementTask.ContinueWith(async task =>
                {
                    if (task.IsFaulted || task.IsCanceled) asyncFailure(task.Exception);
                    else
                    {
                        try
                        {
                            var session = await stage.session.UnderlyingAsync();
                            var rs = await session.ExecuteAsync(task.Result);
                            asyncResult(rs);
                        }
                        catch (Exception e)
                        {
                            asyncFailure(e);
                        }
                    }
                });
            }

            private void TryPushOne()
            {
                if (rowSet != null && IsAvailable(stage.Outlet))
                {
                    if (rowSet.IsExhausted())
                    {
                        Complete(stage.Outlet);
                    }
                    else if (rowSet.GetAvailableWithoutFetching() > 0)
                    {
                        Push(stage.Outlet, rowSet.First());
                    }
                    else
                    {
                        var rs = rowSet;
                        rowSet = null;
                        rs.FetchMoreResultsAsync().ContinueWith(task =>
                        {
                            if (task.IsFaulted || task.IsCanceled) asyncFailure(task.Exception);
                            else asyncResult(rs);
                        });
                    }
                }
            }
        }

        #endregion

        private readonly CassandraSession session;
        private readonly Task<IStatement> statementTask;

        public SelectSource(CassandraSession session, Task<IStatement> statementTask)
        {
            this.session = session;
            this.statementTask = statementTask;
            Outlet = new Outlet<Row>("rows");
            Shape = new SourceShape<Row>(Outlet);
        }

        public Outlet<Row> Outlet { get; }
        public override SourceShape<Row> Shape { get; }

        protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes) => new Logic(this);
    }
}