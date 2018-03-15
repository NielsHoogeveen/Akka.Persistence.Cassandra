using System;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Cassandra;

namespace Akka.Persistence.Cassandra
{
    /// <summary>
    /// The implementation of the <see cref="ISessionProvider"/> is used for creating the
    /// Cassandra Session. By default the <see cref="ConfigSessionProvider"/> is building
    /// the Cluster from configuration properties but it is possible to
    /// replace the implementation of the SessionProvider to reuse another
    /// session or override the Cluster builder with other settings.
    /// 
    /// The implementation is defined in configuration `session-provider` property.
    /// It may optionally have a constructor with an ActorSystem and Config parameter.
    /// The config parameter is the config section of the plugin.
    /// </summary>
    public interface ISessionProvider
    {
        Task<ISession> ConnectAsync();
    }

    internal static class SessionProvider
    {
        public static ISessionProvider CreateSessionProvider(ExtendedActorSystem system, Config config)
        {
            var typeName = config.GetString("session-provider");
            var type = Type.GetType(typeName, throwOnError: true);

            var instance = (TryCreate(type, system, config)
                   ?? TryCreate(type, system)
                   ?? TryCreate(type)) as ISessionProvider;

            return instance ?? throw new ArgumentException($"Cannot create instance of type {typeName} using following constructors:" +
                                                           $"1. {type.Name}(ExtendedActorSystem, Config)" +
                                                           $"2. {type.Name}(ExtendedActorSystem)" +
                                                           $"3. {type.Name}()");
        }

        private static object TryCreate(Type type, params object[] args)
        {
            try
            {
                return Activator.CreateInstance(type, args);
            }
            catch (Exception e)
            {
                return null;
            }
        }
    }
}