using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Threading.Tasks;
using Rebus.Logging;
using Rebus.Pipeline;
using Rebus.Sagas;

namespace Rebus.Persistence.EntityFramework.Sagas
{
    public class EntityFrameworkSagaStorage : ISagaStorage
    {
        private readonly Func<DbContext> _contextFactory;
        private readonly Dictionary<ISagaData, DbContext> _contexts;

        private static ILog Log;

        /// <summary>
        /// Very basic implementation of <see cref="ISagaStorage" /> that uses an Entity Framework <see cref="DbContext" />
        /// to persist saga data.
        /// </summary>
        /// <param name="contextFactory"></param>
        public EntityFrameworkSagaStorage(Func<DbContext> contextFactory)
        {
            _contextFactory = contextFactory;
            _contexts = new Dictionary<ISagaData, DbContext>();

            RebusLoggerFactory.Changed += f => Log = f.GetCurrentClassLogger();

            Log.Debug("Initialized");
        }

        public async Task<ISagaData> Find(Type sagaDataType, string propertyName, object propertyValue)
        {
            Log.Debug("DbContext find [{0} = {1}]", propertyName, propertyValue);
            var context = _contextFactory();
            try
            {
                var entitySet = GetSet(sagaDataType, context);
                var results = await entitySet
                    .SqlQuery("SELECT * FROM [" + sagaDataType.Name + "] WHERE [" + propertyName + "] = @p0", propertyValue)
                    .ToListAsync();
                var sagaData = results.FirstOrDefault() as ISagaData;
                if (sagaData != null)
                    RememberContext(sagaData, context);
                return sagaData;
            }
            catch
            {
                context.Dispose();
                throw;
            }
        }

        public async Task Insert(ISagaData sagaData, IEnumerable<ISagaCorrelationProperty> correlationProperties)
        {
            var context = GetOrCreateContext(sagaData);
            GetSet(sagaData.GetType(), context)
                .Add(sagaData);
            await context.SaveChangesAsync();
        }

        public async Task Update(ISagaData sagaData, IEnumerable<ISagaCorrelationProperty> correlationProperties)
        {
            await GetOrCreateContext(sagaData, true).SaveChangesAsync();
        }

        public async Task Delete(ISagaData sagaData)
        {
            var tempContext = false;
            var context = GetContext(sagaData);
            if (context == null)
            {
                context = _contextFactory();
                tempContext = true;
            }
            try
            {
                GetSet(sagaData.GetType(), context).Remove(sagaData);
                await context.SaveChangesAsync();
            }
            finally
            {
                if (tempContext)
                    context.Dispose();
            }
        }

        /// <summary>
        /// Remembed a contact that is used for a specified instance of saga data
        /// (so that we can save changes on the same context later)
        /// </summary>
        /// <param name="sagaData"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        private DbContext RememberContext(ISagaData sagaData, DbContext context)
        {
            _contexts.Add(sagaData, context);
            Log.Debug("DbContext remembered [{0} saved contexts]", _contexts.Count);

            // When the current message transaction ends, the DbContext should be disposed
            MessageContext.Current.TransactionContext.OnDisposed(() =>
            {
                _contexts.Remove(sagaData);
                context.Dispose();
                Log.Debug("DbContext disposed [{0} saved contexts]", _contexts.Count);
            });

            return context;
        }

        /// <summary>
        /// Retrieve a context for the saga data, or return null if none exists
        /// </summary>
        /// <param name="sagaData"></param>
        /// <returns></returns>
        private DbContext GetContext(ISagaData sagaData)
        {
            if (!_contexts.ContainsKey(sagaData))
            {
                Log.Debug("DbContext cache miss");
                return null;
            }
            return _contexts[sagaData];
        }

        /// <summary>
        /// Get an existing context for the saga data, or if one does not
        /// already exist then create and remember it
        /// </summary>
        /// <param name="sagaData"></param>
        /// <param name="attachModified">whether to attach the saga data to the context immediately as modified</param>
        /// <returns></returns>
        private DbContext GetOrCreateContext(ISagaData sagaData, bool attachModified = false)
        {
            var context = GetContext(sagaData);
            if (context == null)
            {
                context = RememberContext(sagaData, _contextFactory());
                if (attachModified)
                    context.Entry(sagaData).State = EntityState.Modified;
                Log.Debug("DbContext created");
            }
            return context;
        }

        /// <summary>
        /// Get the DbSet that stores objects of the specified type
        /// </summary>
        /// <param name="type"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        private DbSet GetSet(Type type, DbContext context)
        {
            return context.Set(type);
        }
    }
}
