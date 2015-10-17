using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.Core.Objects;
using System.Data.Entity.Infrastructure;
using System.Data.Entity.Validation;
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
        private static readonly Dictionary<ISagaData, DbContext> _contexts = new Dictionary<ISagaData, DbContext>();
        private static readonly object _contextsLock = new object();

        private static ILog Log;

        /// <summary>
        /// Very basic implementation of <see cref="ISagaStorage" /> that uses an Entity Framework <see cref="DbContext" />
        /// to persist saga data.
        /// </summary>
        /// <param name="contextFactory"></param>
        public EntityFrameworkSagaStorage(Func<DbContext> contextFactory)
        {
            _contextFactory = contextFactory;

            RebusLoggerFactory.Changed += f => Log = f.GetCurrentClassLogger();

            Log.Debug("Initialized");
        }

        public async Task<ISagaData> Find(Type sagaDataType, string propertyName, object propertyValue)
        {
            Log.Debug("DbContext find [{0} = {1}]", propertyName, propertyValue);
            var context = _contextFactory();
            try
            {
                // Get the PK with a select from the data table
                var dataShim = await context.Database
                    .SqlQuery<SagaDataStub>("SELECT [Id] FROM [" + sagaDataType.Name + "] WHERE [" + propertyName + "] = @p0", propertyValue)
                    .SingleOrDefaultAsync();

                if (dataShim != null)
                {
                    // Retrieve the actual saga data entity
                    var sagaData = await GetDbSet(sagaDataType, context).FindAsync(dataShim.Id) as ISagaData;
                    if (sagaData != null)
                    {
                        RememberContext(sagaData, context);
                        return sagaData;
                    }
                }

                context.Dispose();
                return null;
            }
            catch
            {
                context.Dispose();
                throw;
            }
        }

        private sealed class SagaDataStub
        {
            public Guid Id { get; set; }
        }

        public async Task Insert(ISagaData sagaData, IEnumerable<ISagaCorrelationProperty> correlationProperties)
        {
            var context = GetOrCreateContext(sagaData);
            try
            {
                GetDbSet(sagaData.GetType(), context)
                    .Add(sagaData);
                await context.SaveChangesAsync();
            }
            catch (DbEntityValidationException ex)
            {
                LogValidationErrors(ex);
                throw;
            }
        }

        public async Task Update(ISagaData sagaData, IEnumerable<ISagaCorrelationProperty> correlationProperties)
        {
            try
            {
                await GetOrCreateContext(sagaData, true).SaveChangesAsync();
            }
            catch (DbEntityValidationException ex)
            {
                LogValidationErrors(ex);
                throw;
            }
        }

        private void LogValidationErrors(DbEntityValidationException ex)
        {
            foreach (var error in ex.EntityValidationErrors.SelectMany(e => e.ValidationErrors))
            {
                Log.Error("DbValidationError {0} / {1}", error.PropertyName, error.ErrorMessage);
            }
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
                GetDbSet(sagaData.GetType(), context).Remove(sagaData);
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
            lock (_contextsLock)
            {
                _contexts.Add(sagaData, context);
                Log.Debug("DbContext remembered [{0} saved contexts]", _contexts.Count);

                // When the current message transaction ends, the DbContext should be disposed
                MessageContext.Current.TransactionContext.OnDisposed(() =>
                {
                    lock (_contextsLock)
                    {
                        _contexts.Remove(sagaData);
                        context.Dispose();
                        Log.Debug("DbContext disposed [{0} saved contexts]", _contexts.Count);
                    }
                });

                return context;
            }
        }

        /// <summary>
        /// Retrieve a context for the saga data, or return null if none exists
        /// </summary>
        /// <param name="sagaData"></param>
        /// <returns></returns>
        public static DbContext GetContext(ISagaData sagaData)
        {
            lock (_contextsLock)
            {
                if (!_contexts.ContainsKey(sagaData))
                {
                    Log.Debug("DbContext cache miss");
                    return null;
                }
                return _contexts[sagaData];
            }
        }

        /// <summary>
        /// Load changes for the specified objects from the data source, maintaining any local changes
        /// </summary>
        /// <param name="sagaData"></param>
        /// <param name="objectsToRefresh"></param>
        /// <returns></returns>
        public static async Task RefreshEntities(ISagaData sagaData, IEnumerable<object> objectsToRefresh)
        {
            var context = ((IObjectContextAdapter)GetContext(sagaData)).ObjectContext;
            if (context != null)
            {
                context.DetectChanges();
                await context.RefreshAsync(RefreshMode.ClientWins, objectsToRefresh);
            }
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
            lock (_contextsLock)
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
        }

        /// <summary>
        /// Get the DbSet that stores objects of the specified type
        /// </summary>
        /// <param name="type"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        private DbSet GetDbSet(Type type, DbContext context)
        {
            return context.Set(type);
        }
    }
}
