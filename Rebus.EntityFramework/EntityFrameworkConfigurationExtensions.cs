using System;
using System.Data.Entity;
using Rebus.Config;
using Rebus.Sagas;

namespace Rebus.Persistence.EntityFramework
{
    public static class EntityFrameworkConfigurationExtensions
    {
        /// <summary>
        /// Configures Rebus to use Entity Framework to store sagas
        /// </summary>
        public static void StoreInEntityFramework(this StandardConfigurer<ISagaStorage> configurer, Func<DbContext> context)
        {
            if (configurer == null) throw new ArgumentNullException("configurer");
            if (context == null) throw new ArgumentNullException("context");

            configurer.Register(c =>
            {
                var sagaStorage = new Sagas.EntityFrameworkSagaStorage(context);
                return sagaStorage;
            });
        }
    }
}
