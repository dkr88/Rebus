﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Rebus.Exceptions;

namespace Rebus.Transport
{
    /// <summary>
    /// Nifty extensions to the transaction context, mostly working on the <see cref="ITransactionContext.Items"/> dictionary
    /// </summary>
    public static class TransactionContextExtensions
    {
        /// <summary>
        /// Gets the item with the given key and type from the dictionary of objects, returning null if the key does not exist.
        /// If the key exists, but the object could not be cast to the given type, a nice exception is throws
        /// </summary>
        public static T GetOrNull<T>(this ITransactionContext context, string key) where T : class
        {
            object item;

            if (!context.Items.TryGetValue(key, out item))
            {
                return default(T);
            }

            if (!(item is T))
            {
                throw new ArgumentException(string.Format("Found item with key '{0}' but it was a {1} and not of type {2} as expected",
                    key, item.GetType(), typeof(T)));
            }

            return (T)item;
        }

        /// <summary>
        /// Gets the item with the given key and type from the dictionary of objects, throwing a nice exception if either the key
        /// does not exist, or the found value cannot be cast to the given type
        /// </summary>
        public static T GetOrThrow<T>(this ITransactionContext context, string key)
        {
            object item;

            if (!context.Items.TryGetValue(key, out item))
            {
                throw new KeyNotFoundException(string.Format("Could not find an item with the key '{0}'", key));
            }

            if (!(item is T))
            {
                throw new ArgumentException(string.Format("Found item with key '{0}' but it was a {1} and not of type {2} as expected",
                    key, item.GetType(), typeof(T)));
            }

            return (T)item;
        }

        /// <summary>
        /// Provides a shortcut to the transaction context's <see cref="ConcurrentDictionary{TKey,TValue}.GetOrAdd(TKey,System.Func{TKey,TValue})"/>,
        /// only as a typed version that 
        /// </summary>
        public static TItem GetOrAdd<TItem>(this ITransactionContext context, string key, Func<TItem> newItemFactory)
        {
            try
            {
                return (TItem)context.Items.GetOrAdd(key, id => newItemFactory());
            }
            catch (Exception exception)
            {
                throw new RebusApplicationException(string.Format("Could not 'GetOrAdd' item with key '{0}' as type {1}",
                    key, typeof(TItem)), exception);
            }
        }
    }
}