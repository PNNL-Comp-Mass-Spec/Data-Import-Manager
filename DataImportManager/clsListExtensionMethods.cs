using System;
using System.Threading;
using System.Collections.Generic;

namespace DataImportManager
{
    /// <summary>
    /// This class instantiates a thread safe random number generator
    /// </summary>
    /// <remarks>
    /// From https://stackoverflow.com/questions/273313/randomize-a-listt
    /// </remarks>
    public static class ThreadSafeRandom
    {
        [ThreadStatic] private static Random mRandGenerator;

        /// <summary>
        /// Returns a random number generator
        /// </summary>
        public static Random ThisThreadsRandom =>
            mRandGenerator ??= new Random(unchecked(Environment.TickCount * 31 + Thread.CurrentThread.ManagedThreadId));
    }

    /// <summary>
    /// The Shuffle extension method is used by xmlFilesToImport.Shuffle() in DoDataImportTask
    /// </summary>
    internal static class ListExtensionMethods
    {
        /// <summary>
        /// Shuffle the items in a list
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="list"></param>
        public static void Shuffle<T>(this IList<T> list)
        {
            var n = list.Count;
            while (n > 1)
            {
                n--;
                var k = ThreadSafeRandom.ThisThreadsRandom.Next(n + 1);
                //  Swap items
                var value = list[k];
                list[k] = list[n];
                list[n] = value;
            }
        }
    }
}
