using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LinqdbClient
{
    public class LinqdbSelectStatistics
    {
        /// <summary>
        ///  Total number of records satisfying criterias.
        /// </summary>
        public int? Total { get; set; }
        /// <summary>
        ///  In case when time limited search is used shows what percentage of data was searched.
        /// </summary>
        public double? SearchedPercentile { get; set; }
        /// <summary>
        ///  Vector distances to the target vector.
        /// </summary>
        public Dictionary<int, double> Distances { get; set; }
        /// <summary>
        ///  Vector distances to the target vector.
        /// </summary>
        public Dictionary<Tuple<int, int>, double> DistancesDist { get; set; }
    }
}
