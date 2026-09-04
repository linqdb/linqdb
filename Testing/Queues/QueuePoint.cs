using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Testing.Queues
{
    [ProtoContract]
    public class QueuePoint
    {
        [ProtoMember(1)]
        public DateTime Time { get; set; }
        [ProtoMember(2)]
        public double Value { get; set; }
    }
}
