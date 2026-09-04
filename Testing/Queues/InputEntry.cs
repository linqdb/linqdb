using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Testing.Queues
{
    [ProtoContract]
    public class InputEntry
    {
        [ProtoMember(1)]
        public int Id { get; set; }
        [ProtoMember(2)]
        public string Guid { get; set; }
        [ProtoMember(3)]
        public DateTime Date { get; set; }
        [ProtoMember(4)]
        public string Program { get; set; }
        [ProtoMember(5)]
        public string Input { get; set; }
        [ProtoMember(6)]
        public int Lang { get; set; }
        [ProtoMember(7)]
        public string Compiler_args { get; set; }
        [ProtoMember(8)]
        public int Processed { get; set; }
    }
}
