using System;

namespace FunctionEGDWDumper
{
    // TODO, move this class to a Contracts assembly that is shared across different projects

    class StoveTemps
    {
        public string timestamp { get; set; }
        public string eventName { get; set; }
        public float SupplyTemp { get; set; }
        public float ReturnTemp { get; set; }
        public float ChargeLevel { get; set; }
    }
}