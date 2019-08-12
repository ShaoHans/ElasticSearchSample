using Nest;
using System;
using System.Collections.Generic;
using System.Text;

namespace ElasticSearchSample.Console
{
    public class Employee
    {
        public long Id { get; set; }

        public string Name { get; set; }

        public DateTime BirthDay { get; set; }

        public string Gender { get; set; }

        public DateTime JoinDate { get; set; }

        public Address Home { get; set; }

        [Text(Fielddata = false)] // mapping中不生成keyword type
        public string Mobile { get; set; }

        public decimal Salary { get; set; }

        public string NewField { get; set; }

        public override string ToString()
        {
            return $"{Id}--{Name}--{Gender}--{Home?.Province}--{BirthDay.Date}--{JoinDate.Date}--{Mobile}--{Salary}";
        }
    }

    public class Address
    {
        public string Province { get; set; }

        public string City { get; set; }
    }
}
