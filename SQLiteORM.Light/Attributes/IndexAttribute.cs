using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SQLiteORM.Attributes
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
    public class IndexAttribute : Attribute
    {
        public string Name { get; set; }
        public bool IsUnique { get; set; }
        public int Order { get; set; }
        public bool AutoCreate { get; set; } = true;
        public IndexAttribute() { }
        public IndexAttribute(string name, bool isUnique = false, int order = 0, bool autoCreate = true)
        {
            Name = name;
            IsUnique = isUnique;
            Order = order;
            AutoCreate = autoCreate;
        }
    }
}
