using System;

namespace FastData
{
    public partial class TableMapTemplate
    {
        public readonly Type Type;
        public readonly string NamespaceName;
        public readonly string ClassName;

        public TableMapTemplate(Type type, string namespaceName, string className)
        {
            Type = type;
            NamespaceName = namespaceName;
            ClassName = className;
        }
    }
}
