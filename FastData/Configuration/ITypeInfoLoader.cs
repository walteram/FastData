using System;
using System.Reflection;

namespace FastData.Configuration
{
    public interface ITypeInfoLoader
    {
        PropertyInfo[] GetMappedProperties(Type type);

        PropertyInfo GetProperty(Type type, string name);

        string GetColumnName(PropertyInfo property);

        string GetTableName(Type type);

        T GetCustomAttribute<T>(MemberInfo member) where T : Attribute;
        
        string GetPrimaryKeyName(Type type);

        string GetChangesDetectionColumnName(Type type);

        string GetCreationDateColumnName(Type type);

        bool CanBeNull(PropertyInfo property);

        PropertyInfo[] GetUniqueKeyNames(Type type);
    }
}
