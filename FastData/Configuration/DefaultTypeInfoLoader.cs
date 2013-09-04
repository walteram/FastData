using System;
using System.Collections.Generic;
using System.Data.Linq.Mapping;
using System.Linq;
using System.Reflection;

namespace FastData.Configuration
{
    public class DefaultTypeInfoLoader : ITypeInfoLoader
    {
        public PropertyInfo[] GetMappedProperties(Type type)
        {
            return type.GetProperties().Where(it => it.GetCustomAttributes(typeof(ColumnAttribute), true).Length > 0).ToArray();
        }

        public PropertyInfo GetProperty(Type type, string name)
        {
            return type.GetProperty(name);
        }

        public string GetColumnName(PropertyInfo property)
        {
            var columnAttribute = GetCustomAttribute<ColumnAttribute>(property);
            if(columnAttribute == null || string.IsNullOrEmpty(columnAttribute.Name))
            {
                return property.Name;
            }
            return columnAttribute.Name;
        }

        public string GetTableName(Type type)
        {
            var tableAttribute = GetCustomAttribute<TableAttribute>(type);
            if (tableAttribute == null || string.IsNullOrEmpty(tableAttribute.Name))
            {
                return type.Name;
            }
            return tableAttribute.Name;
        }

        public T GetCustomAttribute<T>(MemberInfo member) where T : Attribute
        {
            var attribute = member.GetCustomAttributes(typeof(T), true);
            if (attribute.Length == 0)
            {
                return null;
            }
            return (T)attribute[0];
        }

        public string GetPrimaryKeyName(Type type)
        {
            var properties = type.GetProperties();
            foreach (var property in properties)
            {
                var columnAttribute = GetCustomAttribute<ColumnAttribute>(property);
                if (columnAttribute != null && columnAttribute.IsPrimaryKey)
                {
                    return columnAttribute.Name;
                }                
            }
            return "Id";
        }

        public string GetChangesDetectionColumnName(Type type)
        {
            var property = FastDataOptions.Instance.TypeInfoLoader.GetProperty(type, "LastUpdateAt");
            return property == null ? GetPrimaryKeyName(type) : FastDataOptions.Instance.TypeInfoLoader.GetColumnName(property);
        }

        public string GetCreationDateColumnName(Type type)
        {
            return "CreatedAt";
        }

        public bool CanBeNull(PropertyInfo property)
        {
            return property.PropertyType.IsGenericType && property.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>) || property.PropertyType == typeof(string);
        }

        public PropertyInfo[] GetUniqueKeyNames(Type type)
        {
            var properties = new List<PropertyInfo>();
            foreach (var property in type.GetProperties())
            {
                var columnAttribute = GetCustomAttribute<ColumnAttribute>(property);
                if (columnAttribute != null && columnAttribute.Expression=="''")
                {
                    properties.Add(property);
                }
            }
            return properties.ToArray();
        }
    }
}
