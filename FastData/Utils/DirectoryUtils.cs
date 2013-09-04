using System;
using System.IO;
using System.Linq;
using System.Reflection;
using FastData.Configuration;

namespace FastData.Utils
{
    /// <summary>
    /// Funções úteis relacionadas a diretórios e arquivos
    /// </summary>
    internal static class DirectoryUtils
    {
        /// <summary>
        /// Retorna o caminho atual da dll em execução.
        /// </summary>
        public static string GetTypeAssemblyPath(Type type)
        {
            var directory = Path.GetDirectoryName(Assembly.GetAssembly(type).CodeBase);
            var path = directory != null ? directory.ToLower().Replace("file:\\\\", "").Replace("file:\\", "") : "";
            if (!path.EndsWith(@"\"))
            {
                path += @"\";
            }
            return path;
        }

        /// <summary>
        /// Retorna o caminho atual da dll em execução.
        /// </summary>
        public static string GetCallingAssemblyPath()
        {
            var directory = Path.GetDirectoryName(Assembly.GetCallingAssembly().CodeBase);
            var path = directory != null ? directory.ToLower().Replace("file:\\\\", "").Replace("file:\\", "") : "";
            if (!path.EndsWith(@"\"))
            {
                path += @"\";
            }
            return path;
        }

        private static string _assemblyPath;

        /// <summary>
        /// Retorna o caminho atual da dll em execução.
        /// </summary>
        public static string AssemblyPath
        {
            get
            {
                if (String.IsNullOrEmpty(_assemblyPath))
                {
                    _assemblyPath = GetTypeAssemblyPath(typeof(FastDataOptions));
                }                
                return _assemblyPath;
            }
        }

        public static string MapPath(params string[] subPaths)
        {
            return subPaths.Aggregate(AssemblyPath, Path.Combine);
        }
    }
}
