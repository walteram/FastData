using System;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace FastData.Utils
{
    /// <summary>
    /// Funções úteis relacionadas a strings
    /// </summary>
    internal static class StringUtils
    {
        /// <summary>
        /// Obtem a string com base em um texto em Base64
        /// </summary>
        /// <param name="value"></param>
        /// <param name="encoding"></param>
        /// <returns></returns>
        public static string FromBase64(this string value, Encoding encoding = null)
        {
            if (encoding == null)
            {
                encoding = Encoding.UTF8;
            }
            return encoding.GetString(Convert.FromBase64String(value.Replace("+", " ").Replace(";", "/")));
        }
        
        /// <summary>
        /// Converte uma string de um hexa para um int
        /// </summary>
        /// <param name="hexValue"></param>
        /// <returns></returns>
        public static int ToIntFromHex(this string hexValue)
        {
            int value;
            try
            {
                value = Convert.ToInt32(hexValue, 16);
            }
            catch (Exception)
            {
                value = 0;
            }
            return value;
        }

        /// <summary>
        /// Converte uma string de um hexa para um Int64
        /// </summary>
        /// <param name="hexValue"></param>
        /// <returns></returns>
        public static long ToLongFromHex(this string hexValue)
        {
            Int64 value;
            try
            {
                value = Convert.ToInt64(hexValue, 16);
            }
            catch (Exception)
            {
                value = 0;
            }
            return value;
        }

        /// <summary>
        /// Obtem o valor em Base64 de uma string
        /// </summary>
        /// <param name="value"></param>
        /// <param name="encoding"></param>
        /// <returns></returns>
        public static string ToBase64(this string value, Encoding encoding = null)
        {
            if (encoding == null)
            {
                encoding = Encoding.UTF8;
            }
            return Convert.ToBase64String(encoding.GetBytes(value));            
        }

        /// <summary>
        /// Calcula o MD5 de uma string
        /// </summary>
        /// <param name="value"></param>
        /// <param name="encoding"></param>
        /// <returns></returns>
        public static string ToMd5(this string value, Encoding encoding = null)
        {
            if (encoding == null)
            {
                encoding = Encoding.UTF8;
            }
            var md5 = MD5.Create();
            var inputBytes = encoding.GetBytes(value);
            var hash = md5.ComputeHash(inputBytes);
            var sb = new StringBuilder();
            for (var i = 0; i < hash.Length; i++)
            {
                sb.Append(hash[i].ToString("X2"));
            }
            return sb.ToString();
        }

        /// <summary>
        /// Remove caracteres de acentuação e espaços de uma string
        /// </summary>
        /// <param name="text"></param>
        /// <returns></returns>
        public static string RemoveSpecialChars(this string text)
        {
            const string past = "ÄÅÁÂÀÃäáâàãÉÊËÈéêëèÍÎÏÌíîïìÖÓÔÒÕöóôòõÜÚÛüúûùÇç ";
            const string future = "AAAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUuuuuCc_";
            const string not = "?#\"'\\/:<>|*-+";
            for (var i = 0; i < past.Length; i++)
            {
                text = text.Replace(past[i].ToString(), future[i].ToString());
            }
            for (var i = 0; i < not.Length; i++)
            {
                text = text.Replace(not[i].ToString(), "");
            }
            return text;
        }

        /// <summary>
        /// Inverte uma string.
        /// </summary>
        /// <param name="valueToReverse"></param>
        /// <returns></returns>
        public static string Reverse(this string valueToReverse)
        {
            var array = valueToReverse.ToCharArray();
            Array.Reverse(array);
            return (new string(array));
        }

        /// <summary>
        /// Remove os ultimos caracteres do texto que corresponderem ao parametro
        /// </summary>
        /// <param name="value"></param>
        /// <param name="rightText"></param>
        /// <returns></returns>
        public static string RemoveRightChar(this string value, string rightText)
        {
            if (value.EndsWith(rightText))
                value = value.Substring(0, value.Length - rightText.Length);
            return value;
        }

        /// <summary>
        /// Converte uma string para um texto compatível HTML (ex.: Ç para &Ccedil;)
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static string HtmlEncode(this string value)
        {
            return WebUtility.HtmlEncode(value);
        }

        /// <summary>
        /// Converte uma string em HTML para um texto normal (ex.: &Ccedil; para Ç)
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static string HtmlDecode(this string value)
        {
            return WebUtility.HtmlDecode(value);
        }

        /// <summary>
        /// Regex 
        /// </summary>
        public static bool RegexMatch(object value, string regex)
        {
            try
            {
                return Regex.Match(value.ToString(), regex).Success;
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// Limita o tamanho de uma string
        /// </summary>
        public static string LimitLength(this string value, int maxlength)
        {
            if (maxlength > 0 && value.Length > maxlength)
            {
                value = value.Substring(0, maxlength);
            }
            return value;
        }

        /// <summary>
        /// Coverte uma data para string no formato especificado
        /// </summary>
        public static string ToString(this DateTime? value, string format)
        {
            return !value.HasValue ? "" : value.Value.ToString(format);
        }

        /// <summary>
        /// Compara duas versões
        /// -1 = versao antiga é menor que versao nova
        /// 0  = mesma versão ou versão não reconhecida
        /// 1  = versão antiga é maior que versao nova
        /// </summary>
        public static int CompareWithPreviousVersion(this string newVersion, string oldVersion)
        {
            Version oldVer, newVer;
            if (newVersion == null || oldVersion == null || !Version.TryParse(oldVersion, out oldVer) || !Version.TryParse(newVersion, out newVer))
            {
                return 0;
            }
            return oldVer.CompareTo(newVer);
        }

        public static string GetTypeConverter(Type type)
        {
            if (type == typeof(string))
            {
                return "Convert.ToString";
            }
            if (type == typeof(long))
            {
                return "Convert.ToInt64";
            }
            if (type == typeof(int))
            {
                return "Convert.ToInt32";
            }
            if (type == typeof(Int16))
            {
                return "Convert.ToInt16";
            }
            if (type == typeof(DateTime))
            {
                return "Convert.ToDateTime";
            }
            if (type == typeof(bool))
            {
                return "Convert.ToBoolean";
            }
            if (type == typeof(decimal))
            {
                return "Convert.ToDecimal";
            }
            if (type.IsEnum)
            {
                return "(" + type.FullName.Replace("+",".") + ")";    
            }
            return "(" + type.FullName + ")";
        }
    }
}
