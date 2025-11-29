using System;
using System.Collections.Generic;
using System.Reflection;
using System.Linq;
using XVML;

namespace XVML.Serializer
{
	/// <summary>
	/// Simple object-to-XVML serializer and deserializer
	/// </summary>
	public static class XVMLSerializer<T> where T : class, new()
	{
		/// <summary>
		/// Serialize object to XVMLDocument
		/// </summary>
		public static XVMLDocument Serialize(T obj, string rootName = "__root__")
		{
			if (obj == null) throw new ArgumentNullException(nameof(obj));

			var doc = new XVMLDocument();
			var rootNode = doc.CreatePath(rootName);

			SerializeObject(obj, rootNode);

			return doc;
		}

		/// <summary>
		/// Deserialize object from XVMLDocument
		/// </summary>
		public static T Deserialize(XVMLDocument doc, string rootName = "__root__")
		{
			if (doc == null) throw new ArgumentNullException(nameof(doc));

			var rootNode = doc[rootName];
			return DeserializeObject(rootNode, typeof(T)) as T;
		}

		#region Internal helpers

		private static void SerializeObject(object obj, XVMLNode node)
		{
			var type = obj.GetType();
			var props = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);

			foreach (var prop in props)
			{
				var value = prop.GetValue(obj);
				if (value == null) continue;

				if (IsSimpleType(prop.PropertyType))
				{
					// Simple value -> store as inner text
					var child = node.AddChild(prop.Name, value.ToString());
				}
				else if (typeof(System.Collections.IEnumerable).IsAssignableFrom(prop.PropertyType))
				{
					// Enumerable -> multiple children
					var enumerable = value as System.Collections.IEnumerable;
					if (enumerable != null)
					{
						foreach (var item in enumerable)
						{
							var child = node.AddChild(prop.Name);
							SerializeObject(item, child);
						}
					}
				}
				else
				{
					// Complex object -> nested child
					var child = node.AddChild(prop.Name);
					SerializeObject(value, child);
				}
			}
		}

		private static object DeserializeObject(XVMLNode node, Type type)
		{
			var obj = Activator.CreateInstance(type);
			var props = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);

			foreach (var prop in props)
			{
				var childNodes = node.GetChildren(prop.Name);
				if (childNodes.Count == 0) continue;

				if (IsSimpleType(prop.PropertyType))
				{
					// Take first child for simple value
					var val = Convert.ChangeType(childNodes[0].Value, prop.PropertyType);
					prop.SetValue(obj, val);
				}
				else if (typeof(System.Collections.IList).IsAssignableFrom(prop.PropertyType))
				{
					var elementType = prop.PropertyType.IsArray
						? prop.PropertyType.GetElementType()
						: prop.PropertyType.GetGenericArguments().FirstOrDefault();

					if (elementType == null) continue;

					var list = (System.Collections.IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(elementType));

					foreach (var c in childNodes)
					{
						var item = DeserializeObject(c, elementType);
						list.Add(item);
					}

					if (prop.PropertyType.IsArray)
					{
						var arr = Array.CreateInstance(elementType, list.Count);
						list.CopyTo(arr, 0);
						prop.SetValue(obj, arr);
					}
					else
					{
						prop.SetValue(obj, list);
					}
				}
				else
				{
					// Nested object
					var nested = DeserializeObject(childNodes[0], prop.PropertyType);
					prop.SetValue(obj, nested);
				}
			}

			return obj;
		}

		private static bool IsSimpleType(Type type)
		{
			return type.IsPrimitive || type == typeof(string) || type == typeof(decimal);
		}

		#endregion
	}
}