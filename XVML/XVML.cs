// XV// XVML Parser (optimized single-pass)
// Compatible with .NET 8-9 and Unity 2021+
// Requires Newtonsoft.Json (Json.NET)

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace XVML
{
	public class XVMLElement
	{
		public string Name { get; set; } = string.Empty;
		public Dictionary<string, string> Attributes { get; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
		public List<XVMLElement> Children { get; } = new List<XVMLElement>();

		// --- Lazy RawInnerText using ReadOnlyMemory<char> to avoid extra string allocations ---
		// We store a ReadOnlyMemory<char> instead of a plain string and only allocate string on demand
		internal ReadOnlyMemory<char> RawSpan;
		private string? _lazyRawInnerText;

		public string RawInnerText
		{
			get
			{
				if (_lazyRawInnerText == null)
					_lazyRawInnerText = RawSpan.ToString();
				return _lazyRawInnerText;
			}
			set
			{
				_lazyRawInnerText = value;
				RawSpan = value.AsMemory();
				ClearLazyContent();
			}
		}

		private JToken? _lazyContent = null;
		public JToken Content
		{
			get
			{
				if (_lazyContent == null)
				{
					if (!string.IsNullOrWhiteSpace(RawInnerText))
					{
						try
						{
							var normalized = SoftJsonNormalizer.Normalize(RawInnerText);
							_lazyContent = JToken.Parse(normalized);
						}
						catch
						{
							try
							{
								_lazyContent = SoftJsonNormalizer.CoerceSimpleValue(RawInnerText);
							}
							catch
							{
								_lazyContent = JValue.CreateNull();
							}
						}
					}
					else
					{
						_lazyContent = JValue.CreateNull();
					}
				}
				return _lazyContent;
			}
			set => _lazyContent = value;
		}

		public void ClearLazyContent() => _lazyContent = null;

		public string? Id => Attributes.TryGetValue("id", out var v) ? v : null;

		public override string ToString()
		{
			var attr = Attributes.Count > 0 ? string.Join(" ", Attributes.Select(kv => $"{kv.Key}=\"{kv.Value}\"")) : "";
			var ct = (_lazyContent == null) ? "(not parsed)" : Content.Type.ToString();
			return $"<{Name} {attr}> -> Content: {ct}";
		}
	}

	public static class SoftJsonNormalizer
	{
		public static string Normalize(string soft)
		{
			if (string.IsNullOrWhiteSpace(soft)) return "null";

			soft = Regex.Replace(soft, @"<!--(.*?)-->", string.Empty, RegexOptions.Singleline);
			soft = Regex.Replace(soft, @"(^|\n)\s*(//|#).*?(\r?\n|$)", "$1", RegexOptions.Multiline);
			soft = soft.Trim();

			if (soft.StartsWith("{") || soft.StartsWith("["))
			{
				soft = ReplaceSingleQuotes(soft);
				if (TryParseJson(soft, out _)) return soft;
			}

			var lines = SplitPreservingBraces(soft);
			bool isArray = lines.FirstOrDefault(l => !string.IsNullOrWhiteSpace(l))?.TrimStart().StartsWith("[") ?? false;

			if (isArray)
			{
				var items = new List<string>();
				foreach (var l in lines)
				{
					var t = l.Trim();
					if (string.IsNullOrEmpty(t) || t == "[" || t == "]") continue;
					items.Add(NormalizeValue(t));
				}
				return "[" + string.Join(",", items) + "]";
			}
			else
			{
				var pairs = new List<string>();
				foreach (var l in lines)
				{
					var t = l.Trim();
					if (string.IsNullOrEmpty(t)) continue;
					if (t.StartsWith("<") && t.EndsWith(">")) continue;

					var colonMatch = Regex.Match(t, @"^\s*""?([a-zA-Z0-9_:-]+)""?\s*[:=]\s*(.*)$");
					if (colonMatch.Success)
					{
						var key = colonMatch.Groups[1].Value;
						var rawVal = colonMatch.Groups[2].Value.Trim().TrimEnd(',');
						pairs.Add(QuoteKey(key) + ":" + NormalizeValue(rawVal));
					}
					else
					{
						var kvMatch = Regex.Match(t, @"^([a-zA-Z0-9_:-]+)\s+(.+)$");
						if (kvMatch.Success)
						{
							pairs.Add(QuoteKey(kvMatch.Groups[1].Value) + ":" + NormalizeValue(kvMatch.Groups[2].Value.Trim()));
						}
						else
						{
							var trimmed = t.Trim();
							if ((trimmed.StartsWith("{") && trimmed.EndsWith("}")) || (trimmed.StartsWith("[") && trimmed.EndsWith("]")))
							{
								pairs.Add(QuoteKey("_inline_") + ":" + trimmed);
							}
						}
					}
				}

				var json = "{" + string.Join(",", pairs) + "}";
				json = ReplaceSingleQuotes(json);
				if (TryParseJson(json, out _)) return json;

				return JsonConvert.SerializeObject(soft);
			}
		}

		public static string NormalizeValue(string raw)
		{
			if (string.IsNullOrWhiteSpace(raw)) return "null";
			var t = raw.Trim();

			if ((t.StartsWith("[") && t.EndsWith("]")) || (t.StartsWith("{") && t.EndsWith("}")))
			{
				if (t.StartsWith("[") && !t.Contains(","))
				{
					var inner = t.Substring(1, t.Length - 2).Trim();
					var parts = Regex.Split(inner, @"[\s,]+").Where(s => !string.IsNullOrWhiteSpace(s)).ToArray();
					return "[" + string.Join(",", parts.Select(NormalizePrimitive)) + "]";
				}
				return t;
			}

			if (Regex.IsMatch(t, "^(true|false)$", RegexOptions.IgnoreCase)) return t.ToLowerInvariant();
			if (Regex.IsMatch(t, @"^-?\d+$")) return t;
			if (Regex.IsMatch(t, @"^-?\d+\.\d+$")) return double.Parse(t, NumberStyles.Float, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture);

			var m = Regex.Match(t, "^\"(.*)\"$");
			if (m.Success) return JsonConvert.SerializeObject(m.Groups[1].Value);
			return JsonConvert.SerializeObject(t);
		}

		private static string NormalizePrimitive(string p)
		{
			p = p.Trim();
			if (Regex.IsMatch(p, @"^-?\d+$")) return p;
			if (Regex.IsMatch(p, @"^-?\d+\.\d+$")) return p;
			if (Regex.IsMatch(p, @"^(true|false)$", RegexOptions.IgnoreCase)) return p.ToLowerInvariant();
			return JsonConvert.SerializeObject(p.Trim('"', '\''));
		}

		private static string QuoteKey(string k) => (k.StartsWith("\"") && k.EndsWith("\"")) ? k : JsonConvert.SerializeObject(k);
		private static string ReplaceSingleQuotes(string s) => Regex.Replace(s, "'([^']*)'", "\"$1\"");

		private static bool TryParseJson(string s, out JToken? token)
		{
			try
			{
				token = JToken.Parse(s);
				return true;
			}
			catch
			{
				token = null;
				return false;
			}
		}

		public static JToken CoerceSimpleValue(string raw)
		{
			var t = raw.Trim();
			if (string.IsNullOrEmpty(t)) return JValue.CreateNull();
			if (Regex.IsMatch(t, @"^-?\d+$")) return new JValue(long.Parse(t, CultureInfo.InvariantCulture));
			if (Regex.IsMatch(t, @"^-?\d+\.\d+$")) return new JValue(double.Parse(t, NumberStyles.Float, CultureInfo.InvariantCulture));
			if (Regex.IsMatch(t, @"^(true|false)$", RegexOptions.IgnoreCase)) return new JValue(bool.Parse(t.ToLowerInvariant()));
			return new JValue(t.Trim('"', '\''));
		}

		private static List<string> SplitPreservingBraces(string text)
		{
			var lines = new List<string>();
			using (var reader = new StringReader(text))
			{
				string? line;
				while ((line = reader.ReadLine()) != null)
				{
					lines.Add(line);
				}
			}
			return lines;
		}
	}

	public static class XVMLParser
	{
		public const string CurrentVersion = "1.0";

		// precompiled regexes for performance
		private static readonly Regex TagOpen = new Regex(@"<([a-zA-Z0-9_:-]+)([^>]*)>", RegexOptions.Compiled);
		private static readonly Regex TagClose = new Regex(@"</([a-zA-Z0-9_:-]+)>", RegexOptions.Compiled);
		private static readonly Regex AttrRegex = new Regex(@"([a-zA-Z0-9_:-]+)\s*=\s*(?:""([^""]*)""|'([^']*)'|([^\s/>]+))|([a-zA-Z0-9_:-]+)", RegexOptions.Compiled);

		public static List<XVMLElement> ParseFile(string path)
		{
			using var reader = new StreamReader(path, Encoding.UTF8);
			var sb = new StringBuilder();
			string? line;
			while ((line = reader.ReadLine()) != null)
				sb.AppendLine(line);
			var text = sb.ToString();
			var basePath = Path.GetDirectoryName(Path.GetFullPath(path)) ?? string.Empty;
			return Parse(text, basePath);
		}

		internal static string XmlDecode(string s) =>
		s?.Replace("&amp;", "&")
		 .Replace("&lt;", "<")
		 .Replace("&gt;", ">")
		 .Replace("&quot;", "\"")
		 .Replace("&apos;", "'") ?? "";

		// single-pass parse: build completedElements list and assign children by span
		public static List<XVMLElement> Parse(string text, string basePath = "")
		{
			var completed = new List<(XVMLElement elem, int start, int end)>();
			var stack = new Stack<(string name, int openIndex, int tagEndIndex, Dictionary<string, string> attrs)>();
			int pos = 0;
			int len = text.Length;

			while (pos < len)
			{
				var open = TagOpen.Match(text, pos);
				var close = TagClose.Match(text, pos);

				// decide which tag comes next
				if (open.Success && (!close.Success || open.Index < close.Index))
				{
					// found opening tag
					string name = open.Groups[1].Value;
					string attrText = open.Groups[2].Value ?? "";

					// detect self-closing <tag/> or <tag ... />
					bool selfClosing = false;
					string trimmedTag = open.Value.TrimEnd();

					if (trimmedTag.EndsWith("/>", StringComparison.Ordinal))
					{
						selfClosing = true;

						// remove trailing slash from attrText if present (e.g. id="5"/ -> id="5")
						var atTrim = attrText.TrimEnd();
						if (atTrim.EndsWith("/", StringComparison.Ordinal))
						{
							int lastSlash = atTrim.LastIndexOf('/');
							if (lastSlash >= 0)
								attrText = atTrim.Remove(lastSlash).TrimEnd();
							else
								attrText = atTrim; // defensive
						}
					}

					// parse attributes from the cleaned attrText
					var attrs = ParseAttributes(attrText);

					// advance position past this tag
					pos = open.Index + open.Length;

					// if not self-closing — push opening into stack for later matching
					if (!selfClosing)
					{
						stack.Push((name, open.Index, open.Index + open.Length, attrs));
					}
					else
					{
						// create element immediately for self-closing tag
						var elem = new XVMLElement { Name = name };
						foreach (var kv in attrs) elem.Attributes[kv.Key] = kv.Value;
						elem.RawInnerText = string.Empty;

						int elemStart = open.Index;
						int elemEnd = open.Index + open.Length;
						completed.Add((elem, elemStart, elemEnd));
					}
				}
				else if (close.Success)
				{
					var closeName = close.Groups[1].Value;
					pos = close.Index + close.Length;

					if (stack.Count == 0)
					{
						// unmatched close - ignore
						continue;
					}

					// pop until matching open found
					(string name, int openIndex, int tagEndIndex, Dictionary<string, string> attrs) top = stack.Pop();
					if (!string.Equals(top.name, closeName, StringComparison.OrdinalIgnoreCase))
					{
						bool found = false;
						var temp = new Stack<(string name, int openIndex, int tagEndIndex, Dictionary<string, string> attrs)>();
						temp.Push(top);
						while (stack.Count > 0)
						{
							var t = stack.Pop();
							temp.Push(t);
							if (string.Equals(t.name, closeName, StringComparison.OrdinalIgnoreCase))
							{
								top = t;
								found = true;
								break;
							}
						}
						while (temp.Count > 0) stack.Push(temp.Pop());
						if (!found) continue; // unmatched close
					}

					int innerStart = top.tagEndIndex;
					int innerEnd = close.Index;

					// create element
					var elem = new XVMLElement { Name = top.name };

					// copy attributes
					foreach (var kv in top.attrs) elem.Attributes[kv.Key] = kv.Value;

					// attach children from completed list whose spans are within (innerStart..innerEnd)
					var children = new List<(XVMLElement elem, int start, int end)>();
					for (int i = completed.Count - 1; i >= 0; i--)
					{
						var c = completed[i];
						if (c.start >= innerStart && c.end <= innerEnd)
						{
							children.Add(c);
							completed.RemoveAt(i);
							continue;
						}
						// optimization: completed is in document order by start; if this entry starts before innerStart,
						// then earlier ones also start before innerStart -> we can break
						if (c.start < innerStart)
						{
							break;
						}
					}
					// children currently in reverse document order; restore order
					children.Reverse();
					foreach (var ch in children) elem.Children.Add(ch.elem);

					// build RawInnerText that excludes child tag spans (fast linear sweep)
					if (children.Count == 0)
					{
						if (innerEnd > innerStart && innerStart < text.Length)
							elem.RawInnerText = XmlDecode(text.Substring(innerStart, Math.Max(0, innerEnd - innerStart)).Trim());
						else elem.RawInnerText = string.Empty;
					}
					else
					{
						var sb = new StringBuilder();
						int p = innerStart;
						foreach (var ch in children)
						{
							if (ch.start > p)
								sb.Append(text, p, ch.start - p);
							p = ch.end;
						}
						if (p < innerEnd)
							sb.Append(text, p, innerEnd - p);
						elem.RawInnerText = sb.ToString().Trim();
					}

					// set element span: from openIndex to end of closing tag
					int elemStart = top.openIndex;
					int elemEnd = close.Index + close.Length;

					// process include: if element is <include file="..."> then inline parse included file immediately
					if (string.Equals(elem.Name, "include", StringComparison.OrdinalIgnoreCase) &&
	elem.Attributes.TryGetValue("file", out var includeFile))
					{
						try
						{
							var includePath = !Path.IsPathRooted(includeFile) ? Path.Combine(basePath, includeFile) : includeFile;
							if (File.Exists(includePath))
							{
								var includedElements = new List<XVMLElement>();
								// parallel read/parse
								includedElements = XVMLParser.ParseFile(includePath);
								foreach (var ie in includedElements)
									elem.Children.Add(ie);
							}
						}
						catch { /* tolerant */ }
					}

					// add to completed
					completed.Add((elem, elemStart, elemEnd));
				}
				else
				{
					break;
				}
			}

			// remaining completed elements that were not assigned to a parent are roots
			var roots = completed.OrderBy(c => c.start).Select(c => c.elem).ToList();

			// If no tags but text non-empty, optionally produce one root element? To remain tolerant, create __root__ only if there is text outside tags
			if (roots.Count == 0)
			{

			}

			return roots;
		}

		private static Dictionary<string, string> ParseAttributes(string attrText)
		{
			var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
			if (string.IsNullOrWhiteSpace(attrText)) return dict;

			int pos = 0;
			while (pos < attrText.Length)
			{
				// skip spaces
				while (pos < attrText.Length && char.IsWhiteSpace(attrText[pos])) pos++;
				if (pos >= attrText.Length) break;

				int start = pos;
				while (pos < attrText.Length && attrText[pos] != '=' && !char.IsWhiteSpace(attrText[pos])) pos++;
				string key = attrText.Substring(start, pos - start);

				// skip '=' and spaces
				while (pos < attrText.Length && (char.IsWhiteSpace(attrText[pos]) || attrText[pos] == '=')) pos++;

				string value = "";
				if (pos < attrText.Length)
				{
					if (attrText[pos] == '"' || attrText[pos] == '\'')
					{
						char quote = attrText[pos++];
						int valStart = pos;
						while (pos < attrText.Length && attrText[pos] != quote) pos++;
						value = attrText.Substring(valStart, pos - valStart);
						pos++;
					}
					else
					{
						int valStart = pos;
						while (pos < attrText.Length && !char.IsWhiteSpace(attrText[pos])) pos++;
						value = attrText.Substring(valStart, pos - valStart);
					}
				}

				dict[key] = value;
			}

			return dict;
		}

		// Fast child-removal helper (not used anymore; kept for API compatibility)
		private static string RemoveChildTags(string inner)
		{
			if (string.IsNullOrEmpty(inner)) return inner;
			var result = new StringBuilder();
			int pos = 0, depth = 0;
			while (pos < inner.Length)
			{
				if (inner[pos] == '<')
				{
					if (pos + 1 < inner.Length && inner[pos + 1] == '/')
					{
						depth = Math.Max(0, depth - 1);
						int end = inner.IndexOf('>', pos);
						if (end == -1) break;
						pos = end + 1;
					}
					else
					{
						depth++;
						int end = inner.IndexOf('>', pos);
						if (end == -1) break;
						pos = end + 1;
					}
				}
				else
				{
					if (depth == 0) result.Append(inner[pos]);
					pos++;
				}
			}
			return result.ToString().Trim();
		}
	}

	public class XVMLNode
	{
		internal XVMLElement Element { get; }

		public XVMLNode(string name)
		{
			Element = new XVMLElement { Name = name };
		}

		internal XVMLNode(XVMLElement element)
		{
			Element = element ?? throw new ArgumentNullException(nameof(element));
		}

		public string Name => Element.Name;

		public string Value
		{
			get => Element.RawInnerText;
			set
			{
				Element.RawInnerText = value;
				Element.ClearLazyContent();
			}
		}

		public Dictionary<string, string> Attributes => Element.Attributes;

		// cached children to avoid allocating every time; invalidated when counts differ
		private List<XVMLNode>? _childrenCache;
		private int _childrenCacheVersion = -1;

		public List<XVMLNode> Children
		{
			get
			{
				if (_childrenCache == null)
				{
					_childrenCache = new List<XVMLNode>(Element.Children.Count);
					foreach (var e in Element.Children)
						_childrenCache.Add(new XVMLNode(e));
					_childrenCacheVersion = Element.Children.Count;
				}
				return _childrenCache;
			}
		}

		public XVMLNode this[string key]
		{
			get
			{
				var child = Element.Children.FirstOrDefault(c => c.Name == key);
				if (child == null)
				{
					child = new XVMLElement { Name = key };
					Element.Children.Add(child);
					// invalidate cache
					_childrenCache = null;
					_childrenCacheVersion = -1;
				}
				return new XVMLNode(child);
			}
			set
			{
				if (value == null) throw new ArgumentNullException(nameof(value));
				var existing = Element.Children.FirstOrDefault(c => c.Name == key);
				if (existing != null) Element.Children.Remove(existing);
				Element.Children.Add(value.Element);
				_childrenCache = null;
				_childrenCacheVersion = -1;
			}
		}

		// Safe text extraction (tries to prefer explicit child text rather than nested __root__)
		public string GetTextValue()
		{
			// If there are children that are not __root__, prefer first such child's text
			foreach (var child in Children)
			{
				if (!string.Equals(child.Name, "__root__", StringComparison.OrdinalIgnoreCase))
					return child.GetTextValue();
			}

			if (Children.Count == 1 && Children[0].Name == "__root__")
				return Children[0].Value.Trim();

			return Value.Trim();
		}
	}

	public class XVMLDocument : XVMLNode
	{
		public XVMLDocument() : base("__root__") { }

		// --- Maintain a fast lookup dictionary for elements by 'id' attribute ---
		private Dictionary<string, XVMLNode> _idIndex = new(StringComparer.OrdinalIgnoreCase);

		// Register node with 'id' for fast access
		internal void RegisterId(XVMLNode node)
		{
			if (node.Attributes.TryGetValue("id", out var id) && !string.IsNullOrEmpty(id))
				_idIndex[id] = node;
		}

		// Public API for internal fast id lookup
		public XVMLNode? GetById(string id) => _idIndex.TryGetValue(id, out var node) ? node : null;

		public static XVMLDocument Load(string path)
		{
			var doc = new XVMLDocument();
			if (!File.Exists(path)) return doc;

			var elements = XVMLParser.ParseFile(path);
			if (elements.Count > 0)
			{
				if (elements.Count == 1 && elements[0].Name == "__root__")
				{
					doc.Element.Children.AddRange(elements[0].Children);
					doc.Element.RawInnerText = elements[0].RawInnerText;
				}
				else
				{
					doc.Element.Children.AddRange(elements);
				}
			}
			return doc;
		}

		public void Save(string path, bool prettyPrint = true)
		{
			string text;
			if (this.Element.Name == "__root__")
			{
				var sb = new StringBuilder();
				foreach (var child in Element.Children)
					sb.Append(prettyPrint ? SerializeNodePretty(child, 0) : SerializeNode(child));
				text = sb.ToString();
			}
			else
			{
				text = prettyPrint ? SerializeNodePretty(Element, 0) : SerializeNode(Element);
			}

			var dir = Path.GetDirectoryName(Path.GetFullPath(path));
			if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir)) Directory.CreateDirectory(dir);

			File.WriteAllText(path, text, Encoding.UTF8);
		}

		private string SerializeNode(XVMLElement element)
		{
			var attrs = element.Attributes.Count > 0
				? " " + string.Join(" ", element.Attributes.Select(kv => $"{kv.Key}=\"{EscapeAttribute(kv.Value)}\""))
				: "";
			var childrenText = string.Concat(element.Children.Select(SerializeNode));
			var inner = element.RawInnerText ?? "";
			return $"<{element.Name}{attrs}>{inner}{childrenText}</{element.Name}>";
		}

		private string SerializeNodePretty(XVMLElement element, int indentLevel)
		{
			var indent = new string(' ', indentLevel * 2);
			var attrs = element.Attributes.Count > 0
				? " " + string.Join(" ", element.Attributes.Select(kv => $"{kv.Key}=\"{EscapeAttribute(kv.Value)}\""))
				: "";

			var hasInner = !string.IsNullOrWhiteSpace(element.RawInnerText);
			string innerText = "";
			if (hasInner)
			{
				var lines = element.RawInnerText.Replace("\r\n", "\n").Split('\n');
				innerText = "\n" + string.Join("\n", lines.Select(l => new string(' ', (indentLevel + 1) * 2) + l.TrimEnd()));
			}

			var children = element.Children;
			if (children.Count == 0 && !hasInner)
			{
				return $"{indent}<{element.Name}{attrs} />\n";
			}
			else
			{
				var sb = new StringBuilder();
				sb.Append($"{indent}<{element.Name}{attrs}>");
				if (hasInner || children.Count > 0) sb.Append(innerText);
				if (children.Count > 0)
				{
					sb.Append("\n");
					foreach (var ch in children) sb.Append(SerializeNodePretty(ch, indentLevel + 1));
					sb.Append($"{indent}</{element.Name}>\n");
				}
				else
				{
					sb.Append($"\n{indent}</{element.Name}>\n");
				}
				return sb.ToString();
			}
		}

		private static string EscapeAttribute(string s)
		{
			if (string.IsNullOrEmpty(s)) return "";
			return s.Replace("&", "&amp;")
					.Replace("\"", "&quot;")
					.Replace("<", "&lt;")
					.Replace(">", "&gt;");
		}
	}

	public static class JsonToXVMLConverter
	{
		/// <summary>
		/// Parse JSON string and convert to XVMLDocument.
		/// Rules:
		/// - properties starting with '@' => attributes (e.g. "@id":"1" -> id="1")
		/// - property "#text" => element inner text
		/// - arrays => repeated child elements with the property name
		/// - primitives => inner text of an element
		/// </summary>
		public static XVMLDocument FromJsonString(string json, string rootName = "__root__")
		{
			if (string.IsNullOrWhiteSpace(json)) return new XVMLDocument();

			var token = JToken.Parse(json);
			return FromJToken(token, rootName);
		}

		public static XVMLDocument FromJToken(JToken token, string rootName = "__root__")
		{
			var doc = new XVMLDocument();

			if (token is JObject job)
			{
				foreach (var p in job.Properties())
					AddPropertyToParent(doc.Element, p.Name, p.Value);
			}
			else if (token is JArray arr)
			{
				// top-level array -> create many elements named rootName
				foreach (var item in arr)
					doc.Element.Children.Add(ConvertToken(rootName, item));
			}
			else // primitive
			{
				var el = new XVMLElement { Name = rootName, RawInnerText = token.ToString() };
				doc.Element.Children.Add(el);
			}

			return doc;
		}

		private static void AddPropertyToParent(XVMLElement parent, string propName, JToken value)
		{
			if (value is JArray arr)
			{
				// for arrays — create repeated elements with same name
				foreach (var item in arr)
				{
					if (item is JValue)
					{
						var simple = new XVMLElement { Name = propName, RawInnerText = ConvertJValueToString((JValue)item) };
						parent.Children.Add(simple);
					}
					else
					{
						parent.Children.Add(ConvertToken(propName, item));
					}
				}
			}
			else if (value is JObject obj)
			{
				parent.Children.Add(ConvertToken(propName, obj));
			}
			else // JValue
			{
				var v = value as JValue;
				var el = new XVMLElement { Name = propName, RawInnerText = ConvertJValueToString(v) };
				parent.Children.Add(el);
			}
		}

		private static XVMLElement ConvertToken(string name, JToken token)
		{
			var el = new XVMLElement { Name = name };

			if (token is JObject obj)
			{
				foreach (var p in obj.Properties())
				{
					// attribute convention: keys that start with '@'
					if (p.Name.Length > 1 && p.Name[0] == '@')
					{
						var attrName = p.Name.Substring(1);
						// attribute value — convert to plain string
						el.Attributes[attrName] = p.Value.Type == JTokenType.Null ? "" : ConvertJValueToString(p.Value as JValue ?? new JValue(p.Value.ToString()));
					}
					else if (p.Name == "#text")
					{
						// explicit inner text
						el.RawInnerText = p.Value.Type == JTokenType.Null ? "" : ConvertJValueToString(p.Value as JValue ?? new JValue(p.Value.ToString()));
					}
					else
					{
						AddPropertyToParent(el, p.Name, p.Value);
					}
				}
			}
			else if (token is JArray arr)
			{
				// array at this position -> create repeated children with the same name
				foreach (var item in arr)
				{
					if (item is JValue)
					{
						var child = new XVMLElement { Name = name, RawInnerText = ConvertJValueToString((JValue)item) };
						el.Children.Add(child);
					}
					else
					{
						el.Children.Add(ConvertToken(name, item));
					}
				}
			}
			else if (token is JValue v)
			{
				el.RawInnerText = ConvertJValueToString(v);
			}

			return el;
		}

		private static string ConvertJValueToString(JValue? v)
		{
			if (v == null || v.Type == JTokenType.Null) return string.Empty;

			// Keep numbers/culture invariant formatting for floats
			if (v.Type == JTokenType.Float)
				return Convert.ToDouble(v.Value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture);

			if (v.Type == JTokenType.Boolean)
				return v.Value<bool>() ? "true" : "false";

			return v.ToString() ?? string.Empty;
		}

		/// <summary>
		/// Convenience: parse JSON and save as .xvml (uses XVMLDocument.Save)
		/// </summary>
		public static void SaveJsonAsXVMLFile(string json, string filePath, string rootName = "__root__", bool prettyPrint = true)
		{
			var doc = FromJsonString(json, rootName);
			doc.Save(filePath, prettyPrint);
		}
	}

	public static class XVMLToJsonConverter
	{
		/// <summary>
		/// Convert XVMLDocument to JSON string
		/// Rules:
		/// - Element attributes become properties prefixed with '@'
		/// - Element inner text becomes "#text" property
		/// - Child elements become nested objects or arrays
		/// - Multiple elements with same name become arrays
		/// </summary>
		public static string ToJsonString(XVMLDocument document, Formatting formatting = Formatting.Indented)
		{
			var rootObject = ConvertElementToJObject(document.Element);
			return rootObject.ToString(formatting);
		}

		/// <summary>
		/// Convert XVMLNode to JSON string
		/// </summary>
		public static string ToJsonString(XVMLNode node, Formatting formatting = Formatting.Indented)
		{
			var token = ConvertElementToJToken(node.Element);
			return token.ToString(formatting);
		}

		/// <summary>
		/// Convert XVMLDocument to JObject
		/// </summary>
		public static JObject ToJObject(XVMLDocument document)
		{
			return ConvertElementToJObject(document.Element);
		}

		/// <summary>
		/// Convert XVMLNode to JToken
		/// </summary>
		public static JToken ToJToken(XVMLNode node)
		{
			return ConvertElementToJToken(node.Element);
		}

		private static JObject ConvertElementToJObject(XVMLElement element)
		{
			var obj = new JObject();

			// Add attributes as properties prefixed with '@'
			foreach (var attr in element.Attributes)
			{
				obj.Add("@" + attr.Key, attr.Value);
			}

			// Process children - group by name to detect arrays
			var childrenGroups = element.Children.GroupBy(c => c.Name);

			foreach (var group in childrenGroups)
			{
				var childrenList = group.ToList();

				if (childrenList.Count == 1)
				{
					// Single child - convert to object or value
					var child = childrenList[0];
					obj[group.Key] = ConvertElementToJToken(child);
				}
				else
				{
					// Multiple children with same name - create array
					var array = new JArray();
					foreach (var child in childrenList)
					{
						array.Add(ConvertElementToJToken(child));
					}
					obj[group.Key] = array;
				}
			}

			// Add inner text if present
			if (!string.IsNullOrWhiteSpace(element.RawInnerText))
			{
				// If we already have content, add as #text property
				if (obj.HasValues || element.Attributes.Count > 0)
				{
					obj["#text"] = ParseInnerTextToJValue(element.RawInnerText);
				}
				else
				{
					// If element only has text content, return just the value
					return JObject.FromObject(new { value = ParseInnerTextToJValue(element.RawInnerText) });
				}
			}

			return obj;
		}

		private static JToken ConvertElementToJToken(XVMLElement element)
		{
			// If element has attributes, children, or mixed content, use object representation
			if (element.Attributes.Count > 0 || element.Children.Count > 0 ||
				(!string.IsNullOrWhiteSpace(element.RawInnerText) && element.Children.Count > 0))
			{
				return ConvertElementToJObject(element);
			}

			// If element only has text content, return as simple value
			if (!string.IsNullOrWhiteSpace(element.RawInnerText))
			{
				return ParseInnerTextToJValue(element.RawInnerText);
			}

			// Empty element without content
			return string.IsNullOrEmpty(element.Name) ? JValue.CreateNull() : new JObject();
		}

		private static JValue ParseInnerTextToJValue(string innerText)
		{
			if (string.IsNullOrWhiteSpace(innerText))
				return JValue.CreateString(string.Empty);

			var trimmed = innerText.Trim();

			// Try to parse as boolean
			if (bool.TryParse(trimmed, out bool boolResult))
				return new JValue(boolResult);

			// Try to parse as integer
			if (long.TryParse(trimmed, NumberStyles.Integer, CultureInfo.InvariantCulture, out long longResult))
				return new JValue(longResult);

			// Try to parse as double
			if (double.TryParse(trimmed, NumberStyles.Float, CultureInfo.InvariantCulture, out double doubleResult))
				return new JValue(doubleResult);

			// Try to parse as JSON null
			if (trimmed.Equals("null", StringComparison.OrdinalIgnoreCase))
				return JValue.CreateNull();

			// Return as string
			return new JValue(trimmed);
		}

		/// <summary>
		/// Save XVML document as JSON file
		/// </summary>
		public static void SaveAsJsonFile(XVMLDocument document, string filePath, Formatting formatting = Formatting.Indented)
		{
			var json = ToJsonString(document, formatting);
			var dir = Path.GetDirectoryName(Path.GetFullPath(filePath));
			if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir))
				Directory.CreateDirectory(dir);

			File.WriteAllText(filePath, json, Encoding.UTF8);
		}

		/// <summary>
		/// Save XVML node as JSON file
		/// </summary>
		public static void SaveAsJsonFile(XVMLNode node, string filePath, Formatting formatting = Formatting.Indented)
		{
			var json = ToJsonString(node, formatting);
			var dir = Path.GetDirectoryName(Path.GetFullPath(filePath));
			if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir))
				Directory.CreateDirectory(dir);

			File.WriteAllText(filePath, json, Encoding.UTF8);
		}
	}
}