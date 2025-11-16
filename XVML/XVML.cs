// XVML Parser
// Compatible with .NET 8-9 and Unity 2021+
// Requires Newtonsoft.Json (Json.NET)
// If you are using Unity, install NuGetForUnity to easily use the Newtonsoft.Json Nuget package on Unity https://github.com/GlitchEnzo/NuGetForUnity

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace XVML
{
    /// <summary>
    /// Represents a single XVML element (tag). Attributes store metadata (id, hash, version, author...).
    /// RawInnerText contains the soft-json / text between tag open and child tags.
    /// Content is parsed lazily from RawInnerText when first requested.
    /// </summary>
    public class XVMLElement
    {
        public string Name { get; set; } = string.Empty;
        public Dictionary<string, string> Attributes { get; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        public List<XVMLElement> Children { get; } = new List<XVMLElement>();
        public string RawInnerText { get; set; } = string.Empty;

        // Lazy-parsed Content
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
            set
            {
                _lazyContent = value;
            }
        }

        // Convenience accessor for common id metadata
        public string? Id => Attributes.TryGetValue("id", out var v) ? v : null;

        public override string ToString()
        {
            var attr = Attributes.Count > 0 ? string.Join(" ", Attributes.Select(kv => $"{kv.Key}=\"{kv.Value}\"")) : "";
            var ct = (Content == null) ? "null" : Content.Type.ToString();
            return $"<{Name} {attr}> -> Content: {ct}";
        }
    }

	/// <summary>
	/// Represents a node in XVML with convenient accessors.
	/// Wraps XVMLElement for easier use.
	/// </summary>
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
				Element.Content = null; // сброс ленивого парсинга
			}
		}

		public Dictionary<string, string> Attributes => Element.Attributes;

		public List<XVMLNode> Children => Element.Children.Select(e => new XVMLNode(e)).ToList();

		// Исправленный индексатор
		public XVMLNode this[string key]
		{
			get
			{
				// ищем существующий элемент
				var child = Element.Children.FirstOrDefault(c => c.Name == key);
				if (child == null)
				{
					child = new XVMLElement { Name = key };
					Element.Children.Add(child);
				}
				return new XVMLNode(child);
			}
			set
			{
				if (value == null) throw new ArgumentNullException(nameof(value));

				var existing = Element.Children.FirstOrDefault(c => c.Name == key);
				if (existing != null)
					Element.Children.Remove(existing);

				Element.Children.Add(value.Element);
			}
		}
	}

	/// <summary>
	/// Represents a full XVML document.
	/// Provides simple indexers and Load/Save methods.
	/// </summary>
	public class XVMLDocument : XVMLNode
	{
		public XVMLDocument() : base("__root__") { }

		public static XVMLDocument Load(string path)
		{
			var doc = new XVMLDocument();
			if (!File.Exists(path)) return doc;

			var elements = XVMLParser.ParseFile(path);
			doc.Element.Children.AddRange(elements); // Исправлено: doc.Element вместо Element
			return doc;
		}

		public void Save(string path)
		{
			var text = SerializeNode(Element);
			File.WriteAllText(path, text, Encoding.UTF8);
		}

		private string SerializeNode(XVMLElement element)
		{
			var attrs = element.Attributes.Count > 0
				? " " + string.Join(" ", element.Attributes.Select(kv => $"{kv.Key}=\"{kv.Value}\""))
				: "";
			var childrenText = string.Concat(element.Children.Select(SerializeNode));
			return $"<{element.Name}{attrs}>{element.RawInnerText}{childrenText}</{element.Name}>";
		}
	}

	public static class XVMLParser
    {
        /// <summary>
        /// Current format version (update when changing format).
        /// </summary>
        public const string CurrentVersion = "1.0";

        /// <summary>
        /// Parse a file on disk. Includes are resolved relative to the file's directory.
        /// </summary>
        public static List<XVMLElement> ParseFile(string path)
        {
            var text = File.ReadAllText(path, Encoding.UTF8);
            var basePath = Path.GetDirectoryName(Path.GetFullPath(path)) ?? string.Empty;
            return Parse(text, basePath);
        }

        /// <summary>
        /// Parse XVML text. basePath used to resolve includes.
        /// </summary>
        public static List<XVMLElement> Parse(string text, string basePath = "")
        {
            var elements = new List<XVMLElement>();
            int pos = 0;

            // stack: (tagName, innerContentStartIndex, attributes)
            var stack = new Stack<(string name, int startIndex, Dictionary<string, string> attrs)>();

            // simple open/close tag matchers
            var tagOpenRegex = new Regex(@"<([a-zA-Z0-9_:-]+)([^>]*)>", RegexOptions.Compiled);
            var tagCloseRegex = new Regex(@"</([a-zA-Z0-9_:-]+)>", RegexOptions.Compiled);

            while (pos < text.Length)
            {
                var openMatch = tagOpenRegex.Match(text, pos);
                var closeMatch = tagCloseRegex.Match(text, pos);

                if (openMatch.Success && (!closeMatch.Success || openMatch.Index < closeMatch.Index))
                {
                    var name = openMatch.Groups[1].Value;
                    var attrText = openMatch.Groups[2].Value;
                    var attrs = ParseAttributes(attrText);

                    // Handle include immediately (self-closing or not). If include present, inject included elements
                    if (string.Equals(name, "include", StringComparison.OrdinalIgnoreCase) && attrs.TryGetValue("file", out var includeFile))
                    {
                        var includePath = !Path.IsPathRooted(includeFile) ? Path.Combine(basePath, includeFile) : includeFile;
                        if (File.Exists(includePath))
                        {
                            try
                            {
                                var included = ParseFile(includePath);
                                elements.AddRange(included);
                            }
                            catch
                            {
                                // ignore include errors (tolerant behavior)
                            }
                        }
                        // advance position past the open tag and continue
                        pos = openMatch.Index + openMatch.Length;
                        continue;
                    }

                    // push tag to stack (start index points after the opening '>' to capture inner text)
                    stack.Push((name, openMatch.Index + openMatch.Length, attrs));
                    pos = openMatch.Index + openMatch.Length;
                }
                else if (closeMatch.Success)
                {
                    var closeName = closeMatch.Groups[1].Value;
                    pos = closeMatch.Index + closeMatch.Length;

                    if (stack.Count == 0) continue;

                    // pop until matching open found (tolerant to mismatches)
                    var (openName, innerStart, attrs) = stack.Pop();
                    if (!string.Equals(openName, closeName, StringComparison.OrdinalIgnoreCase))
                    {
                        var found = false;
                        var temp = new Stack<(string, int, Dictionary<string, string>)>();
                        while (stack.Count > 0)
                        {
                            var top = stack.Pop();
                            temp.Push(top);
                            if (string.Equals(top.Item1, closeName, StringComparison.OrdinalIgnoreCase))
                            {
                                found = true;
                                (openName, innerStart, attrs) = top;
                                break;
                            }
                        }
                        while (temp.Count > 0) stack.Push(temp.Pop());
                        if (!found) continue; // unmatched close, skip
                    }

                    // extract inner text between open and close
                    var innerText = text.Substring(innerStart, closeMatch.Index - innerStart);
                    var element = new XVMLElement { Name = openName, RawInnerText = innerText };

                    // copy attributes
                    foreach (var kv in attrs) element.Attributes[kv.Key] = kv.Value;

                    // recursively parse children inside innerText
                    var childElements = Parse(innerText, basePath);
                    if (childElements.Count > 0) element.Children.AddRange(childElements);

                    // remove embedded child tags so RawInnerText becomes only the "softjson/text" part
                    var innerTextWithoutChildren = RemoveChildTags(innerText);
                    element.RawInnerText = innerTextWithoutChildren.Trim();

                    // Do NOT eagerly parse Content here — keep lazy behavior.
                    // But for backward compatibility with previous behavior we might set Content if small: (optional)
                    elements.Add(element);
                }
                else
                {
                    break;
                }
            }

            // If no tags were found but text looks like a JSON/soft block, create a root element
            if (elements.Count == 0)
            {
                var trimmed = text.Trim();
                if (!string.IsNullOrEmpty(trimmed))
                {
                    var root = new XVMLElement { Name = "__root__", RawInnerText = trimmed };
                    elements.Add(root);
                }
            }

            return elements;
        }

        /// <summary>
        /// Parse attributes string into a dictionary.
        /// Supports key="value", key='value', key=value (no quotes), or standalone key.
        /// </summary>
        private static Dictionary<string, string> ParseAttributes(string attrText)
        {
            var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            if (string.IsNullOrWhiteSpace(attrText)) return dict;

            // Note: use verbatim string with doubled quotes for "" inside regex
            var regex = new Regex(@"([a-zA-Z0-9_:-]+)\s*=\s*(?:""([^""]*)""|'([^']*)'|([^\s/>]+))|([a-zA-Z0-9_:-]+)", RegexOptions.Compiled);
            var matches = regex.Matches(attrText);
            foreach (Match m in matches)
            {
                if (m.Groups[1].Success)
                {
                    var key = m.Groups[1].Value;
                    var value = m.Groups[2].Success ? m.Groups[2].Value
                              : m.Groups[3].Success ? m.Groups[3].Value
                              : m.Groups[4].Value;
                    dict[key] = value ?? string.Empty;
                }
                else if (m.Groups[5].Success)
                {
                    dict[m.Groups[5].Value] = string.Empty;
                }
            }
            return dict;
        }

        /// <summary>
        /// Remove child tags (and their content) from inner text, leaving only text outside children.
        /// Heuristic: removes patterns like &lt;tag ...&gt;...&lt;/tag&gt;.
        /// </summary>
        private static string RemoveChildTags(string inner)
        {
            var tagRegex = new Regex(@"<([a-zA-Z0-9_:-]+)([^>]*)>(.*?)</\1>", RegexOptions.Singleline);
            return tagRegex.Replace(inner, "");
        }
    }

    /// <summary>
    /// SoftJsonNormalizer provides tolerant normalization from SoftJSON (user-broken JSON-like text)
    /// into valid JSON string. It also provides helpers to coerce simple values.
    /// Keep this tolerant — it's intentionally heuristic and forgiving.
    /// </summary>
    public static class SoftJsonNormalizer
    {
        public static string Normalize(string soft)
        {
            if (string.IsNullOrWhiteSpace(soft)) return "null";

            // remove XML comments and line comments
            soft = Regex.Replace(soft, @"<!--(.*?)-->", string.Empty, RegexOptions.Singleline);
            soft = Regex.Replace(soft, @"(^|\n)\s*(//|#).*?(\r?\n|$)", "$1", RegexOptions.Multiline);
            soft = soft.Trim();

            // if it already looks like JSON object/array try to validate quickly
            if (soft.StartsWith("{") || soft.StartsWith("["))
            {
                soft = ReplaceSingleQuotes(soft);
                if (TryParseJson(soft, out _)) return soft;
            }

            // otherwise normalize line-by-line into an object or array
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
                    if (t.StartsWith("<") && t.EndsWith(">")) continue; // skip tags

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
                            // If it's a raw JSON snippet like {..} or [..], try to inline it under a synthetic key
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

                // last resort: return quoted whole text as string
                return JsonConvert.SerializeObject(soft);
            }
        }

        // Convert SoftJSON value into a valid JSON value string
        public static string NormalizeValue(string raw)
        {
            if (string.IsNullOrWhiteSpace(raw)) return "null";
            var t = raw.Trim();

            // arrays or objects
            if ((t.StartsWith("[") && t.EndsWith("]")) || (t.StartsWith("{") && t.EndsWith("}")))
            {
                if (t.StartsWith("[") && !t.Contains(","))
                {
                    // convert "[1 2 3]" -> "[1,2,3]"
                    var inner = t.Substring(1, t.Length - 2).Trim();
                    var parts = Regex.Split(inner, @"[\s,]+").Where(s => !string.IsNullOrWhiteSpace(s)).ToArray();
                    return "[" + string.Join(",", parts.Select(NormalizePrimitive)) + "]";
                }
                return t;
            }

            // booleans
            if (Regex.IsMatch(t, "^(true|false)$", RegexOptions.IgnoreCase)) return t.ToLowerInvariant();

            // numbers
            if (Regex.IsMatch(t, @"^-?\d+$")) return t;
            if (Regex.IsMatch(t, @"^-?\d+\.\d+$")) return t;

            // quoted string -> ensure proper JSON quoting
            var m = Regex.Match(t, "^\"(.*)\"$");
            if (m.Success) return JsonConvert.SerializeObject(m.Groups[1].Value);

            // else -> treat as bare string
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

        // Try parse into JToken; token is nullable to avoid assigning null to non-nullable type
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

        // Coerce a simple raw value into a JToken (number, bool or string)
        public static JToken CoerceSimpleValue(string raw)
        {
            var t = raw.Trim();
            if (string.IsNullOrEmpty(t)) return JValue.CreateNull();
            if (Regex.IsMatch(t, @"^-?\d+$")) return new JValue(long.Parse(t));
            if (Regex.IsMatch(t, @"^-?\d+\.\d+$")) return new JValue(double.Parse(t));
            if (Regex.IsMatch(t, @"^(true|false)$", RegexOptions.IgnoreCase)) return new JValue(bool.Parse(t.ToLowerInvariant()));
            return new JValue(t.Trim('"', '\''));
        }

        // Split text into lines while preserving bracketed arrays/objects as lines (simple approach)
        private static List<string> SplitPreservingBraces(string text)
        {
            var lines = new List<string>();
            using (var reader = new StringReader(text))
            {
                string? line;
                while ((line = reader.ReadLine()) != null)
                {
                    lines.Add(line); // safe: line != null due to loop condition
                }
            }
            return lines;
        }
    }
}
