namespace MonetDb.Mapi.Helpers.Mapi
{
    using System;
    using System.Collections.Generic;
    using System.Text;

    public class Lexer
    {
        private enum StringType
        {
            None,
            String,
            Number
        }

        public static IEnumerable<string> Parse(string s, char start, char end)
        {
            var e = s.GetEnumerator();
            while (e.MoveNext())
            {
                if (e.Current == start)
                {
                    break;
                }
            }

            var val = new StringBuilder();
            var escape = false;

            while (e.MoveNext())
            {
                if (e.Current == end)
                {
                    break;
                }
                else if (e.Current == ',')
                {
                    yield return val.ToString();
                    val.Clear();
                }
                else if (e.Current == 't')
                {
                    yield return ReadWord(e, "true");
                }
                else if (e.Current == 'T')
                {
                    yield return ReadWord(e, "TRUE");
                }
                else if (e.Current == 'f')
                {
                    yield return ReadWord(e, "false");
                }
                else if (e.Current == 'F')
                {
                    yield return ReadWord(e, "FALSE");
                }
                else if (e.Current == 'n')
                {
                    yield return ReadWord(e, "null");
                }
                else if (e.Current == 'N')
                {
                    yield return ReadWord(e, "NULL");
                }
                else if (e.Current == ' ' || e.Current == '\t')
                {
                    continue;
                }
                else if (e.Current >= '0' && e.Current <= '9' || e.Current == '.' || e.Current == '-' || e.Current == '+')
                {
                    val.Append(e.Current);
                    var hasE = false;
                    var hasDot = e.Current == '.';
                    while (e.MoveNext())
                    {
                        if (e.Current == ',')
                        {
                            yield return val.ToString();
                            val.Clear();
                            break;
                        }

                        val.Append(e.Current);
                        if (e.Current == '.')
                        {
                            if (hasDot)
                            {
                                Throw(val);
                            }

                            hasDot = true;
                        }
                        else if (e.Current == 'e' || e.Current == 'E')
                        {
                            if (hasE)
                            {
                                Throw(val);
                            }

                            hasE = true;
                            e.MoveNext();
                            val.Append(e.Current);
                            if (!(e.Current >= '0' && e.Current <= '9' || e.Current == '-' || e.Current == '+'))
                            {
                                Throw(val);
                            }
                        }
                        else if (e.Current < '0' || e.Current > '9')
                        {
                            Throw(val);
                        }
                    }
                }
                else if (e.Current == '"')
                {
                    val.Append(e.Current);
                    while (e.MoveNext())
                    {
                        val.Append(e.Current);
                        if (escape)
                        {
                            escape = false;
                        }
                        else if (e.Current == '"')
                        {
                            break;
                        }
                        else if (e.Current == '\\')
                        {
                            escape = true;
                        }
                    }
                }
                else
                {
                    Throw(val);
                }
            }
        }

        private static string ReadWord(CharEnumerator charEnumerator, string word)
        {
            var we = word.GetEnumerator();
            while (we.MoveNext())
            {
                if (charEnumerator.Current != we.Current|| !charEnumerator.MoveNext())
                {
                    throw new ArgumentException($"Unrecognized char '{word[0]}'");
                }
            }

            return word;
        }

        private static void Throw(StringBuilder val)
        {
            throw new ArgumentException($"Unrecognized char sequence '{val.ToString()}'");
        }
    }
}