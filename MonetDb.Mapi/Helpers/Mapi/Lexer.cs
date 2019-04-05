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

            string res = null;
            var val = new StringBuilder();
            var escape = false;

            while (e.MoveNext())
            {
                if (e.Current == end)
                {
                    break;
                }
                // column separator
                else if (e.Current == ',')
                {
                    res = val.ToString();
                    if (!string.IsNullOrEmpty(res))
                    {
                        yield return res;
                    }

                    val.Clear();
                }
                // boolean -->
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
                // <-- boolean
                // null -->
                else if (e.Current == 'n')
                {
                    yield return ReadWord(e, "null");
                }
                else if (e.Current == 'N')
                {
                    yield return ReadWord(e, "NULL");
                }
                // <-- null
                // nothing
                else if (e.Current == ' ' || e.Current == '\t')
                {
                    continue;
                }
                // number
                // date
                else if (e.Current >= '0' && e.Current <= '9' || e.Current == '.' || e.Current == '-' || e.Current == '+')
                {
                    yield return ReadNumber(e, val, end);
                    val.Clear();
                }
                // string
                else if (e.Current == '"')
                {
                    val.Append(e.Current);
                    while (e.MoveNext())
                    {
                        if (e.Current == '\\' && !escape)
                        {
                            escape = true;
                            continue;
                        }

                        val.Append(e.Current);
                        if (escape)
                        {
                            escape = false;
                        }
                        else if (e.Current == '"')
                        {
                            break;
                        }
                    }
                }
                // hmm...
                else
                {
                    Throw(val);
                }
            }

            res = val.ToString();
            if (!string.IsNullOrEmpty(res))
            {
                yield return res;
            }
        }

        private static string ReadNumber(CharEnumerator e, StringBuilder val, char end)
        {
            val.Append(e.Current);
            var hasE = false;
            var hasDot = e.Current == '.';
            while (e.MoveNext())
            {
                if (e.Current == '\t' || e.Current == ',' || e.Current == end)
                {
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
                else if (e.Current == '-')
                {
                    ReadDateFromNumber(e, val, end);
                    break;
                }
                else if (e.Current < '0' || e.Current > '9')
                {
                    Throw(val);
                }
            }

            return val.ToString();
        }

        /// <summary>
        /// 2018-08-20 10:00:00.000000
        /// </summary>
        /// <param name="e"></param>
        /// <param name="val">2018-</param>
        /// <param name="end"></param>
        private static void ReadDateFromNumber(CharEnumerator e, StringBuilder val, char end)
        {
            ConditionalRead(e, val, x => x >= '0' && x <= '9'); // M
            ConditionalRead(e, val, x => x >= '0' && x <= '9'); // M

            ConditionalRead(e, val, x => x == '-');

            ConditionalRead(e, val, x => x >= '0' && x <= '9'); // d
            ConditionalRead(e, val, x => x >= '0' && x <= '9'); // d

            if (!ConditionalRead(e, val, x => x == ' ', false))
            {
                if (e.Current == '\t' || e.Current == ',' || e.Current == end)
                {
                    return;
                }
            }

            ConditionalRead(e, val, x => x >= '0' && x <= '9'); // H
            ConditionalRead(e, val, x => x >= '0' && x <= '9'); // H

            ConditionalRead(e, val, x => x == ':');

            ConditionalRead(e, val, x => x >= '0' && x <= '9'); // m
            ConditionalRead(e, val, x => x >= '0' && x <= '9'); // m

            ConditionalRead(e, val, x => x == ':');

            ConditionalRead(e, val, x => x >= '0' && x <= '9'); // s
            ConditionalRead(e, val, x => x >= '0' && x <= '9'); // s

            ConditionalRead(e, val, x => x == '.');

            ConditionalRead(e, val, x => x >= '0' && x <= '9'); // i
            ConditionalRead(e, val, x => x >= '0' && x <= '9'); // i
            ConditionalRead(e, val, x => x >= '0' && x <= '9'); // i
            ConditionalRead(e, val, x => x >= '0' && x <= '9'); // i
            ConditionalRead(e, val, x => x >= '0' && x <= '9'); // i
            ConditionalRead(e, val, x => x >= '0' && x <= '9'); // i
        }

        private static bool ConditionalRead(CharEnumerator e, StringBuilder val, Func<char, bool> cond, bool throwCond = true)
        {
            e.MoveNext();
            if (!cond(e.Current))
            {
                if (throwCond)
                {
                    val.Append(e.Current);
                    Throw(val);
                }

                return false;
            }

            val.Append(e.Current);
            return true;
        }

        private static string ReadWord(CharEnumerator charEnumerator, string word)
        {
            var we = word.GetEnumerator();
            while (we.MoveNext())
            {
                if (charEnumerator.Current != we.Current || !charEnumerator.MoveNext())
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