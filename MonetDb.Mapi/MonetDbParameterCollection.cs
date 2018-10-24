namespace MonetDb.Mapi
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Linq;

    internal class MonetDbParameterCollection : DbParameterCollection
    {
        private readonly List<MonetDbParameter> list = new List<MonetDbParameter>();
        private object syncRoot = new object();

        public override int Count => this.list.Count;

        public override object SyncRoot => this.syncRoot;

        public override int Add(object value)
        {
            return this.Add((MonetDbParameter)value);
        }

        public override void AddRange(Array values)
        {
            this.AddRange(values);
        }

        public override void Clear()
        {
            this.list.Clear();
        }

        public override bool Contains(object value)
        {
            return this.list.Any(param => param.ParameterName.Equals(value));
        }

        public override bool Contains(string value)
        {
            return this.list.Any(param => param.ParameterName.Equals(value));
        }

        public override void CopyTo(Array array, int index)
        {
            this.list.CopyTo((MonetDbParameter[])array, index);
        }

        public override IEnumerator GetEnumerator()
        {
            return this.list.GetEnumerator();
        }

        protected override DbParameter GetParameter(int index)
        {
            return this.list.ElementAt(index);
        }

        protected override DbParameter GetParameter(string parameterName)
        {
            return this.list.First(param => param.ParameterName.Equals(parameterName));
        }

        public override int IndexOf(object value)
        {
            return this.list.FindIndex(param => param.ParameterName.Equals(value));
        }

        public override int IndexOf(string parameterName)
        {
            return this.list.FindIndex(param => param.ParameterName.Equals(parameterName));
        }

        public override void Insert(int index, object value)
        {
            this.list.Insert(index, (MonetDbParameter)value);
        }

        public override void Remove(object value)
        {
            this.list.Remove((MonetDbParameter)value);
        }

        public override void RemoveAt(int index)
        {
            this.list.RemoveAt(index);
        }

        public override void RemoveAt(string parameterName)
        {
            var index = this.IndexOf(parameterName);
            if (index > -1)
            {
                this.RemoveAt(index);
            }
        }

        protected override void SetParameter(int index, DbParameter value)
        {
            this.RemoveAt(index);
            this.Insert(index, (MonetDbParameter)value);
        }

        protected override void SetParameter(string parameterName, DbParameter value)
        {
            this.Remove(parameterName);
            this.Add((MonetDbParameter)value);
        }
    }
}