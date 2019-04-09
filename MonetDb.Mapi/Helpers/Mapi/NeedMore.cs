namespace MonetDb.Mapi.Helpers.Mapi
{
    public class NeedMore
    {
        private bool needMore;

        public static implicit operator bool(NeedMore needMore)
        {
            if (needMore.needMore)
            {
                needMore.needMore = false;
                return true;
            }

            return false;
        }

        public static implicit operator NeedMore(bool needMore)
        {
            return new NeedMore { needMore = needMore };
        }
    }
}