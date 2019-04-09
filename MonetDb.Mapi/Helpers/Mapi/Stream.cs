namespace MonetDb.Mapi.Helpers.Mapi
{
    using System;
    using System.IO;
    using MonetDb.Mapi.Enums;

    /// <summary>
    /// The MonetDB server has it's own protocol for streaming chunked input and output.
    /// This is known as the "block" stream.  
    /// 
    /// A byte stream to and from the MonetDB server consists of one or more "blocks".
    /// A block is a sequence of bytes, with the first two bytes indicating a 16-bit
    /// integer length followed by the length number of bytes of data.  This can go on
    /// for as many blocklength+block series are sent from the server, and the end of a 
    /// sequence is indicated by a block with the most significant big set to 1 (blockHeader[0] &amp; 0x1) == 1).
    /// 
    /// When reading from the stream we end the sequence with a \n.\n (the first \n is added if not sent
    /// by the server).  This makes this class trivial to wrap with a StreamReader and StreamWriter.
    /// 
    /// When writing to the server, we write the terminating block header
    /// when the Flush() function is called.  If that's not called, we write out
    /// blocks to the server as they're filled.
    /// </summary>
    internal class Stream : System.IO.Stream
    {
        private readonly System.IO.Stream monetStream;

        private readonly byte[] readBlock = new byte[short.MaxValue + 3];
        private readonly byte[] writeBlock = new byte[short.MaxValue];

        private int readPos, writePos, readLength, writeLength;
        private bool lastReadBlock;

        public Stream(System.IO.Stream monetStream)
        {
            this.monetStream = new BufferedStream(monetStream);
            this.lastReadBlock = false;
        }

        public NeedMore NeedMore { get; set; } = new NeedMore();

        public override bool CanRead => this.monetStream.CanRead;

        public override bool CanSeek => this.monetStream.CanSeek;

        public override bool CanWrite => this.monetStream.CanWrite;

        public override long Length => this.monetStream.Length;

        public override void Flush()
        {
            WriteNextBlock(true);
        }

        public override long Position
        {
            get { return monetStream.Position; }
            set { monetStream.Position = value; }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (offset + count > buffer.Length)
                throw new ArgumentOutOfRangeException("offset",
                    "offset + count cannot be greater than the size of the buffer");

            var available = readLength - readPos;
            var retval = 0;
            if (available == 0)
                available = ReadNextBlock();
            while (available > 0 && retval < count)
            {
                var length = count - retval > available
                    ? available
                    : count - retval;

                Array.Copy(readBlock, readPos, buffer, offset, length);

                retval += length;
                offset += length;
                readPos += length;
                available = readLength - readPos;

                if (!lastReadBlock && available == 0)
                    available = ReadNextBlock();
            }

            return retval;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new InvalidOperationException("Seeking not allowed on a network based stream");
        }

        public override void SetLength(long value)
        {
            throw new InvalidOperationException("SetLength is not valid on a network based stream");
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (offset + count > buffer.Length)
                throw new ArgumentOutOfRangeException("offset",
                    "offset + count cannot be greater than the buffer length");

            while (count > 0)
            {
                if (count < writeBlock.Length - writePos)
                {
                    //in this case we won't fill up the block buffer so we can just copy the bytes
                    //to the buffer.
                    Array.Copy(buffer, offset, writeBlock, writePos, count);
                    writeLength += count;
                    count = 0;
                }
                else
                {
                    //In this case we will fill up the block buffer, so we need to copy
                    //what we can to the block buffer, write it out, and write what's left
                    var tempCount = writeBlock.Length - writePos;
                    Array.Copy(buffer, offset, writeBlock, writePos, tempCount);
                    offset += tempCount;
                    count -= tempCount;
                    writeLength += tempCount;
                    WriteNextBlock(false);
                }
            }
        }

        /// <summary>
        /// Reads the next available block on the provided stream.  Returns the bytes available in the block buffer.
        /// </summary>
        private int ReadNextBlock()
        {
            var blockHeader = new byte[2];

            if (monetStream.Read(blockHeader, 0, blockHeader.Length) != 2)
                throw new MonetDbException(
                    new InvalidDataException("Invalid block header length"),
                    "Error reading data from MonetDB server");

            readLength = ((blockHeader[0] & 0xFF) >> 1 |
                            (blockHeader[1] & 0xFF) << 7);

            lastReadBlock = (blockHeader[0] & 0x1) == 1;

            var read = 0;

            while (read < readLength)
                read += monetStream.Read(readBlock, read, readLength - read);

            readPos = 0;

            if (!lastReadBlock)
                return readLength - readPos;

            if (readLength > 0 && readBlock[readLength - 1] != '\n')
                readBlock[readLength++] = (byte)'\n';

            readBlock[readLength++] = (byte)DbLineType.Prompt;
            readBlock[readLength++] = (byte)'\n';

            return readLength - readPos;
        }

        /// <summary>
        /// Writes the next block to the provided stream.
        /// </summary>
        /// <param name="last">If <c>true</c> then we should write out the block header to indicate that this is the end
        /// of the sequence.  If <c>false</c> the the server should expect more data.</param>
        private void WriteNextBlock(bool last)
        {
            var blockHeader = new byte[2];
            var blockSize = (short)writeLength;

            //if this is the last block then we set the most significant bit to 1
            //if this is not the last block then we set the most significant bit to 0
            if (last && !this.NeedMore)
            {
                blockHeader[0] = (byte)(blockSize << 1 & 0xFF | 1);
                blockHeader[1] = (byte)(blockSize >> 7);
            }
            else
            {
                blockHeader[0] = (byte)(blockSize << 1 & 0xFF);
                blockHeader[1] = (byte)(blockSize >> 7);
            }

            monetStream.Write(blockHeader, 0, blockHeader.Length);
            monetStream.Write(writeBlock, 0, writeLength);
            monetStream.Flush();

            writePos = 0;
            writeLength = 0;
        }
    }
}
