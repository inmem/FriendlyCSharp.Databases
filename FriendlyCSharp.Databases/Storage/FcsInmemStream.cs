// Licensed to the KUBDAT® under one or more agreements.
// The KUBDAT® licenses this file to you under the Apache-2.0 license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;

namespace FriendlyCSharp.Databases
{
  public partial class FcsInmemStream<T> : Stream, IDisposable, IEnumerable, IEnumerable<T> 
                                           where T : struct//, ICloneable
  {
    protected int _maxBlockByte = 1 << 20;
    protected int _bufferCols;
    protected int _bufferRows;
    protected int _bufferRowsCount;
    protected BufferPage[] _bufferPage;
    protected bool _bRecDelete;
    protected long _capacity;
    protected long _length;
    protected bool _isOpen;
    private long _position;
    protected readonly object _lockAppend = new object();
    public const long OFFSET_ERROR = long.MinValue;
    //
    private long _offsetPosition;
    public long OFFSET_POSITION { get => _offsetPosition; }
    //
    private bool _bFuncPosition;
    public bool FuncPosition
    {
      get
      {
        return _bFuncPosition;
      }
      set
      {
        if (_bFuncPosition)
          _offsetPosition = -1;
        else
          _offsetPosition = 0;
        _bFuncPosition = value;
      }
    }
    //
    private bool _bFuncException;
    public bool FuncException { get => _bFuncException; set => _bFuncException = value; }
    //
    private bool _disposed = false;
    private readonly int _sizeT = 1;
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////         Transaction          //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public struct Transaction //: ICloneable
    {
      public long   keyPos;
      public UInt16 keyCount;
      public bool   valueOK;
      public T[]    valueARec;
      /////////////////////////////////////////////
      public Transaction(long pos, UInt16 count)
      {
        keyPos = pos;
        keyCount = count;
        valueOK = false;
        valueARec = null;
      }
      /////////////////////////////////////////////
      public Transaction(Transaction Trans)
      {
        keyPos = Trans.keyPos;
        keyCount = Trans.keyCount;
        valueOK = false;
        valueARec = null;
      }
      /////////////////////////////////////////////
      public static Transaction[] Copy(Transaction[] aTransSrc)
      {
        if (aTransSrc == null)
          return null;
        Transaction[] aTransDesc = new Transaction[aTransSrc.Length];
        for (int uu = 0; uu < aTransSrc.Length; uu++)
          aTransDesc[uu] = new Transaction(aTransSrc[uu]);
        return aTransDesc; 
      }
      /////////////////////////////////////////////
      public object Clone()
      {
        Transaction Trans = (Transaction)MemberwiseClone();
        Trans.valueARec = (T[])valueARec.Clone();
        return Trans;
      }
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////          BufferPage          //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected class BufferPage : IDisposable
    {
      public int iUpdate;
      public DateTime timeRead;
      public DateTime timeWrite;
      public T[] aRec;
      public long[] aDel;
      private bool disposed = false;
      /////////////////////////////////////////////
      public BufferPage(int iValue, bool bRecDelete)
      {
        iUpdate = 0;
        timeRead = DateTime.MinValue;
        timeWrite = DateTime.MinValue;
        aRec = new T[iValue];
        if (bRecDelete)
          aDel = new long[iValue];
        else
          aDel = null;
      }
      /////////////////////////////////////////////
      protected virtual void Dispose(bool disposing)
      {
        if (!disposed)
        {
          disposed = true;
          // TODO: Nastavte velká pole na hodnotu null.
          aRec = null;
          aDel = null;
          // Call base class implementation.
          //base.Dispose(disposing);
        }
      }
      /////////////////////////////////////////////
      public void Dispose()
      {
        Dispose(true);
        // TODO: Zrušte komentář následujícího řádku, pokud se přepisuje finalizační metoda.
        //GC.SuppressFinalize(this);
      }
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////        FcsInmemStream        //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public FcsInmemStream() : this(0, false)
    {
    }
    /////////////////////////////////////////////
    public FcsInmemStream(short deltaPage) : this(deltaPage, false)
    {
    }
    /////////////////////////////////////////////
    public FcsInmemStream(short deltaPage, bool bRecDelete)
    {
      if (typeof(T) == typeof(DateTime))
        throw new ArgumentOutOfRangeException("Type DateTime can not be directly insert it into the structure.");
      _sizeT = Marshal.SizeOf(default(T));
      // deltaPage == -4 up 8GB == -3 up 16GB == -2 up 32GB, == -1 up 64GB, == 0 up 128GB 
      // deltaPage == 1 up 256 GB, == 2 up 512GB, == 3 up 1024GB, == 4 up 2048GB
      if ((deltaPage < -4) || (deltaPage > 4))
        throw new ArgumentOutOfRangeException(nameof(deltaPage));
      if (deltaPage < 0)
        _maxBlockByte >>= Math.Abs(deltaPage);
      else if (deltaPage > 0)
        _maxBlockByte <<= Math.Abs(deltaPage);
      _bufferCols = (_maxBlockByte / _sizeT);
      if (_bufferCols > 0x0400)      // 1024
        _bufferCols &= 0x7FFFFD00;   // nasobky 512
      else if (_bufferCols > 0x0100) // 256
        _bufferCols &= 0x7FFFFF80;   // nasobky 128
      else if (_bufferCols > 0x0010) // 16
        _bufferCols &= 0x7FFFFFF0;   // nasobky 16
      _bufferRows = 0x0100;          // 256 BufferPage
      _bufferPage = new BufferPage[_bufferRows];
      _bRecDelete = bRecDelete;
      _bufferRowsCount = 0;
      _capacity = 0;
      _length = 0;
      _position = 0;
      FuncPosition = false;
      FuncException = true;
      _isOpen = true;
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////            Append            //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public virtual long Append(T value)
    {
      T[] aValue = new T[1];
      aValue[0] = value;
      return Append(aValue);
    }
    /////////////////////////////////////////////
    public virtual long Append(T[] aValue)
    {
      if (_bFuncException)
      {
        if (aValue == null)
          throw new ArgumentOutOfRangeException(nameof(aValue));
        if ((aValue.Length <= 0) || (aValue.Length > UInt16.MaxValue))
          throw new ArgumentOutOfRangeException(nameof(aValue.Length));
      }

      return Append(aValue, 0, (UInt16)aValue.Length);
    }
    /////////////////////////////////////////////
    public virtual long Append(T[] aValue, int index, UInt16 count)
    {
      if (!CanWrite)
        return OFFSET_ERROR;

      if (_bFuncException)
      {
        if (aValue == null)
          throw new ArgumentOutOfRangeException(nameof(aValue));
        if ((index < 0) || (index >= aValue.Length))
          throw new ArgumentOutOfRangeException(nameof(index));
        if ((count == 0) || (count + index > aValue.Length))
          throw new ArgumentOutOfRangeException(nameof(count));
      }

      long lengthTemp = OFFSET_ERROR;
      lock (_lockAppend)
      {
        lengthTemp = Length;
        if (lengthTemp < 0)
          throw new ArgumentOutOfRangeException(nameof(Length));
        while (lengthTemp + count >= Capacity)
        {
          if (_bufferRowsCount >= _bufferRows) 
          {
            _bufferRows <<= 1;
            if (_bufferRows >= 0x00020000) // 131072
              throw new OutOfMemoryException();
            Array.Resize<BufferPage>(ref _bufferPage, _bufferRows);
          }
          _bufferPage[_bufferRowsCount++] = new BufferPage(_bufferCols, _bRecDelete);
          _capacity = _bufferCols * _bufferRowsCount;
        }
        int iWrite = _BlockCopyInternal(false, lengthTemp, aValue, index, count);
        if (iWrite == count)
        {
          _length += iWrite;
          if (FuncPosition)
            Position = Length;
        }
        else
          lengthTemp = OFFSET_ERROR;
      } // !lock
      return lengthTemp;
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////      CanRead, CanSeek, CanWrite       //////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public override bool CanRead
    {
      get { return _isOpen; }
    }
    /////////////////////////////////////////////
    public override bool CanSeek
    {
      get { return _isOpen; }
    }
    /////////////////////////////////////////////
    public override bool CanWrite
    {
      get { return _isOpen; }
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////       Capacity, Close        //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public virtual long Capacity { get => _capacity; }
    /////////////////////////////////////////////
    public void Close()
    {
      _isOpen = false;
      Dispose();
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////            Delete            //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public virtual void Delete(long offset)
    {
      Delete(offset, -1);
    }
    /////////////////////////////////////////////
    public virtual void Delete(long offset, long offsetNew)
    {
      BufferPage[] buffPageLocal = _bufferPage;
      if ((offset < 0) || (offset >= Length) || (!_RowColsOffset(buffPageLocal, offset, out int iRow, out int iCols)))
        throw new ArgumentOutOfRangeException(nameof(offset));
      if (!_bRecDelete)
      {
        lock (_lockAppend)
        {
          if (!_bRecDelete)
          {
            _bRecDelete = true;
            for (int idx = 0; idx < _bufferRows; idx++)
              buffPageLocal[idx].aDel = new long[_bufferCols];
          }
        }
      }
      if (buffPageLocal[iRow].aDel == null)
        throw new ArgumentNullException("", "_bufferPage[iRow].aDel");

      buffPageLocal[iRow].aDel[iCols] = offsetNew;
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////        Dispose, Flush        //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public new void Dispose()
    {
      Dispose(true);
      //GC.SuppressFinalize(this);
    }
    /////////////////////////////////////////////
    protected override void Dispose(bool disposing)
    {
      if (_disposed)
      {
        _disposed = true;
        _isOpen = false;
        _bufferPage = null;
        // Call base class implementation.
        base.Dispose(disposing);
      }
    }
    /////////////////////////////////////////////
    public override void Flush()
    {
      throw new NotImplementedException("Cannot Flush() to this FcsInmemStream<T>.");
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////           IsDelete           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public virtual long IsDelete(long offset)
    {
      if (!_bRecDelete)
        return 0;

      BufferPage[] buffPageLocal = _bufferPage;
      if ((offset < 0) || (offset >= Length) || (!_RowColsOffset(buffPageLocal, offset, out int iRow, out int iCols)))
        throw new ArgumentOutOfRangeException(nameof(offset));
      if (buffPageLocal[iRow].aDel == null)
        throw new ArgumentNullException("", "_bufferPage[iRow].aDel");
      return buffPageLocal[iRow].aDel[iCols];
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////    Length, Open, Position    //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public override long Length { get => _length; }
    /////////////////////////////////////////////
    public static FcsInmemStream<T> Open()
    {
      return new FcsInmemStream<T>(0, false);
    }
    /////////////////////////////////////////////
    public static FcsInmemStream<T> Open(short deltaPage)
    {
      return new FcsInmemStream<T>(deltaPage, false);
    }
    /////////////////////////////////////////////
    public static FcsInmemStream<T> Open(short deltaPage, bool bRecDelete)
    {
      return new FcsInmemStream<T>(deltaPage, bRecDelete);
    }
    /////////////////////////////////////////////
    public override long Position
    {
      get
      {
        if (FuncPosition)
          return _position;
        throw new NotImplementedException("Cannot Position() to this FcsInmemStream<T>.");
      }
      set
      {
        if (FuncPosition)
        {
          long lengthTemp = Length;
          if ((value >= 0) && (value <= lengthTemp))
            _position = value;
          else
            _position = lengthTemp;
        }
        else
          throw new NotImplementedException("Cannot Position() to this FcsInmemStream<T>.");
      }
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////             Read             //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public override int Read(byte[] buffer, int offset, int count)
    {
      throw new NotImplementedException("Cannot Read() to this FcsInmemStream<T>.");
    }
    /////////////////////////////////////////////
    public virtual int Read(out T value)
    {
      if (FuncPosition)
        return Read(OFFSET_POSITION, out value);
      value = default(T);
      return 0;
    }
    /////////////////////////////////////////////
    public virtual int Read(out T[] aValue, int count)
    {
      if (FuncPosition)
        return Read(OFFSET_POSITION, out aValue, count);
      aValue = new T[1];
      aValue[0] = default(T);
      return 0;
    }
    /////////////////////////////////////////////
    public virtual int Read(long offset, out T value)
    {
      value = default(T);
      if (!CanRead)
        return 0;

      if ((FuncPosition) && (offset == OFFSET_POSITION))
        offset = Position;
      if ((offset < 0) || (offset >= Length))
        return 0;

      T[] aValue = new T[1];
      int iRead = _BlockCopyInternal(true, offset, aValue, 0, 1);
      if (FuncPosition)
      {
        lock (_lockAppend)
          Position = offset + iRead;
      }
      if (iRead == 1)
        value = aValue[0];
      else
        iRead = 0;
      return iRead;
    }
    /////////////////////////////////////////////
    public virtual int Read(long offset, out T[] aValue, int count)
    {
      if (_bFuncException)
      {
        if ((count <= 0) || (count > UInt16.MaxValue))
          throw new ArgumentOutOfRangeException(nameof(count));
      }
      aValue = new T[count];
      return Read(offset, aValue, 0, (UInt16)count);
    }
    /////////////////////////////////////////////
    public virtual int Read(long offset, T[] aValue)
    {
      if (_bFuncException)
      {
        if (aValue == null)
          throw new ArgumentOutOfRangeException(nameof(aValue));
        if ((aValue.Length == 0) || (aValue.Length > UInt16.MaxValue))
          throw new ArgumentOutOfRangeException(nameof(aValue.Length));
      }
      return Read(offset, aValue, 0, (UInt16)aValue.Length);
    }
    /////////////////////////////////////////////
    public virtual int Read(long offset, T[] aValue, int count)
    {
      if (_bFuncException)
      {
        if (aValue == null)
          throw new ArgumentOutOfRangeException(nameof(aValue));
        if ((aValue.Length == 0) || (aValue.Length > UInt16.MaxValue))
          throw new ArgumentOutOfRangeException(nameof(aValue.Length));
        if ((count <= 0) || (count > UInt16.MaxValue))
          throw new ArgumentOutOfRangeException(nameof(count));
      }
      return Read(offset, aValue, 0, (UInt16)count);
    }
    /////////////////////////////////////////////
    public virtual int Read(Transaction[] aTrans, int index, UInt16 count)
    {
      if (!CanRead)
        return 0;

      if (_bFuncException)
      {
        if (aTrans == null)
          throw new ArgumentOutOfRangeException(nameof(aTrans));
        if ((index < 0) || (index >= aTrans.Length))
          throw new ArgumentOutOfRangeException(nameof(index));
        if ((count == 0) || (count + index > aTrans.Length))
          throw new ArgumentOutOfRangeException(nameof(count));
      }

      int iRead = 0;
      for (int pr = index; pr < index + count; pr++)
      {
        UInt16 iCount = aTrans[pr].keyCount;
        if ( (iCount > 0) && (iCount <= UInt16.MaxValue) && 
                             (aTrans[pr].keyPos >= 0) && (aTrans[pr].keyPos + iCount <= Length) )
        {
          aTrans[pr].valueARec = new T[iCount];
          if (_BlockCopyInternal(true, aTrans[pr].keyPos, aTrans[pr].valueARec, 0, iCount) == iCount)
          {
            aTrans[pr].valueOK = true;
            iRead++;
          }
          else
          {
            aTrans[pr].valueARec = null;
            aTrans[pr].valueOK = false;
          }
        }
        else
        {
          aTrans[pr].valueARec = null;
          aTrans[pr].valueOK = false;
        }
      }
      return iRead;
    }
    /////////////////////////////////////////////
    public virtual int Read(long offset, T[] aValue, int index, UInt16 count)
    {
      if (!CanRead)
        return 0;

      if (_bFuncException)
      {
        if (aValue == null)
          throw new ArgumentOutOfRangeException(nameof(aValue));
        if ((index < 0) || (index >= aValue.Length))
          throw new ArgumentOutOfRangeException(nameof(index));
        if ((count == 0) || (count + index > aValue.Length))
          throw new ArgumentOutOfRangeException(nameof(count));
      }

      long lengthTemp = Length;
      if ((FuncPosition) && (offset == OFFSET_POSITION))
        offset = Position;
      if (offset == lengthTemp) // foreach must return 0
        return 0;
      if ((offset < 0) || (offset > lengthTemp))
        throw new ArgumentOutOfRangeException(nameof(offset));

      if (offset + count > lengthTemp)
        count = (UInt16)(lengthTemp - offset);
      int iRead = _BlockCopyInternal(true, offset, aValue, index, count);
      if (FuncPosition)
      {
        lock (_lockAppend)
          Position = offset + iRead;
      }
      return iRead;
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////        Seek, SetLength       //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public override long Seek(long deltaOffset, SeekOrigin origin)
    {
      if (!FuncPosition)
        throw new NotImplementedException("Cannot Seek() to this FcsInmemStream<T>.");

      if (!CanSeek)
        return OFFSET_ERROR;

      long tempPosition = OFFSET_ERROR;
      switch (origin)
      {
        case SeekOrigin.Begin:
          {
            if (deltaOffset < 0)
              return OFFSET_ERROR;
            tempPosition = unchecked(deltaOffset);
            break;
          }
        case SeekOrigin.Current:
          {
            tempPosition = unchecked(Position + deltaOffset);
            break;
          }
        case SeekOrigin.End:
          {
            tempPosition = unchecked(Length + deltaOffset);
            break;
          }
        default:
          throw new ArgumentException(nameof(origin));
      }
      if ((tempPosition < 0) || (tempPosition >= Length))
        return OFFSET_ERROR;
      lock (_lockAppend)
        Position = tempPosition;
      return Position;
    }
    /////////////////////////////////////////////
    public override void SetLength(long value)
    {
      throw new NotImplementedException("Cannot SetLength() to this FcsInmemStream<T>.");
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////            Write             //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public override void Write(byte[] buffer, int offset, int count)
    {
      throw new NotImplementedException("Cannot Write() to this FcsInmemStream<T>.");
    }
    /////////////////////////////////////////////
    public virtual int Write(T value)
    {
      if (FuncPosition)
        return Write(OFFSET_POSITION, value);
      return 0;
    }
    /////////////////////////////////////////////
    public virtual int Write(T[] aValue)
    {
      if (FuncPosition)
        return Write(OFFSET_POSITION, aValue);
      return 0;
    }
    /////////////////////////////////////////////
    public virtual int Write(long offset, T value)
    {
      T[] aValue = new T[1];
      aValue[0] = value;
      return Write(offset, aValue, 0, 1);
    }
    /////////////////////////////////////////////
    public virtual int Write(long offset, T[] aValue)
    {
      if (_bFuncException)
      {
        if (aValue == null)
          throw new ArgumentOutOfRangeException(nameof(aValue));
        if ((aValue.Length == 0) || (aValue.Length > UInt16.MaxValue))
          throw new ArgumentOutOfRangeException(nameof(aValue.Length));
      }
      return Write(offset, aValue, 0, (UInt16)aValue.Length);
    }
    /////////////////////////////////////////////
    public virtual int Write(long offset, T[] aValue, int count)
    {
      if (_bFuncException)
      {
        if (aValue == null)
          throw new ArgumentOutOfRangeException(nameof(aValue));
        if ((aValue.Length == 0) || (aValue.Length > UInt16.MaxValue))
          throw new ArgumentOutOfRangeException(nameof(aValue.Length));
        if ((count <= 0) || (count > UInt16.MaxValue))
          throw new ArgumentOutOfRangeException(nameof(count));
      }
      return Write(offset, aValue, 0, (UInt16)count);
    }
    /////////////////////////////////////////////
    public virtual int Write(Transaction[] aTrans, int index, UInt16 count)
    {
      if (!CanWrite)
        return 0;

      if (_bFuncException)
      {
        if (aTrans == null)
          throw new ArgumentOutOfRangeException(nameof(aTrans));
        if ((index < 0) || (index >= aTrans.Length))
          throw new ArgumentOutOfRangeException(nameof(index));
        if ((count == 0) || (count + index > aTrans.Length))
          throw new ArgumentOutOfRangeException(nameof(count));
      }

      int iWrite = 0;
      for (int pr = index; pr < index + count; pr++)
      {
        UInt16 iCount = aTrans[pr].keyCount;
        if ((iCount > 0) && (iCount <= UInt16.MaxValue) &&
                            (aTrans[pr].keyPos >= 0) && (aTrans[pr].keyPos + iCount <= Length))
        {
          aTrans[pr].valueARec = new T[iCount];
          if (_BlockCopyInternal(false, aTrans[pr].keyPos, aTrans[pr].valueARec, 0, iCount) == iCount)
          {
            aTrans[pr].valueOK = true;
            iWrite++;
          }
          else
          {
            aTrans[pr].valueARec = null;
            aTrans[pr].valueOK = false;
          }
        }
        else
        {
          aTrans[pr].valueARec = null;
          aTrans[pr].valueOK = false;
        }
      }
      return iWrite;
    }
    /////////////////////////////////////////////
    public virtual int Write(long offset, T[] aValue, int index, UInt16 count)
    {
      if (!CanWrite)
        return 0;

      if (_bFuncException)
      {
        if (aValue == null)
          throw new ArgumentOutOfRangeException(nameof(aValue));
        if ((index < 0) || (index >= aValue.Length))
          throw new ArgumentOutOfRangeException(nameof(index));
        if ((count == 0) || (count + index > aValue.Length))
          throw new ArgumentOutOfRangeException(nameof(count));
      }

      if ((FuncPosition) && (offset == OFFSET_POSITION))
        offset = Position;
      if ((offset < 0) || (offset + count > Length))
        throw new ArgumentOutOfRangeException(nameof(offset));

      int iWrite = _BlockCopyInternal(false, offset, aValue, index, count);
      if (FuncPosition)
      {
        lock (_lockAppend)
          Position = offset + iWrite;
      }
      return iWrite;
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////           Undelete           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public virtual bool Undelete(long offset)
    {
      if (!_bRecDelete)
        return false;

      BufferPage[] buffPageLocal = _bufferPage;
      if ((offset < 0) || (offset >= Length) || (!_RowColsOffset(buffPageLocal, offset, out int iRow, out int iCols)))
        throw new ArgumentOutOfRangeException(nameof(offset));
      if (buffPageLocal[iRow].aDel == null)
        throw new ArgumentNullException("", "_bufferPage[iRow].aDel");

      if (buffPageLocal[iRow].aDel[iCols] != 0)
      {
        buffPageLocal[iRow].aDel[iCols] = 0;
        return true;
      }
      return false;
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////            locals            //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    private int _BlockCopyInternal(bool bReadArrayBuff, long srcOffset, T[] aRW, int aRWOffset, int count)
    {
      BufferPage[] buffPageLocal = _bufferPage;
      if (!_RowColsOffset(buffPageLocal, srcOffset, out int iRow, out int iCols))
        throw new ArgumentOutOfRangeException(nameof(srcOffset));

      int iResult = 0;
      int iCount = _bufferCols - iCols;
      if (iCount > count)
        iCount = count;

      while (iCount > 0)
      {
        if (buffPageLocal[iRow].aRec == null)
          throw new ArgumentNullException("", "_bufferPage[iRow].aRec[]");

        lock (buffPageLocal[iRow])
        {
          if (bReadArrayBuff)
          {
            Array.Copy(buffPageLocal[iRow].aRec, iCols, aRW, aRWOffset, iCount);
            buffPageLocal[iRow].timeRead = DateTime.Now;
          }
          else
          {
            Array.Copy(aRW, aRWOffset, buffPageLocal[iRow].aRec, iCols, iCount);
            buffPageLocal[iRow].timeWrite = DateTime.Now;
            buffPageLocal[iRow].iUpdate++;
          }
        }

        iResult += iCount;
        count -= iCount;
        aRWOffset += iCount;
        // nastavi iCount, iCols, iRows pro pripadne opakovani
        if (count > _bufferCols)
          iCount = _bufferCols;
        else
          iCount = count;
        iCols = 0;
        iRow++;
      }
      return iResult;
    }
    /////////////////////////////////////////////
    private bool _RowColsOffset(BufferPage[] buffPageLocal, long offset, out int iRow, out int iCols)
    {
      iRow = (int)(offset / _bufferCols);
      iCols = (int)(offset % _bufferCols);
      if ((iRow >= _bufferRowsCount) || (buffPageLocal == null))
        return false;
      else
        return true;
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////         IEnumerator          //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    private static int _imsEnumeratorCacheLen = 32;
    public static int ImsEnumeratorCacheLen
    {
      get => _imsEnumeratorCacheLen;
      set
      {
        if (value < 16)
          _imsEnumeratorCacheLen = 16;
        else if (value > 1024)
          _imsEnumeratorCacheLen = 1024;
        else
          _imsEnumeratorCacheLen = value;
      }
    }
    /////////////////////////////////////////////
    public FcsInmemStream<T>.ImsEnumerator GetEnumeratorEx(long posLo)
    {
      return new ImsEnumerator(this, posLo, -1);
    }
    /////////////////////////////////////////////
    public FcsInmemStream<T>.ImsEnumerator GetEnumeratorEx(long posLo, int countMax)
    {
      return new ImsEnumerator(this, posLo, countMax);
    }
    /////////////////////////////////////////////
    IEnumerator IEnumerable.GetEnumerator()
    {
      // call the generic version of the method
      return GetEnumerator();
    }
    /////////////////////////////////////////////
    public IEnumerator<T> GetEnumerator()
    {
      return new ImsEnumerator(this, OFFSET_ERROR, -1);
    }
    /////////////////////////////////////////////
    public class ImsEnumerator : IEnumerator<T>
    {
      // constructor
      private long _posLo;
      private int _countMax;
      private FcsInmemStream<T> _inmem;
      // locals
      private int _count;
      private long _pos;
      private int _idx;
      private int _len;
      private UInt16 _cacheLen;
      private T[] _aT;
      /////////////////////////////////////////////
      public ImsEnumerator(FcsInmemStream<T> inmem, long posLo, int countMax)
      {
        _inmem = inmem ?? throw new NullReferenceException();
        _posLo = posLo;
        _countMax = countMax;
        Reset();
      }
      /////////////////////////////////////////////
      public bool MoveNext()
      {
        if (_count == 0)
          return false;
        else if (_count > 0)
          _count--;

        int iRead = 1;
        if ((_idx >= 0) && (_idx < _len))
          _idx++;
        else
        {
          iRead = _inmem.Read(_pos, _aT, 0, _cacheLen);
          _pos += iRead;
          _idx = 0;
          _len = iRead - 1;
        }
        return (iRead > 0);
      }
      /////////////////////////////////////////////
      public T Current => _aT[_idx];
      /////////////////////////////////////////////
      object IEnumerator.Current => _aT[_idx];
      /////////////////////////////////////////////
      public void Dispose()
      {
        _aT = null;
        _inmem = null;
      }
      /////////////////////////////////////////////
      public void Reset()
      {
        _count = _countMax;
        if (_posLo < 0)
          _pos = 0;
        else
          _pos = _posLo;
        if ( (_pos > _inmem.Length) || ((_count > 0) && (_pos + _count > _inmem.Length)) )
          throw new ArgumentOutOfRangeException(nameof(_posLo));
        _idx = -1;
        _len = 0;
        _cacheLen = (UInt16)FcsInmemStream<T>.ImsEnumeratorCacheLen;
        _aT = new T[_cacheLen];
      }
    }
  }
}