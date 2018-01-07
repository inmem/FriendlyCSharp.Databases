// Licensed to the KUBDAT® under one or more agreements.
// The KUBDAT® licenses this file to you under the Apache-2.0 license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using TImsId = System.UInt32;

namespace FriendlyCSharp.Databases
{
  public class FcsInmemStream<T> : Stream, IDisposable, IEnumerable, IEnumerable<T> 
                                   where T : struct, ICloneable
  {
    public static readonly TImsId NoDelete = default(TImsId);
    public static readonly TImsId ErrorId = default(TImsId);
    protected short _delta64KPerPageT;
    protected int _recPageCols = 0x00010000; // 65536
    protected int _recPageRows;
    protected int _recPageRowsCount;
    protected RecPage[] _recPage;
    protected bool _bRecDelete;
    protected TImsId _capacity;
    protected TImsId _length;
    protected bool _isOpen;
    private TImsId _position;
    protected readonly object _lockAppend = new object();
    public readonly TImsId OFFSET_ERROR = TImsId.MaxValue;
    //
    private TImsId _offsetPosition;
    public TImsId OFFSET_POSITION { get => _offsetPosition; }
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
          _offsetPosition = OFFSET_ERROR;
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
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////         Transaction          //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public struct Transaction : ICloneable
    {
      public TImsId keyPos;
      public UInt16 keyCount;
      public bool   valueOK;
      public T[]    valueARec;
      /////////////////////////////////////////////
      public Transaction(TImsId pos, UInt16 count)
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
    /////////////////////////////////////////////           RecPage            //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected class RecPage : IDisposable
    {
      public int iUpdate;
      public DateTime timeRead;
      public DateTime timeWrite;
      public T[] aRec;
      public TImsId[] aDel;
      private bool disposed = false;
      /////////////////////////////////////////////
      public RecPage(int iRecCount, bool bRecDelete)
      {
        iUpdate = 0;
        timeRead = DateTime.MinValue;
        timeWrite = DateTime.MinValue;
        aRec = new T[iRecCount];
        if (bRecDelete)
          aDel = new TImsId[iRecCount];
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
    public FcsInmemStream(short delta64KPerPageT) : this(delta64KPerPageT, false)
    {
    }
    /////////////////////////////////////////////
    public FcsInmemStream(short delta64KPerPageT, bool bRecDelete)
    {
      if (typeof(T) == typeof(DateTime))
        throw new ArgumentOutOfRangeException("Type DateTime can not be directly insert it into the structure.");
      _delta64KPerPageT = delta64KPerPageT;
      // min.T 4096, max.T 1048576 (rec per page), maxRows = 32768, min.T 128M, max.T 32G
      // deltaPage == -4 up 2^12T, == -3 up 2^13T, == -2 up 2^14T, == -1 up 2^15T (rec per page)
      // deltaPage ==  0 up 2^16T (rec per page)
      // deltaPage ==  1 up 2^17T, ==  2 up 2^18T, ==  3 up 2^19T, ==  4 up 2^20T (rec per page)
      if ((delta64KPerPageT < -4) || (delta64KPerPageT > 4))
        throw new ArgumentOutOfRangeException(nameof(delta64KPerPageT));
      if (delta64KPerPageT < 0)
        _recPageCols >>= Math.Abs(delta64KPerPageT);
      else if (delta64KPerPageT > 0)
        _recPageCols <<= Math.Abs(delta64KPerPageT);
      _recPageRows = 0x0100;          // 256 BufferPage
      _recPage = new RecPage[_recPageRows];
      _bRecDelete = bRecDelete;
      _recPageRowsCount = 0;
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
    public virtual TImsId Append(T value)
    {
      T[] aValue = new T[1];
      aValue[0] = value;
      return Append(aValue, null, 0, (UInt16)aValue.Length);
    }
    /////////////////////////////////////////////
    public virtual TImsId Append(T[] aValue)
    {
      if (_bFuncException)
      {
        if (aValue == null)
          throw new ArgumentOutOfRangeException(nameof(aValue));
        if ((aValue.Length <= 0) || (aValue.Length > UInt16.MaxValue))
          throw new ArgumentOutOfRangeException(nameof(aValue.Length));
      }

      return Append(aValue, null, 0, (UInt16)aValue.Length);
    }
    /////////////////////////////////////////////
    public virtual TImsId Append(T[] aValue, int index, UInt16 count)
    {
      if (_bFuncException)
      {
        if (aValue == null)
          throw new ArgumentOutOfRangeException(nameof(aValue));
        if ((aValue.Length <= 0) || (aValue.Length > UInt16.MaxValue))
          throw new ArgumentOutOfRangeException(nameof(aValue.Length));
      }

      return Append(aValue, null, index, count);
    }
    /////////////////////////////////////////////
    protected virtual TImsId Append(T[] aValue, TImsId[] aDel, int index, UInt16 count)
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

      TImsId lengthTemp = OFFSET_ERROR;
      lock (_lockAppend)
      {
        if (Length < 0)
          throw new ArgumentOutOfRangeException(nameof(Length));
        lengthTemp = (TImsId)Length;
        long lNewLen = lengthTemp + count;
        while (lNewLen > Capacity)
        {
          if (_recPageRowsCount >= _recPageRows)
          {
            _recPageRows <<= 1;
            if (_recPageRows >= 0x00008000) // 32768
              throw new OutOfMemoryException();
            Array.Resize<RecPage>(ref _recPage, _recPageRows);
          }
          _recPage[_recPageRowsCount++] = new RecPage(_recPageCols, _bRecDelete);
          long lCapacity = _recPageCols * _recPageRowsCount;
          if (lCapacity < 0)
            throw new ArgumentOutOfRangeException();
          else
            _capacity = (TImsId)lCapacity;
        }
        int iWrite = _BlockCopyInternal(false, lengthTemp, aValue, aDel, index, count);
        if (iWrite == count)
        {
          _length += (TImsId)iWrite;
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
    public virtual TImsId Capacity { get => _capacity; }
    /////////////////////////////////////////////
    public override void Close()
    {
      _isOpen = false;
      Dispose();
      base.Close();
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////            Delete            //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public virtual bool Delete(TImsId offset)
    {
      return Delete(offset, ErrorId);
    }
    /////////////////////////////////////////////
    public virtual bool Delete(TImsId offset, TImsId offsetNew)
    {
      RecPage[] buffPageLocal = _recPage;
      if ((offset < 0) || (offset >= Length) || (!_RowColsOffset(buffPageLocal, offset, out int iRow, out int iCols)))
        throw new ArgumentOutOfRangeException(nameof(offset));
      if (!_bRecDelete)
      {
        lock (_lockAppend)
        {
          if (!_bRecDelete)
          {
            _bRecDelete = true;
            for (int idx = 0; idx < _recPageRowsCount; idx++)
              buffPageLocal[idx].aDel = new TImsId[_recPageCols];
          }
        }
      }
      if (buffPageLocal[iRow].aDel == null)
        throw new ArgumentNullException("", "_bufferPage[iRow].aDel");

      buffPageLocal[iRow].aDel[iCols] = offsetNew;
      return true;
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////        Dispose, Flush        //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public new void Dispose()
    {
      Dispose(true);
      //GC.SuppressFinalize(this);
      base.Dispose(true);
    }
    /////////////////////////////////////////////
    protected override void Dispose(bool disposing)
    {
      if (!_disposed)
      {
        _disposed = true;
        _isOpen = false;
        if (_recPage != null)
        {
          for (int ii = 0; ii < _recPageRowsCount; ii++)
          {
            _recPage[ii].Dispose();
            _recPage[ii] = null;
          }
        }
        _recPage = null;
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
    public virtual long IsDelete(TImsId offset)
    {
      if (!_bRecDelete)
        return 0;

      RecPage[] buffPageLocal = _recPage;
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
    public static FcsInmemStream<T> Open(short delta64KPerPageT)
    {
      return new FcsInmemStream<T>(delta64KPerPageT, false);
    }
    /////////////////////////////////////////////
    public static FcsInmemStream<T> Open(short delta64KPerPageT, bool bRecDelete)
    {
      return new FcsInmemStream<T>(delta64KPerPageT, bRecDelete);
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
            _position = (TImsId)value;
          else
            _position = (TImsId)lengthTemp;
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
      if (!_bFuncPosition)
        throw new NotImplementedException("Cannot Read() to this FcsInmemStream<T>.");
      if (!CanRead)
        return 0;

      int sizeT = Marshal.SizeOf<T>();
      int countT = Math.Abs(count) / sizeT;
      if ((count > 0) && (countT <= 0))
        countT = sizeT;
      countT &= 0xFFFF;
      if (_bFuncException)
      {
        if (buffer == null)
          throw new ArgumentOutOfRangeException(nameof(buffer));
        if ((offset < 0) || (offset >= buffer.Length))
          throw new ArgumentOutOfRangeException(nameof(offset));
        if ((count <= 0) || (count + offset > buffer.Length))
          throw new ArgumentOutOfRangeException(nameof(count));
      }

      long lengthTemp = Length;
      long position = Position;
      if (position == lengthTemp) // foreach must return 0
        return 0;
      if ((position < 0) || (position > lengthTemp))
        throw new ArgumentOutOfRangeException(nameof(Position));

      if (position + countT > lengthTemp)
        countT = (UInt16)(lengthTemp - position);
      T[] aValue = new T[countT];
      int iRead = _BlockCopyInternal(true, position, aValue, null, 0, countT);
      //
      int size = iRead * sizeT;
      IntPtr ptr = Marshal.AllocHGlobal(size);
      Marshal.StructureToPtr(aValue, ptr, false);
      Marshal.Copy(ptr, buffer, offset, size);
      Marshal.FreeHGlobal(ptr);
      //
      lock (_lockAppend)
        Position = position + iRead;
      return iRead * sizeT;
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
      aValue = new T[0];
      return 0;
    }
    /////////////////////////////////////////////
    public virtual int Read(TImsId offset, out T value, out TImsId DeleteRecId)
    {
      value = default(T);
      DeleteRecId = NoDelete;
      if ((FuncPosition) && (offset == OFFSET_POSITION))
        offset = (TImsId)Position;
      if ((!CanRead) || (offset < 0) || (offset >= Length))
        return 0;

      T[] aValue = new T[1];
      TImsId[] aDel = new TImsId[1];
      int iRead = _BlockCopyInternal(true, offset, aValue, aDel, 0, 1);
      if (FuncPosition)
      {
        lock (_lockAppend)
          Position = offset + iRead;
      }
      if (iRead != 1)
        return 0;
      value = aValue[0];
      DeleteRecId = aDel[0];
      return iRead;
    }
    /////////////////////////////////////////////
    public virtual int Read(TImsId offset, out T value)
    {
      value = default(T);
      if ((FuncPosition) && (offset == OFFSET_POSITION))
        offset = (TImsId)Position;
      if ((!CanRead) || (offset < 0) || (offset >= Length))
        return 0;

      T[] aValue = new T[1];
      int iRead = _BlockCopyInternal(true, offset, aValue, null, 0, 1);
      if (FuncPosition)
      {
        lock (_lockAppend)
          Position = offset + iRead;
      }
      if (iRead != 1)
        return 0;
      value = aValue[0];
      return iRead;
    }
    /////////////////////////////////////////////
    public virtual int Read(TImsId offset, out T[] aValue, int count)
    {
      if (_bFuncException)
      {
        if ((count <= 0) || (count > UInt16.MaxValue))
          throw new ArgumentOutOfRangeException(nameof(count));
      }
      aValue = new T[count];
      return Read(offset, aValue, null, 0, (UInt16)count);
    }
    /////////////////////////////////////////////
    public virtual int Read(TImsId offset, out T[] aValue, out TImsId[] aDel, int count)
    {
      if (_bFuncException)
      {
        if ((count <= 0) || (count > UInt16.MaxValue))
          throw new ArgumentOutOfRangeException(nameof(count));
      }
      aValue = new T[count];
      aDel = new TImsId[count];
      return Read(offset, aValue, aDel, 0, (UInt16)count);
    }
    /////////////////////////////////////////////
    public virtual int Read(TImsId offset, T[] aValue)
    {
      if (_bFuncException)
      {
        if (aValue == null)
          throw new ArgumentOutOfRangeException(nameof(aValue));
        if ((aValue.Length == 0) || (aValue.Length > UInt16.MaxValue))
          throw new ArgumentOutOfRangeException(nameof(aValue.Length));
      }
      return Read(offset, aValue, null, 0, (UInt16)aValue.Length);
    }
    /////////////////////////////////////////////
    public virtual int Read(TImsId offset, T[] aValue, int count)
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
      return Read(offset, aValue, null, 0, (UInt16)count);
    }
    /////////////////////////////////////////////
    public virtual int Read(TImsId offset, T[] aValue, TImsId[] aDel, int count)
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
      return Read(offset, aValue, aDel, 0, (UInt16)count);
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
          if (_BlockCopyInternal(true, aTrans[pr].keyPos, aTrans[pr].valueARec, null, 0, iCount) == iCount)
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
    public virtual int Read(TImsId offset, T[] aValue, TImsId[] aDel, int index, UInt16 count)
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
        offset = (TImsId)Position;
      if (offset == lengthTemp) // foreach must return 0
        return 0;
      if ((offset < 0) || (offset > lengthTemp))
        throw new ArgumentOutOfRangeException(nameof(offset));

      if (offset + count > lengthTemp)
        count = (UInt16)(lengthTemp - offset);
      int iRead = _BlockCopyInternal(true, offset, aValue, aDel, index, count);
      if (FuncPosition)
      {
        lock (_lockAppend)
          Position = offset + iRead;
      }
      return iRead;
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////          ReadNoLock             //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public virtual int ReadNoLock(TImsId offset, out T value)
    {
      value = default(T);
      if ((!CanRead) || (offset < 0) || (offset >= Length))
        return 0;

      T[] aValue = new T[1];
      int iRead = _BlockReadNoLockInternal(offset, aValue, null, 0, 1);
      if (iRead != 1)
        return 0;
      value = aValue[0];
      return iRead;
    }
    /////////////////////////////////////////////
    public virtual int ReadNoLock(TImsId offset, out T value, out TImsId DeleteRecId)
    {
      value = default(T);
      DeleteRecId = NoDelete;
      if ((!CanRead) || (offset < 0) || (offset >= Length))
        return 0;

      T[] aValue = new T[1];
      TImsId[] aDel = new TImsId[1];
      int iRead = _BlockReadNoLockInternal(offset, aValue, aDel, 0, 1);
      if (iRead != 1)
        return 0;
      value = aValue[0];
      DeleteRecId = aDel[0];
      return iRead;
    }
    /////////////////////////////////////////////
    public virtual int ReadNoLock(TImsId offset, out T[] aValue, UInt16 count)
    {
      if ((!CanRead) || (offset < 0) || (offset >= Length))
      {
        aValue = new T[0];
        return 0;
      }

      aValue = new T[count];
      int iRead = _BlockReadNoLockInternal(offset, aValue, null, 0, 1);
      if (iRead <= 0)
      {
        aValue = new T[0];
        return 0;
      }
      return iRead;
    }
    /////////////////////////////////////////////
    public virtual int ReadNoLock(TImsId offset, out T[] aValue, out TImsId[] aDel, UInt16 count)
    {
      if ((!CanRead) || (offset < 0) || (offset >= Length))
      {
        aValue = new T[0];
        aDel = new TImsId[0];
        return 0;
      }

      aValue = new T[count];
      aDel = new TImsId[count];
      int iRead = _BlockReadNoLockInternal(offset, aValue, aDel, 0, count);
      if (iRead <= 0)
      {
        aValue = new T[0];
        aDel = new TImsId[0];
        return 0;
      }
      return iRead;
    }
    /////////////////////////////////////////////
    public virtual int ReadNoLock(TImsId offset, T[] aValue, TImsId[] aDel, int index, UInt16 count)
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
        offset = (TImsId)Position;
      if (offset == lengthTemp) // foreach must return 0
        return 0;
      if ((offset < 0) || (offset > lengthTemp))
        throw new ArgumentOutOfRangeException(nameof(offset));

      if (offset + count > lengthTemp)
        count = (UInt16)(lengthTemp - offset);
      int iRead = _BlockReadNoLockInternal(offset, aValue, aDel, index, count);
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
      return Write(offset, aValue, null, 0, 1);
    }
    /////////////////////////////////////////////
    public virtual int Write(long offset, T value, TImsId DeleteRecId)
    {
      T[] aValue = new T[1];
      aValue[0] = value;
      TImsId[] aDel = new TImsId[1];
      aDel[0] = DeleteRecId;
      return Write(offset, aValue, aDel, 0, 1);
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
      return Write(offset, aValue, null, 0, (UInt16)aValue.Length);
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
      return Write(offset, aValue, null, 0, (UInt16)count);
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
          if (_BlockCopyInternal(false, aTrans[pr].keyPos, aTrans[pr].valueARec, null, 0, iCount) == iCount)
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
    public virtual int Write(long offset, T[] aValue, TImsId[] aDel, int index, UInt16 count)
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

      int iWrite = _BlockCopyInternal(false, offset, aValue, aDel, index, count);
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

      RecPage[] buffPageLocal = _recPage;
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
    private int _BlockCopyInternal(bool bReadArrayBuff, long srcOffset, T[] aRW, TImsId[] aDelRW, int aRWOffset, int count)
    {
      RecPage[] buffPageLocal = _recPage;
      if (!_RowColsOffset(buffPageLocal, srcOffset, out int iRow, out int iCols))
        throw new ArgumentOutOfRangeException(nameof(srcOffset));

      int iResult = 0;
      int iCount = _recPageCols - iCols;
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
            if ((aDelRW != null) && (buffPageLocal[iRow].aDel != null))
              Array.Copy(buffPageLocal[iRow].aDel, iCols, aDelRW, aRWOffset, iCount);
            buffPageLocal[iRow].timeRead = DateTime.Now;
          }
          else
          {
            Array.Copy(aRW, aRWOffset, buffPageLocal[iRow].aRec, iCols, iCount);
            if ((aDelRW != null) && (buffPageLocal[iRow].aDel != null))
              Array.Copy(aDelRW, aRWOffset, buffPageLocal[iRow].aDel, iCols, iCount);
            buffPageLocal[iRow].timeWrite = DateTime.Now;
            buffPageLocal[iRow].iUpdate++;
          }
        }

        iResult += iCount;
        count -= iCount;
        aRWOffset += iCount;
        // nastavi iCount, iCols, iRows pro pripadne opakovani
        if (count > _recPageCols)
          iCount = _recPageCols;
        else
          iCount = count;
        iCols = 0;
        iRow++;
      }
      return iResult;
    }
    /////////////////////////////////////////////
    private int _BlockReadNoLockInternal(long srcOffset, T[] aRW, TImsId[] aDelRW, int aRWOffset, int count)
    {
      RecPage[] buffPageLocal = _recPage;
      if (!_RowColsOffset(buffPageLocal, srcOffset, out int iRow, out int iCols))
        throw new ArgumentOutOfRangeException(nameof(srcOffset));

      int iResult = 0;
      int iCount = _recPageCols - iCols;
      if (iCount > count)
        iCount = count;

      while (iCount > 0)
      {
        if (buffPageLocal[iRow].aRec == null)
          throw new ArgumentNullException("", "_bufferPage[iRow].aRec[]");

        Array.Copy(buffPageLocal[iRow].aRec, iCols, aRW, aRWOffset, iCount);
        if ((aDelRW != null) && (buffPageLocal[iRow].aDel != null))
          Array.Copy(buffPageLocal[iRow].aDel, iCols, aDelRW, aRWOffset, iCount);
        buffPageLocal[iRow].timeRead = DateTime.Now;

        iResult += iCount;
        count -= iCount;
        aRWOffset += iCount;
        // nastavi iCount, iCols, iRows pro pripadne opakovani
        if (count > _recPageCols)
          iCount = _recPageCols;
        else
          iCount = count;
        iCols = 0;
        iRow++;
      }
      return iResult;
    }
    /////////////////////////////////////////////
    private bool _RowColsOffset(RecPage[] buffPageLocal, long offset, out int iRow, out int iCols)
    {
      iRow = (int)(offset / _recPageCols);
      iCols = (int)(offset % _recPageCols);
      if ((iRow >= _recPageRowsCount) || (buffPageLocal == null))
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
    public FcsInmemStream<T>.ImsEnumerator GetEnumeratorEx(TImsId posLo)
    {
      return new ImsEnumerator(this, posLo, -1);
    }
    /////////////////////////////////////////////
    public FcsInmemStream<T>.ImsEnumerator GetEnumeratorEx(TImsId posLo, int countMax)
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
      private TImsId _posLo;
      private int _countMax;
      private FcsInmemStream<T> _inmem;
      // locals
      private int _count;
      private TImsId _pos;
      private int _idx;
      private int _len;
      private UInt16 _cacheLen;
      private T[] _aT;
      /////////////////////////////////////////////
      public ImsEnumerator(FcsInmemStream<T> inmem, TImsId posLo, int countMax)
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
          iRead = _inmem.Read(_pos, _aT, null, 0, _cacheLen);
          _pos += (TImsId)iRead;
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