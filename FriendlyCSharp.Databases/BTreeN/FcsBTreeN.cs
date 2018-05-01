// Licensed to the KUBDAT® under one or more agreements.
// The KUBDAT® licenses this file to you under the Apache-2.0 license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections;
using System.Collections.Generic;

namespace FriendlyCSharp.Databases
{
  public partial class FcsBTreeN<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
                                                 where TKey : IComparable<TKey>
  {
    protected const int _btnDefaultBTreeN = 32;
    protected object _btnLockAdd;
    protected KeyValuePage _btnRoot = null;
    protected KeyValueFast[] _btnKVFast;
    protected object _objCompares = null;
    //////////////////////////
    private int    _btnBTreeN;
    protected int  BtnBTreeN { get => _btnBTreeN; }
    //////////////////////////
    private int    _btnVersionPage;
    protected int  BtnVersionPage { get => _btnVersionPage; }
    //////////////////////////
    private int    _btnVersion;
    protected int  BtnVersion { get => _btnVersion; }
    //////////////////////////
    private bool   _btnUpdatedRoot;
    protected bool BtnUpdatedRoot { get => _btnUpdatedRoot; }
    //////////////////////////
    private const int _btnMaxBTreeN = 512;
    private const uint _btnMaxIdxFast = 4096;
    //////////////////////////
    protected internal struct KeyValue
    {
      public TKey key;
      public TValue value;
    }
    //////////////////////////
    public struct KeyValueFast : IDisposable
    {
      internal int version;
      internal int fastMiddle;
      internal KeyValuePage fastPage;
      //////////////////////////
      public int Version { get => version; set => version = 0; } // value; }
      //////////////////////////
      public void Dispose(bool disposing)
      {
        fastPage = null;
      }
      //////////////////////////
      public void Dispose()
      {
        Dispose(true);
      }
    }
    //////////////////////////
    protected internal class KeyValuePage : IDisposable, ICloneable
    {
      public UInt32 flags;
      public bool bUpdatedValue;
      public int iDataCount;
      public KeyValue[] aData;
      public KeyValuePage[] kvPageNextRight;
      //////////////////////////
      public KeyValuePage(int iPageN, bool bPageNext)
      {
        bUpdatedValue = false;
        flags = 0;
        iDataCount = 0;
        aData = new KeyValue[(iPageN * 2) + 1];
        if (bPageNext)
          kvPageNextRight = new KeyValuePage[(iPageN * 2) + 1];
        else
          kvPageNextRight = null;
      }
      #region IDisposable
      private bool disposedValue = false;
      public void Dispose(bool disposing)
      {
        if (!disposedValue)
        {
          if (disposing)
          {
            if (kvPageNextRight != null)
            {
              for (int oo = 0; oo <= iDataCount; oo++)
              {
                if (kvPageNextRight[oo] != null)
                  kvPageNextRight[oo].Dispose(true);
              }
            }
            aData = null;
            kvPageNextRight = null;
          }
          disposedValue = true;
        }
      }
      void IDisposable.Dispose()
      {
        Dispose(true);
      }
      #endregion
      public object Clone()
      {
        KeyValuePage root = (KeyValuePage)this.MemberwiseClone();
        aData = (KeyValue[])aData.Clone();
        if (kvPageNextRight != null)
        {
          for (int ii = 0; ii <= root.iDataCount; ii++)
          {
            if (root.kvPageNextRight[ii] != null)
              root.kvPageNextRight[ii] = (KeyValuePage)root.kvPageNextRight[ii].Clone();
          }
        }
        return root;
      }
    }
    //////////////////////////
    public FcsBTreeN() : this(_btnDefaultBTreeN, 1, null)
    {
    }
    //////////////////////////
    protected FcsBTreeN(object objCmp) : this(_btnDefaultBTreeN, 1, objCmp)
    {
    }
    //////////////////////////
    public FcsBTreeN(int btnBTreeN) : this(btnBTreeN, 1, null)
    {
    }
    //////////////////////////
    protected FcsBTreeN(int btnBTreeN, uint allocIdxFast, object objCmp)
    {
      if ((btnBTreeN <= 0) || (btnBTreeN > _btnMaxBTreeN))
        throw new ArgumentOutOfRangeException();
      if (allocIdxFast > _btnMaxIdxFast)
        throw new ArgumentOutOfRangeException();
      _btnUpdatedRoot = false;
      _btnVersion = 0;
      _btnVersionPage = 0;
      _btnLockAdd = new object();
      _btnRoot = null;
      _btnBTreeN = btnBTreeN;
      _btnKVFast = new KeyValueFast[allocIdxFast];
      _objCompares = objCmp;
    }
    //////////////////////////
    public TValue this[TKey key]
    {
      get
      {
        if (BtnFind(key, out TValue value) == true)
          return value;
        return default(TValue);
      }
      set
      {
        BtnAdd(key, ref value);
      }
    }
    //////////////////////////
// Specify if necessary, the symbol of:   Project -> Properties -> Build -> Conditional Compilation Symbols 
#if KEY_VALUE_PAIR
    public KeyValuePair<TKey, TValue>? this[TKey key, bool bNext]
    {
      get
      {
        if (!bNext)
          return BtnSearch(key);
        return BtnNext(key);
      }
    }
#else
    public (TKey key, TValue value)? this[TKey key, bool bNext]
    {
      get
      {
        if (!bNext)
          return BtnSearch(key);
        return BtnNext(key);
      }
    }
#endif
    //////////////////////////
    public bool TryGetValue(TKey key, out TValue value)
    {
      if (BtnFind(key, out TValue valueOut) == true)
      {
        value = valueOut;
        return true;
      }
      value = default(TValue);
      return false;
    }
    //////////////////////////
    protected virtual void BtnAddFirst(TValue valueIn, out TValue valueAdd)
    {
      valueAdd = valueIn;
    }
    //////////////////////////
    protected virtual int BtnCompares(TKey keyX, TKey keyY, object objCmp)
    {
      // return < 0 (less), = 0 (equal), > 0 (greater)
      return keyX.CompareTo(keyY);
    }
    //////////////////////////
    protected virtual bool BtnUpdates(TKey keyAdd, TValue valueAdd, ref TValue valueUpdates, object objUpdates)
    {
      return false;
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////           BtnAdd           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    private bool? _BtnAdd(ref KeyValue kvAdd, out KeyValue kvUp, ref KeyValuePage kvPageUp, ref bool bUp, object objUpdates)
    {
      bool? bNullResult = null;
      if (kvPageUp == null)
      { 
        bUp = true;
        kvUp.key = kvAdd.key;
        BtnAddFirst(kvAdd.value, out kvUp.value);
      }
      else
      {
        int iResultCmp;
        int middle, low = 1, high = kvPageUp.iDataCount;
        KeyValuePage QQ = null;
        // binary search
        do
        {
          middle = (low + high);
          middle >>= 1;   // deleno 2
          iResultCmp = BtnCompares(kvAdd.key, kvPageUp.aData[middle].key, _objCompares);
          if (iResultCmp < 0)
            high = middle - 1;
          else if (iResultCmp > 0)
            low = middle + 1;
        }
        while ((high >= low) && (iResultCmp != 0));

        // zpracuj vysledek
        if (iResultCmp == 0)
        {
          bUp = false;
          // bVyslNull je pri update = false, else null - viz predchozi radek; add = true - viz vyse
          if (BtnUpdates(kvAdd.key, kvAdd.value, ref kvPageUp.aData[middle].value, objUpdates))
            kvPageUp.bUpdatedValue = true;
          kvAdd = kvPageUp.aData[middle];
          kvUp = default(KeyValue);
          bNullResult = new bool?(false);
        }
        else
        {
          if (kvPageUp.kvPageNextRight == null)      // last level
            QQ = null;
          else if (high > 0)
            QQ = kvPageUp.kvPageNextRight[high];     // right
          else
            QQ = kvPageUp.kvPageNextRight[0];        // left
          bNullResult = _BtnAdd(ref kvAdd, out kvUp, ref QQ, ref bUp, objUpdates);
          if (bUp)
          {
            if (kvPageUp.iDataCount < (BtnBTreeN * 2))     // pridej kvAdd do prave vetve, je volne misto
            {
              _btnVersion++;
              if (kvPageUp.kvPageNextRight != null)
                _btnUpdatedRoot = true;
              else
                kvPageUp.bUpdatedValue = true;
              bUp = false;
              for (int ii = kvPageUp.iDataCount+1; ii > high + 1; ii--)
              {
                kvPageUp.aData[ii] = kvPageUp.aData[ii - 1];
                if (kvPageUp.kvPageNextRight != null)
                  kvPageUp.kvPageNextRight[ii] = kvPageUp.kvPageNextRight[ii - 1];
              }
              kvPageUp.aData[high + 1] = kvUp;
              if (QQ != null)
              {
                if (high > 0)
                  kvPageUp.kvPageNextRight[high + 1] = QQ;
                else
                  kvPageUp.kvPageNextRight[1] = QQ;
              }
              kvPageUp.iDataCount++;
              bNullResult = new bool?(true);
            }
            else
            {
              _btnVersionPage++;
              _btnUpdatedRoot = true;
              KeyValuePage kvPageRight = null;
              KeyValue kvUpLocal;
              KeyValuePage kvPageNew = new KeyValuePage(BtnBTreeN, kvPageUp.kvPageNextRight != null);
              if (high <= BtnBTreeN)
              {
                if (kvPageUp.kvPageNextRight != null)
                  kvPageRight = kvPageUp.kvPageNextRight[BtnBTreeN];
                if (high == BtnBTreeN)
                  kvUpLocal = kvUp;
                else
                {
                  kvUpLocal = kvPageUp.aData[BtnBTreeN];
                  for (int ii = BtnBTreeN; ii >= high + 2; ii--)
                  {
                    kvPageUp.aData[ii] = kvPageUp.aData[ii - 1];
                    if (kvPageUp.kvPageNextRight != null)
                      kvPageUp.kvPageNextRight[ii] = kvPageUp.kvPageNextRight[ii - 1];
                  }
                  kvPageUp.aData[high + 1] = kvUp;
                }
                for (int ii = 1; ii <= BtnBTreeN; ii++)
                {
                  kvPageNew.aData[ii] = kvPageUp.aData[ii + BtnBTreeN];
                  if (kvPageNew.kvPageNextRight != null)
                    kvPageNew.kvPageNextRight[ii] = kvPageUp.kvPageNextRight[ii + BtnBTreeN];
                }
                if (kvPageUp.kvPageNextRight != null)
                {
                  if (high < BtnBTreeN)
                  {
                    kvPageNew.kvPageNextRight[0] = kvPageRight;
                    kvPageUp.kvPageNextRight[high + 1] = QQ;
                  }
                  else
                    kvPageNew.kvPageNextRight[0] = QQ;
                }
              }
              else
              {
                high -= BtnBTreeN;
                kvUpLocal = kvPageUp.aData[BtnBTreeN + 1];
                if (kvPageUp.kvPageNextRight != null)
                  kvPageRight = kvPageUp.kvPageNextRight[BtnBTreeN + 1];
                for (int ii = 1; ii < high; ii++)
                {
                  kvPageNew.aData[ii] = kvPageUp.aData[ii + BtnBTreeN + 1];
                  if (kvPageNew.kvPageNextRight != null)
                    kvPageNew.kvPageNextRight[ii] = kvPageUp.kvPageNextRight[ii + BtnBTreeN + 1];
                }
                kvPageNew.aData[high] = kvUp;
                for (int ii = high + 1; ii <= BtnBTreeN; ii++)
                {
                  kvPageNew.aData[ii] = kvPageUp.aData[ii + BtnBTreeN];
                  if (kvPageNew.kvPageNextRight != null)
                    kvPageNew.kvPageNextRight[ii] = kvPageUp.kvPageNextRight[ii + BtnBTreeN];
                }
                if (kvPageNew.kvPageNextRight != null)
                {
                  kvPageNew.kvPageNextRight[0] = kvPageRight;
                  kvPageNew.kvPageNextRight[high] = QQ;
                }
              }
              kvUp = kvUpLocal;

              kvPageUp.iDataCount = BtnBTreeN;
              kvPageNew.iDataCount = BtnBTreeN;
              kvPageUp = kvPageNew;
            }
          }
        }
      }
      return bNullResult;
    }
    //////////////////////////
    public bool? BtnAddNoLock(TKey key, ref TValue value, object objUpdates)
    {
      KeyValue keyValue;
      keyValue.key = key;
      keyValue.value = value;
      bool bUp = false;
      KeyValuePage kvPageDown = _btnRoot;
      bool? bNullResult = null;

      bNullResult = _BtnAdd(ref keyValue, out KeyValue kvUp, ref kvPageDown, ref bUp, objUpdates);
      if (bUp)
      {
        bUp = false;
        _btnVersion++;
        _btnVersionPage++;
        _btnUpdatedRoot = true;
        KeyValuePage QQ = new KeyValuePage(BtnBTreeN, kvPageDown != null) { iDataCount = 1 };
        QQ.aData[1] = kvUp;
        if (QQ.kvPageNextRight != null)
        {
          QQ.kvPageNextRight[0] = _btnRoot;
          QQ.kvPageNextRight[1] = kvPageDown;
        }
        _btnRoot = QQ;
        bNullResult = new bool?(true);
      }
      if (bNullResult != null)
        value = keyValue.value;
      else
        value = default(TValue);

      return bNullResult; // add = true, update = false, else null;
    }
    //////////////////////////
    public bool? BtnAddNoLock(TKey key, TValue value, object objUpdates)
    {
      return BtnAddNoLock(key, ref value, objUpdates);
    }
    //////////////////////////
    public bool? BtnAddNoLock(TKey key, ref TValue value)
    {
      return BtnAddNoLock(key, ref value, null);
    }
    //////////////////////////
    public bool? BtnAddNoLock(TKey key, TValue value)
    {
      return BtnAddNoLock(key, ref value, null);
    }
    //////////////////////////
    public bool? BtnAdd(TKey key, ref TValue value, object objUpdates)
    {
      lock (_btnLockAdd)
      {
        return BtnAddNoLock(key, ref value, objUpdates);
      }
    }
    //////////////////////////
    public bool? BtnAdd(TKey key, TValue value, object objUpdates)
    {
      lock (_btnLockAdd)
      {
        return BtnAddNoLock(key, ref value, objUpdates);
      }
    }
    //////////////////////////
    public bool? BtnAdd(TKey key, ref TValue value)
    {
      lock (_btnLockAdd)
      {
        return BtnAddNoLock(key, ref value, null);
      }
    }
    //////////////////////////
    public bool? BtnAdd(TKey key, TValue value)
    {
      lock (_btnLockAdd)
      {
        return BtnAddNoLock(key, ref value, null);
      }
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////         BtnDeleteAll         //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public void BtnDeleteAll()
    {
      BtnDeleteAll(false);
    }
    //////////////////////////
    public virtual void BtnDeleteAll(bool bRunGC)
    {
      lock (_btnLockAdd)
      {
        _btnVersion++;
        _btnVersionPage++;
        _btnUpdatedRoot = true;
        _btnRoot = null;
        if (_btnKVFast != null)
        {
          for (int idx = 0; idx < _btnKVFast.Length; idx++)
            _btnKVFast[idx].fastPage = null;
        }
        _btnKVFast = null;
        if (bRunGC)
          GC.GetTotalMemory(true);
      }
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////            BtnFind           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected bool? _BtnFind(ref TKey key, out TValue value, KeyValuePage kvPage, out KeyValueFast btnFast)
    {
      bool? bNullResult = null;
      if (kvPage == null)
      {
        value = default(TValue);
        btnFast = default(KeyValueFast);
      }
      else
      {
        int iResultCmp;
        int middle, low = 1, high = kvPage.iDataCount;
        KeyValuePage QQ = null;
        // binary search
        do
        {
          middle = (low + high);
          middle >>= 1;   // deleno 2
          iResultCmp = BtnCompares(key, kvPage.aData[middle].key, _objCompares);
          if (iResultCmp < 0)
            high = middle - 1;
          else if (iResultCmp > 0)
            low = middle + 1;
        }
        while ((high >= low) && (iResultCmp != 0));
        // zpracuj vysledek
        if (iResultCmp == 0)
        {
          key = kvPage.aData[middle].key;
          value = kvPage.aData[middle].value;
          if (kvPage.kvPageNextRight == null)
          {
            btnFast.version = BtnVersion;
            btnFast.fastMiddle = middle;
            btnFast.fastPage = kvPage;
          }
          else
            btnFast = default(KeyValueFast);
          bNullResult = new bool?(true);
        }
        else
        {
          if (kvPage.kvPageNextRight == null)
            QQ = null;
          else if (high > 0)
            QQ = kvPage.kvPageNextRight[high];       // right
          else
            QQ = kvPage.kvPageNextRight[0];          // left
          bNullResult = _BtnFind(ref key, out value, QQ, out btnFast);
        }
      }
      return bNullResult;
    }
    //////////////////////////
    public virtual  bool? BtnFind(TKey key, out TValue value)
    {
      return _BtnFind(ref key, out value, _btnRoot, out _btnKVFast[0]);
    }
    //////////////////////////
#if KEY_VALUE_PAIR
    public virtual KeyValuePair<TKey, TValue>? BtnFind(TKey key)
    {
      if (_BtnFind(ref key, out TValue valueOut, _btnRoot, out _btnKVFast[0]) != null)
        return new KeyValuePair<TKey, TValue>(key, valueOut);
      else
        return null;
    }
#else
    public virtual (TKey key, TValue value)? BtnFind(TKey key)
    {
      if (_BtnFind(ref key, out TValue valueOut, _btnRoot, out _btnKVFast[0]) != null)
        return (key: key, value: valueOut);
      else
        return null;
    }
#endif
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////           BtnFirst           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected bool? _BtnFirst(out TKey key, out TValue value, out KeyValueFast btnFast)
    {
      KeyValuePage QQ = _btnRoot;
      while ((QQ != null) && (QQ.kvPageNextRight != null))
        QQ = QQ.kvPageNextRight[0];

      if (QQ != null)
      {
        key = QQ.aData[1].key;
        value = QQ.aData[1].value;
        btnFast.version = BtnVersion;
        btnFast.fastMiddle = 1;
        btnFast.fastPage = QQ;
        return new bool?(true);
      }
      else
      {
        key = default(TKey);
        value = default(TValue);
        btnFast = default(KeyValueFast);
        return null;
      }
    }
    public virtual bool? BtnFirst(out TKey key, out TValue value)
    {
      return _BtnFirst(out key, out value, out _btnKVFast[0]);
    }
    //////////////////////////
#if KEY_VALUE_PAIR
    protected KeyValuePair<TKey, TValue>? _BtnFirst(out KeyValueFast btnFast)
    {
      KeyValuePage QQ = _btnRoot;
      while ((QQ != null) && (QQ.kvPageNextRight != null))
        QQ = QQ.kvPageNextRight[0];

      if (QQ != null)
      {
        btnFast.version = BtnVersion;
        btnFast.fastMiddle = 1;
        btnFast.fastPage = QQ;
        return new KeyValuePair<TKey, TValue>(QQ.aData[1].key, QQ.aData[1].value);
      }
      else
      {
        btnFast = default(KeyValueFast);
        return null;
      }
    }
    public virtual KeyValuePair<TKey, TValue>? BtnFirst()
    {
      return _BtnFirst(out _btnKVFast[0]);
    }
#else
    protected (TKey key, TValue value)? _BtnFirst(out BtnFastKeyValue btnFast)
    {
      BtnKeyValuePage QQ = _btnRoot;
      while ((QQ != null) && (QQ.kvPageNextRight != null))
        QQ = QQ.kvPageNextRight[0];

      if (QQ != null)
      {
        btnFast.version = BtnVersion;
        btnFast.fastMiddle = 1;
        btnFast.fastPage = QQ;
        return (key: QQ.aData[1].key, value: QQ.aData[1].value);
      }
      else
      {
        btnFast = default(BtnFastKeyValue);
        return null;
      }
    }
    public virtual (TKey key, TValue value)? BtnFirst()
    {
      return _BtnFirst(out _btnKVFast[0]);
    }
#endif
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////            BtnLast           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected bool? _BtnLast(out TKey key, out TValue value, out KeyValueFast btnFast)
    {
      KeyValuePage QQ = _btnRoot;
      while ((QQ != null) && (QQ.kvPageNextRight != null))
        QQ = QQ.kvPageNextRight[QQ.iDataCount];

      if (QQ != null)
      {
        key = QQ.aData[QQ.iDataCount].key;
        value = QQ.aData[QQ.iDataCount].value;
        btnFast.version = BtnVersion;
        btnFast.fastMiddle = QQ.iDataCount;
        btnFast.fastPage = QQ;
        return new bool?(true);
      }
      else
      {
        key = default(TKey);
        value = default(TValue);
        btnFast = default(KeyValueFast);
        return null;
      }
    }
    public virtual bool? BtnLast(out TKey key, out TValue value)
    {
      return _BtnLast(out key, out value, out _btnKVFast[0]);
    }
    //////////////////////////
#if KEY_VALUE_PAIR
    protected KeyValuePair<TKey, TValue>? _BtnLast(out KeyValueFast btnFast)
    {
      KeyValuePage QQ = _btnRoot;
      while ((QQ != null) && (QQ.kvPageNextRight != null))
        QQ = QQ.kvPageNextRight[QQ.iDataCount];

      if (QQ != null)
      {
        btnFast.version = BtnVersion;
        btnFast.fastMiddle = QQ.iDataCount;
        btnFast.fastPage = QQ;
        return new KeyValuePair<TKey, TValue>(QQ.aData[QQ.iDataCount].key, QQ.aData[QQ.iDataCount].value);
      }
      else
      {
        btnFast = default(KeyValueFast);
        return null;
      }
    }
    public virtual KeyValuePair<TKey, TValue>? BtnLast()
    {
      return _BtnLast(out _btnKVFast[0]);
    }
#else
    protected (TKey key, TValue value)? _BtnLast(out BtnFastKeyValue btnFast)
    {
      BtnKeyValuePage QQ = _btnRoot;
      while ((QQ != null) && (QQ.kvPageNextRight != null))
        QQ = QQ.kvPageNextRight[QQ.iDataCount];

      if (QQ != null)
      {
        btnFast.version = BtnVersion;
        btnFast.fastMiddle = QQ.iDataCount;
        btnFast.fastPage = QQ;
        return (key: QQ.aData[QQ.iDataCount].key, value: QQ.aData[QQ.iDataCount].value);
      }
      else
      {
        btnFast = default(BtnFastKeyValue);
        return null;
      }
    }
    public virtual (TKey key, TValue value)? BtnLast()
    {
      return _BtnLast(out _btnKVFast[0]);
    }
#endif
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////            BtnNext           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected bool? _BtnNext(ref TKey key, out TValue value, KeyValuePage kvPage, ref bool bNext, out KeyValueFast btnFast)
    {
      bool? bNullResult = null;
      if (kvPage == null)
      {
        value = default(TValue);
        btnFast = default(KeyValueFast);
        bNext = false;
      }
      else
      {
        int iResultCmp;
        int middle, low = 1, high = kvPage.iDataCount;
        KeyValuePage QQ = null;
        // binary search
        do
        {
          middle = (low + high);
          middle >>= 1;   // deleno 2
          iResultCmp = BtnCompares(key, kvPage.aData[middle].key, _objCompares);
          if (iResultCmp < 0)
            high = middle - 1;
          else if (iResultCmp > 0)
            low = middle + 1;
        }
        while ((high >= low) && (iResultCmp != 0));
        // zpracuj vysledek
        if (iResultCmp == 0)
        {
          bNext = false;
          if (kvPage.kvPageNextRight != null)
          {
            QQ = kvPage.kvPageNextRight[middle];
            while ((QQ != null) && (QQ.kvPageNextRight != null))
              QQ = QQ.kvPageNextRight[0];
            key = QQ.aData[1].key;
            value = QQ.aData[1].value;
            if (QQ.kvPageNextRight == null)
            {
              btnFast.version = BtnVersion;
              btnFast.fastMiddle = 1;
              btnFast.fastPage = QQ;
            }
            else
              btnFast = default(KeyValueFast);
            bNullResult = new bool?(true);
          }
          else
          {
            if (middle < kvPage.iDataCount)
            {
              key = kvPage.aData[middle + 1].key;
              value = kvPage.aData[middle + 1].value;
              btnFast.version = BtnVersion;
              btnFast.fastMiddle = middle + 1;
              btnFast.fastPage = kvPage;
              bNullResult = new bool?(true);
            }
            else
            {
              bNext = true;
              value = default(TValue);
              btnFast = default(KeyValueFast);
            }
          }
        }
        else
        {
          if (kvPage.kvPageNextRight == null)
            QQ = null;
          else if (high > 0)
            QQ = kvPage.kvPageNextRight[high];       // right
          else
            QQ = kvPage.kvPageNextRight[0];          // left
          bNullResult = _BtnNext(ref key, out value, QQ, ref bNext, out btnFast);
          if ((bNext == true) && ((iResultCmp < 0) || (middle < kvPage.iDataCount)))
          {
            if ((iResultCmp > 0) && (middle < kvPage.iDataCount))
              middle++;
            key = kvPage.aData[middle].key;
            value = kvPage.aData[middle].value;
            if (kvPage.kvPageNextRight == null)
            {
              btnFast.version = BtnVersion;
              btnFast.fastMiddle = middle;
              btnFast.fastPage = kvPage;
            }
            bNext = false;
            bNullResult = new bool?(true);
          }
        }
      }
      return bNullResult;
    }
    //////////////////////////
    public virtual bool? BtnNext(ref TKey key, out TValue value)
    {
      bool bNext = false;
      return _BtnNext(ref key, out value, _btnRoot, ref bNext, out _btnKVFast[0]);
    }
    //////////////////////////
#if KEY_VALUE_PAIR
    public virtual KeyValuePair<TKey, TValue>? BtnNext(TKey key)
    {
      bool bNext = false;
      if (_BtnNext(ref key, out TValue valueOut, _btnRoot, ref bNext, out _btnKVFast[0]) != null)
        return new KeyValuePair<TKey, TValue>(key, valueOut);
      else
        return null;
    }
#else
    public virtual (TKey key, TValue value)? BtnNext(TKey key)
    {
      bool bNext = false;
      if (_BtnNext(ref key, out TValue valueOut, _btnRoot, ref bNext, out _btnKVFast[0]) != null)
        return (key: key, value: valueOut);
      else
        return null;
    }
#endif
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////            BtnPrev           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected bool? _BtnPrev(ref TKey key, out TValue value, KeyValuePage kvPage, ref bool bNext, out KeyValueFast btnFast)
    {
      bool? bNullResult = null;
      if (kvPage == null)
      {
        value = default(TValue);
        btnFast = default(KeyValueFast);
        bNext = false;
      }
      else
      {
        int iResultCmp;
        int middle, low = 1, high = kvPage.iDataCount;
        KeyValuePage QQ = null;
        // binary search
        do
        {
          middle = (low + high);
          middle >>= 1;   // deleno 2
          iResultCmp = BtnCompares(key, kvPage.aData[middle].key, _objCompares);
          if (iResultCmp < 0)
            high = middle - 1;
          else if (iResultCmp > 0)
            low = middle + 1;
        }
        while ((high >= low) && (iResultCmp != 0));
        // zpracuj vysledek
        if (iResultCmp == 0)
        {
          bNext = false;
          if (kvPage.kvPageNextRight != null)
          {
            middle--;
            if (middle > 0)
              QQ = kvPage.kvPageNextRight[middle];
            else
              QQ = kvPage.kvPageNextRight[0];
            while ((QQ != null) && (QQ.kvPageNextRight != null))
              QQ = QQ.kvPageNextRight[QQ.iDataCount];
            key = QQ.aData[QQ.iDataCount].key;
            value = QQ.aData[QQ.iDataCount].value;
            if (QQ.kvPageNextRight == null)
            {
              btnFast.version = BtnVersion;
              btnFast.fastMiddle = QQ.iDataCount;
              btnFast.fastPage = QQ;
            }
            else
              btnFast = default(KeyValueFast);
            bNullResult = new bool?(true);
          }
          else
          {
            if (middle > 1)
            {
              key = kvPage.aData[middle - 1].key;
              value = kvPage.aData[middle - 1].value;
              btnFast.version = BtnVersion;
              btnFast.fastMiddle = middle - 1;
              btnFast.fastPage = kvPage;
              bNullResult = new bool?(true);
            }
            else
            {
              bNext = true;
              value = default(TValue);
              btnFast = default(KeyValueFast);
            }
          }
        }
        else
        {
          if (kvPage.kvPageNextRight == null)
            QQ = null;
          else if (high > 0)
            QQ = kvPage.kvPageNextRight[high];       // right
          else
            QQ = kvPage.kvPageNextRight[0];          // left
          bNullResult = _BtnPrev(ref key, out value, QQ, ref bNext, out btnFast);
          if ((bNext == true) && ((iResultCmp >= 0) || (middle > 1)))
          {
            if ((iResultCmp < 0) && (middle > 1))
              middle--;
            key = kvPage.aData[middle].key;
            value = kvPage.aData[middle].value;
            if (kvPage.kvPageNextRight == null)
            {
              btnFast.version = BtnVersion;
              btnFast.fastMiddle = middle;
              btnFast.fastPage = kvPage;
            }
            bNext = false;
            bNullResult = new bool?(true);
          }
        }
      }
      return bNullResult;
    }
    //////////////////////////
    public virtual bool? BtnPrev(ref TKey key, out TValue value)
    {
      bool bNext = false;
      return _BtnPrev(ref key, out value, _btnRoot, ref bNext, out _btnKVFast[0]);
    }
    //////////////////////////
#if KEY_VALUE_PAIR
    public virtual KeyValuePair<TKey, TValue>? BtnPrev(TKey key)
    {
      bool bNext = false;
      if (_BtnPrev(ref key, out TValue valueOut, _btnRoot, ref bNext, out _btnKVFast[0]) != null)
        return new KeyValuePair<TKey, TValue>(key, valueOut);
      else
        return null;
    }
#else
    public virtual (TKey key, TValue value)? BtnPrev(TKey key)
    {
      bool bNext = false;
      if (_BtnPrev(ref key, out TValue valueOut, _btnRoot, ref bNext, out _btnKVFast[0]) != null)
        return (key: key, value: valueOut);
      else
        return null;
    }
#endif
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////           BtnSearch          //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected bool? _BtnSearch(ref TKey key, out TValue value, KeyValuePage kvPage, ref bool bNext, out KeyValueFast btnFast)
    {
      bool? bNullResult = null;
      if (kvPage == null)
      {
        value = default(TValue);
        btnFast = default(KeyValueFast);
        bNext = true;
      }
      else
      {
        int iResultCmp;
        int middle, low = 1, high = kvPage.iDataCount;
        KeyValuePage QQ = null;
        // binary search
        do
        {
          middle = (low + high);
          middle >>= 1;   // deleno 2
          iResultCmp = BtnCompares(key, kvPage.aData[middle].key, _objCompares);
          if (iResultCmp < 0)
            high = middle - 1;
          else if (iResultCmp > 0)
            low = middle + 1;
        }
        while ((high >= low) && (iResultCmp != 0));
        // zpracuj vysledek
        if (iResultCmp == 0)
        {
          key = kvPage.aData[middle].key;
          value = kvPage.aData[middle].value;
          if (kvPage.kvPageNextRight == null)
          {
            btnFast.version = BtnVersion;
            btnFast.fastMiddle = middle;
            btnFast.fastPage = kvPage;
          }
          else
            btnFast = default(KeyValueFast);
          bNext = false;
          bNullResult = new bool?(true);
        }
        else
        {
          if (kvPage.kvPageNextRight == null)
            QQ = null;
          else if (high > 0)
            QQ = kvPage.kvPageNextRight[high];       // right
          else
            QQ = kvPage.kvPageNextRight[0];          // left
          bNullResult = _BtnSearch(ref key, out value, QQ, ref bNext, out btnFast);
          if ((bNext == true) && ((iResultCmp < 0) || (middle < kvPage.iDataCount)))
          {
            if ((iResultCmp > 0) && (middle < kvPage.iDataCount))
              middle++;
            key = kvPage.aData[middle].key;
            value = kvPage.aData[middle].value;
            if (kvPage.kvPageNextRight == null)
            {
              btnFast.version = BtnVersion;
              btnFast.fastMiddle = middle;
              btnFast.fastPage = kvPage;
            }
            bNext = false;
            bNullResult = new bool?(false);
          }
        }
      }
      return bNullResult;
    }
    //////////////////////////
    public virtual bool? BtnSearch(ref TKey key, out TValue value)
    {
      bool bNext = false;
      return _BtnSearch(ref key, out value, _btnRoot, ref bNext, out _btnKVFast[0]);
    }
    //////////////////////////
#if KEY_VALUE_PAIR
    public virtual KeyValuePair<TKey, TValue>? BtnSearch(TKey key)
    {
      bool bNext = false;
      if (_BtnSearch(ref key, out TValue valueOut, _btnRoot, ref bNext, out _btnKVFast[0]) != null)
        return new KeyValuePair<TKey, TValue>(key, valueOut);
      else
        return null;
    }
#else
    public virtual (TKey key, TValue value)? BtnSearch(TKey key)
    {
      bool bNext = false;
      if (_BtnSearch(ref key, out TValue valueOut, _btnRoot, ref bNext, out _btnKVFast[0]) != null)
        return (key: key, value: valueOut);
      else
        return null;
    }
#endif
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////         BtnSearchPrev        //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected bool? _BtnSearchPrev(ref TKey key, out TValue value, KeyValuePage kvPage, ref bool bNext, out KeyValueFast btnFast)
    {
      bool? bNullResult = null;
      if (kvPage == null)
      {
        value = default(TValue);
        btnFast = default(KeyValueFast);
        bNext = true;
      }
      else
      {
        int iResultCmp;
        int middle, low = 1, high = kvPage.iDataCount;
        KeyValuePage QQ = null;
        // binary search
        do
        {
          middle = (low + high);
          middle >>= 1;   // deleno 2
          iResultCmp = BtnCompares(key, kvPage.aData[middle].key, _objCompares);
          if (iResultCmp < 0)
            high = middle - 1;
          else if (iResultCmp > 0)
            low = middle + 1;
        }
        while ((high >= low) && (iResultCmp != 0));
        // zpracuj vysledek
        if (iResultCmp == 0)
        {
          bNext = false;
          key = kvPage.aData[middle].key;
          value = kvPage.aData[middle].value;
          if (kvPage.kvPageNextRight == null)
          {
            btnFast.version = BtnVersion;
            btnFast.fastMiddle = middle;
            btnFast.fastPage = kvPage;
          }
          else
            btnFast = default(KeyValueFast);
          bNullResult = new bool?(true);
        }
        else
        {
          if (kvPage.kvPageNextRight == null)
            QQ = null;
          else if (high > 0)
            QQ = kvPage.kvPageNextRight[high];       // right
          else
            QQ = kvPage.kvPageNextRight[0];          // left
          bNullResult = _BtnSearchPrev(ref key, out value, QQ, ref bNext, out btnFast);
          if ((bNext == true) && ((iResultCmp >= 0) || (middle > 1)))
          {
            if ((iResultCmp < 0) && (middle > 1))
              middle--;
            key = kvPage.aData[middle].key;
            value = kvPage.aData[middle].value;
            if (kvPage.kvPageNextRight == null)
            {
              btnFast.version = BtnVersion;
              btnFast.fastMiddle = middle;
              btnFast.fastPage = kvPage;
            }
            bNext = false;
            bNullResult = new bool?(false);
          }
        }
      }
      return bNullResult;
    }
    //////////////////////////
    public virtual bool? BtnSearchPrev(ref TKey key, out TValue value)
    {
      bool bNext = false;
      return _BtnSearchPrev(ref key, out value, _btnRoot, ref bNext, out _btnKVFast[0]);
    }
    //////////////////////////
#if KEY_VALUE_PAIR
    public virtual KeyValuePair<TKey, TValue>? BtnSearchPrev(TKey key)
    {
      bool bNext = false;
      if (_BtnSearchPrev(ref key, out TValue valueOut, _btnRoot, ref bNext, out _btnKVFast[0]) != null)
        return new KeyValuePair<TKey, TValue>(key, valueOut);
      else
        return null;
    }
#else
    public virtual (TKey key, TValue value)? BtnSearchPrev(TKey key)
    {
      bool bNext = false;
      if (_BtnSearchPrev(ref key, out TValue valueOut, _btnRoot, ref bNext, out _btnKVFast[0]) != null)
        return (key: key, value: valueOut);
      else
        return null;
    }
#endif
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////         BtnUpdate          //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    private bool? _BtnUpdate(ref KeyValue kvUpdate, KeyValuePage kvPage, object objUpdates)
    {
      bool? bNullResult = null;
      if (kvPage != null)
      {
        int iResultCmp;
        int middle, low = 1, high = kvPage.iDataCount;
        KeyValuePage QQ = null;
        // binary search
        do
        {
          middle = (low + high);
          middle >>= 1;   // deleno 2
          iResultCmp = BtnCompares(kvUpdate.key, kvPage.aData[middle].key, _objCompares);
          if (iResultCmp < 0)
            high = middle - 1;
          else if (iResultCmp > 0)
            low = middle + 1;
        }
        while ((high >= low) && (iResultCmp != 0));

        // zpracuj vysledek
        if (iResultCmp == 0)
        {
          // bNullResult je pri update = false, jinak null 
          if (BtnUpdates(kvUpdate.key, kvUpdate.value, ref kvPage.aData[middle].value, objUpdates))
            kvPage.bUpdatedValue = true;
          kvUpdate = kvPage.aData[middle];
          bNullResult = new bool?(false);
        }
        else
        {
          if (kvPage.kvPageNextRight == null)        // last level
            QQ = null;
          else if (high > 0)
            QQ = kvPage.kvPageNextRight[high];       // right
          else
            QQ = kvPage.kvPageNextRight[0];          // left
          bNullResult = _BtnUpdate(ref kvUpdate, QQ, objUpdates);
        }
      }
      return bNullResult;
    }
    //////////////////////////
    public bool? BtnUpdate(TKey key, ref TValue value, object objUpdates)
    {
      KeyValue keyValue;
      keyValue.key = key;
      keyValue.value = value;
      KeyValuePage kvPageDown = _btnRoot;
      bool? bNullResult = null;

      bNullResult = _BtnUpdate(ref keyValue, kvPageDown, objUpdates);
      if (bNullResult != null)
        value = keyValue.value;
      else
        value = default(TValue);

      return bNullResult; // update = false, else null;
    }
    //////////////////////////
    public bool? BtnUpdate(TKey key, TValue value, object objUpdates)
    {
      return BtnUpdate(key, ref value, objUpdates);
    }
    //////////////////////////
    public bool? BtnUpdate(TKey key, ref TValue value)
    {
      return BtnUpdate(key, ref value, null);
    }
    //////////////////////////
    public bool? BtnUpdate(TKey key, TValue value)
    {
      return BtnUpdate(key, ref value, null);
    }
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////           BtnUsedKeys           ///////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    private void _BtnUsedKeys(ref uint Count, KeyValuePage QQ)
    {
      if (QQ != null)
      {
        if (QQ.kvPageNextRight != null)
        {
          _BtnUsedKeys(ref Count, QQ.kvPageNextRight[0]);
          for (int II = 1; II <= QQ.iDataCount; II++)
            _BtnUsedKeys(ref Count, QQ.kvPageNextRight[II]);
        }
        Count += (uint)QQ.iDataCount;
      }
    }
    //////////////////////////
    public virtual uint BtnUsedKeys()
    {
      uint Count = 0;
      _BtnUsedKeys(ref Count, _btnRoot);
      return Count;
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////         IEnumerator          //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public FcsBTreeN<TKey, TValue>.BtnEnumerator GetEnumeratorEx(bool reverse)
    {
      return new BtnEnumerator(this, default(TKey), default(TKey), reverse, -2);
    }
    //////////////////////////
    public FcsBTreeN<TKey, TValue>.BtnEnumerator GetEnumeratorEx(bool reverse, int maxCount)
    {
      return new BtnEnumerator(this, default(TKey), default(TKey), reverse, maxCount);
    }
    //////////////////////////
    public FcsBTreeN<TKey, TValue>.BtnEnumerator GetEnumeratorEx(TKey keyLo, TKey keyHi, bool reverse)
    {
      return new BtnEnumerator(this, keyLo, keyHi, reverse, -3);
    }
    //////////////////////////
    public FcsBTreeN<TKey, TValue>.BtnEnumerator GetEnumeratorEx(TKey keyLo, TKey keyHi, bool reverse, int maxCount)
    {
      return new BtnEnumerator(this, keyLo, keyHi, reverse, maxCount);
    }
    //////////////////////////
    IEnumerator IEnumerable.GetEnumerator()
    {
      // call the generic version of the method
      return GetEnumerator();
    }
    //////////////////////////
    public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
    {
      return new BtnEnumerator(this, default(TKey), default(TKey), false, -1);
    }
    //////////////////////////
    public class BtnEnumerator : IEnumerator<KeyValuePair<TKey, TValue>>
    {
      // constructor
      private FcsBTreeN<TKey, TValue> _btn = null;
      private TKey  _keyLo;
      private TKey  _keyHi;
      private int    _maxCount;
      private bool   _reverse;
      // locals
      private bool   _bOK;
      private bool   _bFirst;
      private TKey   _key;
      private TValue _value;
      private int    _count;
      //////////////////////////
      public BtnEnumerator(FcsBTreeN<TKey, TValue> btn, TKey keyLo, TKey keyHi, bool reverse, int maxCount)
      {
        _btn = btn ?? throw new NullReferenceException();
        _keyLo = keyLo;
        _keyHi = keyHi;
        _maxCount = maxCount;
        _reverse = reverse;
        Reset();
      }
      //////////////////////////
      public bool MoveNext()
      {
        if ((_count == 0) || (!_bOK))
          return false;
        else if (_count > 0)
          _count--;

        if (_bFirst)
        {
          _bFirst = false;
          if ((_keyLo.Equals(default(TKey))) && (!_reverse))
            _bOK = (_btn.BtnFirst(out _key, out _value) != null);
          else if ((_keyHi.Equals(default(TKey))) && (_reverse))
            _bOK = (_btn.BtnLast(out _key, out _value) != null);
          else
          {
            if (_reverse)
            {
              _key = _keyHi;
              _bOK = (_btn.BtnSearchPrev(ref _key, out _value) != null);
              if ((!_keyLo.Equals(default(TKey))) && (_bOK))
                _bOK = (_key.CompareTo(_keyLo) >= 0);
            }
            else
            {
              _key = _keyLo;
              _bOK = (_btn.BtnSearch(ref _key, out _value) != null);
              if ((!_keyHi.Equals(default(TKey))) && (_bOK))
                _bOK = (_key.CompareTo(_keyHi) <= 0);
            }
          }
        }
        else
        {
          if (_reverse)
          {
            _bOK = (_btn.BtnPrev(ref _key, out _value) != null);
            if ((!_keyLo.Equals(default(TKey))) && (_bOK))
              _bOK = (_key.CompareTo(_keyLo) >= 0);
          }
          else
          {
            _bOK = (_btn.BtnNext(ref _key, out _value) != null);
            if ((!_keyHi.Equals(default(TKey))) && (_bOK))
              _bOK = (_key.CompareTo(_keyHi) <= 0);
          }
        }
        return _bOK;
      }
      //////////////////////////
      public KeyValuePair<TKey, TValue> Current
      {
        get
        {
          if (!_bOK)
            return new KeyValuePair<TKey, TValue>(default(TKey), default(TValue));
          return new KeyValuePair<TKey, TValue>(_key, _value);
        }
      }
      //////////////////////////
      object IEnumerator.Current
      {
        get
        {
          if (!_bOK)
            return new KeyValuePair<TKey, TValue>(default(TKey), default(TValue));
          return new KeyValuePair<TKey, TValue>(_key, _value);
        }
      }
      //////////////////////////
      public void Dispose() { _btn = null; }
      //////////////////////////
      public void Reset()
      {
        _count = _maxCount;
        _bOK = true;
        _bFirst = true;
      }
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////         !IEnumerator         //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  }
}
