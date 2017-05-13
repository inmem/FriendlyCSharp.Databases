// Licensed to the KUBDAT® under one or more agreements.
// The KUBDAT® licenses this file to you under the Apache-2.0 license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections;
using System.Collections.Generic;

namespace FriendlyCSharp.Databases
{
  public partial class FcsDuplValueFastBTreeN<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue[]>>
                                                              where TKey : struct, IComparable<TKey>
  {
    protected const int _btnDefaultBTreeN = 32;
    protected object _btnLockAdd;
    protected BtnDuplValuePage _btnRoot = null;
    protected BtnFastDuplValue _btnFast;
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
    //////////////////////////
    protected internal struct BtnDuplValue
    {
      public TKey key;
      public TValue[] value;
    }
    //////////////////////////
    public struct BtnFastDuplValue : IDisposable
    {
      internal int version;
      internal int fastMiddle;
      internal BtnDuplValuePage fastPage;
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
    protected internal class BtnDuplValuePage
    {
      public UInt32 flags;
      public bool bUpdatedValue;
      public int iDataCount;
      public BtnDuplValue[] aData;
      public BtnDuplValuePage[] kvPageNextRight;
      public long[] aPos;
      //////////////////////////
      public BtnDuplValuePage(int iPageN, bool bPageNext)
      {
        bUpdatedValue = false;
        flags = 0;
        iDataCount = 0;
        aData = new BtnDuplValue[(iPageN * 2) + 1];
        if (bPageNext)
          kvPageNextRight = new BtnDuplValuePage[(iPageN * 2) + 1];
        else
          kvPageNextRight = null;
        aPos = null;
      }
    }
    //////////////////////////
    public FcsDuplValueFastBTreeN() : this(_btnDefaultBTreeN, null)
    {
    }
    //////////////////////////
    protected FcsDuplValueFastBTreeN(object objCmp) : this(_btnDefaultBTreeN, objCmp)
    {
    }
    //////////////////////////
    public FcsDuplValueFastBTreeN(int btnBTreeN) : this(btnBTreeN, null)
    {
    }
    //////////////////////////
    protected FcsDuplValueFastBTreeN(int btnBTreeN, object objCmp)
    {
      if ((btnBTreeN <= 0) || (btnBTreeN > _btnMaxBTreeN))
        throw new ArgumentOutOfRangeException();
      _btnUpdatedRoot = false;
      _btnVersion = 0;
      _btnVersionPage = 0;
      _btnFast = default(BtnFastDuplValue);
      _btnLockAdd = new object();
      _btnRoot = null;
      _btnBTreeN = btnBTreeN;
      _objCompares = objCmp;
    }
    //////////////////////////
    public TValue[] this[TKey key]
    {
      get
      {
        if (BtnFind(key, out TValue[] value) == true)
          return value;
        return null;
      }
      set
      {
        BtnAdd(key, ref value);
      }
    }
    //////////////////////////
#if KEY_VALUE_PAIR
    public KeyValuePair<TKey, TValue[]> this[TKey key, bool bNext]
    {
      get
      {
        if (!bNext)
          return BtnSearch(key);
        return BtnNext(key);
      }
    }
#else
    public (TKey key, TValue[] value) this[TKey key, bool bNext]
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
    public bool TryGetValue(TKey key, out TValue[] value)
    {
      if (BtnFind(key, out TValue[] valueOut) == true)
      {
        value = valueOut;
        return true;
      }
      value = null;
      return false;
    }
    //////////////////////////
    protected virtual void BtnAddFirst(TValue[] valueIn, out TValue[] valueAdd)
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
    protected virtual bool BtnUpdates(TKey keyAdd, TValue[] valueAdd, ref TValue[] valueUpdates, object objUpdates)
    {
      return false;
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////           BtnAdd           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    private bool? _BtnAdd(ref BtnDuplValue kvAdd, out BtnDuplValue kvUp, ref BtnDuplValuePage kvPageUp, ref bool bUp, object objUpdates)
    {
      bool? bNullResult = null;
      if (kvPageUp == null)
      {  //  Pridani
        bUp = true;
        kvUp.key = kvAdd.key;
        BtnAddFirst(kvAdd.value, out kvUp.value);
      }
      else
      {
        int iResultCmp;
        int middle, low = 1, high = kvPageUp.iDataCount;
        BtnDuplValuePage QQ = null;
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
          kvUp = default(BtnDuplValue);
          // bVyslNull je pri update = false, jinak null - viz predchozi radek; add = true - viz vyse
          if (BtnUpdates(kvAdd.key, kvAdd.value, ref kvPageUp.aData[middle].value, objUpdates))
            kvPageUp.bUpdatedValue = true;
          kvAdd = kvPageUp.aData[middle];
          bNullResult = new bool?(false);
        }
        else
        {
          if (kvPageUp.kvPageNextRight == null)      // last level
            QQ = null;
          else if (high <= 0)
            QQ = kvPageUp.kvPageNextRight[0];        // left
          else
            QQ = kvPageUp.kvPageNextRight[high];     // right
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
              for (int ii = kvPageUp.iDataCount + 1; ii > high + 1; ii--)
              {
                kvPageUp.aData[ii] = kvPageUp.aData[ii - 1];
                if (kvPageUp.kvPageNextRight != null)
                  kvPageUp.kvPageNextRight[ii] = kvPageUp.kvPageNextRight[ii - 1];
              }
              kvPageUp.aData[high + 1] = kvUp;
              if (QQ != null)
              {
                if (high <= 0)
                  kvPageUp.kvPageNextRight[1] = QQ;
                else
                  kvPageUp.kvPageNextRight[high + 1] = QQ;
              }
              kvPageUp.iDataCount++;
              bNullResult = new bool?(true);
            }
            else
            {
              _btnVersionPage++;
              _btnUpdatedRoot = true;
              BtnDuplValuePage kvPageRight = null;
              BtnDuplValue kvUpLocal;
              BtnDuplValuePage kvPageNew = new BtnDuplValuePage(BtnBTreeN, kvPageUp.kvPageNextRight != null);
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
    public bool? BtnAdd(TKey key, ref TValue[] value, object objUpdates)
    {
      BtnDuplValue kvAdd;
      kvAdd.key = key;
      kvAdd.value = value;
      bool bUp = false;
      BtnDuplValuePage kvPageDown = _btnRoot;
      bool? bNullResult = null;

      lock (_btnLockAdd)
      {
        bNullResult = _BtnAdd(ref kvAdd, out BtnDuplValue kvUp, ref kvPageDown, ref bUp, objUpdates);
        if (bUp)
        {
          bUp = false;
          _btnVersion++;
          _btnVersionPage++;
          _btnUpdatedRoot = true;
          BtnDuplValuePage QQ = new BtnDuplValuePage(BtnBTreeN, kvPageDown != null) { iDataCount = 1 };
          QQ.aData[1] = kvUp;
          if (QQ.kvPageNextRight != null)
          {
            QQ.kvPageNextRight[0] = _btnRoot;
            QQ.kvPageNextRight[1] = kvPageDown;
          }
          _btnRoot = QQ;
          bNullResult = new bool?(true);
        }
      }
      if (bNullResult != null)
        value = kvAdd.value;
      else
        value = null;

      return bNullResult; // add = true, update = false, else null;
    }
    //////////////////////////
    public bool? BtnAdd(TKey key, TValue[] value, object objUpdates)
    {
      return BtnAdd(key, ref value, objUpdates);
    }
    //////////////////////////
    public bool? BtnAdd(TKey key, ref TValue[] value)
    {
      return BtnAdd(key, ref value, null);
    }
    //////////////////////////
    public bool? BtnAdd(TKey key, TValue[] value)
    {
      return BtnAdd(key, ref value, null);
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
        _btnFast.Dispose();
        if (bRunGC)
          GC.GetTotalMemory(true);
      }
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////            BtnFind           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected bool? _BtnFind(ref TKey key, out TValue[] value, BtnDuplValuePage kvPage, out BtnFastDuplValue btnFast)
    {
      bool? bNullResult = null;
      if (kvPage == null)
      {
        value = null;
        btnFast = default(BtnFastDuplValue);
      }
      else
      {
        int iResultCmp;
        int middle, low = 1, high = kvPage.iDataCount;
        BtnDuplValuePage QQ = null;
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
            btnFast = default(BtnFastDuplValue);
          bNullResult = new bool?(true);
        }
        if (kvPage.kvPageNextRight == null)
          QQ = null;
        else if (high <= 0)
          QQ = kvPage.kvPageNextRight[0];        // left
        else
          QQ = kvPage.kvPageNextRight[high];       // right
        bNullResult = _BtnFind(ref key, out value, QQ, out btnFast);
      }
      return bNullResult;
    }
    //////////////////////////
    public virtual bool? BtnFind(TKey key, out TValue[] value)
    {
      return _BtnFind(ref key, out value, _btnRoot, out _btnFast);
    }
    //////////////////////////
    public virtual bool? BtnFastFind(TKey key, out TValue[] value, out BtnFastDuplValue btnFast)
    {
      return _BtnFind(ref key, out value, _btnRoot, out btnFast);
    }
    ////////////////////////// 
#if KEY_VALUE_PAIR
    public virtual KeyValuePair<TKey, TValue[]> BtnFind(TKey key)
    {
      if (_BtnFind(ref key, out TValue[] valueOut, _btnRoot, out _btnFast) != null)
        return new KeyValuePair<TKey, TValue[]>(key, valueOut);
      else
        return new KeyValuePair<TKey, TValue[]>(default(TKey), null);
    }
    ////////////////////////// 
    public virtual KeyValuePair<TKey, TValue[]> BtnFastFind(TKey key, out BtnFastDuplValue btnFast)
    {
      if (_BtnFind(ref key, out TValue[] valueOut, _btnRoot, out btnFast) != null)
        return new KeyValuePair<TKey, TValue[]>(key, valueOut);
      else
        return new KeyValuePair<TKey, TValue[]>(default(TKey), null);
    }
#else
    public virtual (TKey key, TValue[] value) BtnFind(TKey key)
    {
      if (_BtnFind(ref key, out TValue[] valueOut, _btnRoot, out _btnFast) != null)
        return (key: key, value: valueOut);
      else
        return (key: default(TKey), value: null);
    }
    public virtual (TKey key, TValue[] value) BtnFastFind(TKey key, out BtnFastKeyDuplValue btnFast)
    {
      if (_BtnFind(ref key, out TValue[] valueOut, _btnRoot, out btnFast) != null)
        return (key: key, value: valueOut);
      else
        return (key: default(TKey), value: null);
    }
#endif
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////           BtnFirst           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected bool? _BtnFirst(out TKey key, out TValue[] value, out BtnFastDuplValue btnFast)
    {
      BtnDuplValuePage QQ = _btnRoot;
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
        value = null;
        btnFast = default(BtnFastDuplValue);
        return null;
      }
    }
    public virtual bool? BtnFirst(out TKey key, out TValue[] value)
    {
      return _BtnFirst(out key, out value, out _btnFast);
    }
    //////////////////////////
    public virtual bool? BtnFastFirst(out TKey key, out TValue[] value, out BtnFastDuplValue btnFast)
    {
      return _BtnFirst(out key, out value, out btnFast);
    }
    //////////////////////////
#if KEY_VALUE_PAIR
    protected KeyValuePair<TKey, TValue[]> _BtnFirst(out BtnFastDuplValue btnFast)
    {
      BtnDuplValuePage QQ = _btnRoot;
      while ((QQ != null) && (QQ.kvPageNextRight != null))
        QQ = QQ.kvPageNextRight[0];

      if (QQ != null)
      {
        btnFast.version = BtnVersion;
        btnFast.fastMiddle = 1;
        btnFast.fastPage = QQ;
        return new KeyValuePair<TKey, TValue[]>(QQ.aData[1].key, QQ.aData[1].value);
      }
      else
      {
        btnFast = default(BtnFastDuplValue);
        return new KeyValuePair<TKey, TValue[]>(default(TKey), null);
      }
    }
    public virtual KeyValuePair<TKey, TValue[]> BtnFirst()
    {
      return _BtnFirst(out _btnFast);
    }
    public virtual KeyValuePair<TKey, TValue[]> BtnFastFirst(out BtnFastDuplValue btnFast)
    {
      return _BtnFirst(out btnFast);
    }
#else
    protected (TKey key, TValue[] value) _BtnFirst(out BtnFastKeyDuplValue btnFast)
    {
      BtnKeyDuplValuePage QQ = _btnRoot;
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
        btnFast = default(BtnFastKeyDuplValue);
        return (key: default(TKey), value: null);
      }
    }
    public virtual (TKey key, TValue[] value) BtnFirst()
    {
      return _BtnFirst(out _btnFast);
    }
    public virtual (TKey key, TValue[] value) BtnFastFirst(out BtnFastKeyDuplValue btnFast)
    {
      return _BtnFirst(out btnFast);
    }
#endif
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////            BtnLast           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected bool? _BtnLast(out TKey key, out TValue[] value, out BtnFastDuplValue btnFast)
    {
      BtnDuplValuePage QQ = _btnRoot;
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
        value = null;
        btnFast = default(BtnFastDuplValue);
        return null;
      }
    }
    public virtual bool? BtnLast(out TKey key, out TValue[] value)
    {
      return _BtnLast(out key, out value, out _btnFast);
    }
    //////////////////////////
    public virtual bool? BtnFastLast(out TKey key, out TValue[] value, out BtnFastDuplValue btnFast)
    {
      return _BtnLast(out key, out value, out btnFast);
    }
    //////////////////////////
#if KEY_VALUE_PAIR
    protected KeyValuePair<TKey, TValue[]> _BtnLast(out BtnFastDuplValue btnFast)
    {
      BtnDuplValuePage QQ = _btnRoot;
      while ((QQ != null) && (QQ.kvPageNextRight != null))
        QQ = QQ.kvPageNextRight[QQ.iDataCount];

      if (QQ != null)
      {
        btnFast.version = BtnVersion;
        btnFast.fastMiddle = QQ.iDataCount;
        btnFast.fastPage = QQ;
        return new KeyValuePair<TKey, TValue[]>(QQ.aData[QQ.iDataCount].key, QQ.aData[QQ.iDataCount].value);
      }
      else
      {
        btnFast = default(BtnFastDuplValue);
        return new KeyValuePair<TKey, TValue[]>(default(TKey), null);
      }
    }
    public virtual KeyValuePair<TKey, TValue[]> BtnLast()
    {
      return _BtnLast(out _btnFast);
    }
    public virtual KeyValuePair<TKey, TValue[]> BtnFastLast(out BtnFastDuplValue btnFast)
    {
      return _BtnLast(out btnFast);
    }
#else
    protected (TKey key, TValue[] value) _BtnLast(out BtnFastKeyDuplValue btnFast)
    {
      BtnKeyDuplValuePage QQ = _btnRoot;
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
        btnFast = default(BtnFastKeyDuplValue);
        return (key: default(TKey), value: null);
      }
    }
    public virtual (TKey key, TValue[] value) BtnLast()
    {
      return _BtnLast(out _btnFast);
    }
    public virtual (TKey key, TValue[] value) BtnFastLast(out BtnFastKeyDuplValue btnFast)
    {
      return _BtnLast(out btnFast);
    }
#endif
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////            BtnNext           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected bool? _BtnNext(ref TKey key, out TValue[] value, BtnDuplValuePage kvPage, ref bool bNext, out BtnFastDuplValue btnFast)
    {
      bool? bNullResult = null;
      if (kvPage == null)
      {
        value = null;
        btnFast = default(BtnFastDuplValue);
        bNext = false;
      }
      else
      {
        int iResultCmp;
        int middle, low = 1, high = kvPage.iDataCount;
        BtnDuplValuePage QQ = null;
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
              btnFast = default(BtnFastDuplValue);
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
              value = null;
              btnFast = default(BtnFastDuplValue);
            }
          }
        }
        else
        {
          if (kvPage.kvPageNextRight == null)
            QQ = null;
          else if (high <= 0)
            QQ = kvPage.kvPageNextRight[0];          // left
          else
            QQ = kvPage.kvPageNextRight[high];       // right
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
    public virtual bool? BtnNext(ref TKey key, out TValue[] value)
    {
      bool bNext = false;
      return _BtnNext(ref key, out value, _btnRoot, ref bNext, out _btnFast);
    }
    //////////////////////////
    public virtual bool? BtnFastNext(ref TKey key, out TValue[] value, ref BtnFastDuplValue btnFast)
    {
      int middle = btnFast.fastMiddle;
      if ( (btnFast.version == BtnVersion) && (btnFast.fastPage != null) && (middle > 0) &&
           (middle < btnFast.fastPage.iDataCount) && (BtnCompares(key, btnFast.fastPage.aData[middle].key, _objCompares) == 0))
      {
        middle = ++btnFast.fastMiddle;
        key = btnFast.fastPage.aData[middle].key;
        value = btnFast.fastPage.aData[middle].value;
        return true;
      }
      else
      {
        bool bNext = false;
        return _BtnNext(ref key, out value, _btnRoot, ref bNext, out btnFast);
      }
    }
    //////////////////////////
#if KEY_VALUE_PAIR
    public virtual KeyValuePair<TKey, TValue[]> BtnNext(TKey key)
    {
      bool bNext = false;
      if (_BtnNext(ref key, out TValue[] valueOut, _btnRoot, ref bNext, out _btnFast) != null)
        return new KeyValuePair<TKey, TValue[]>(key, valueOut);
      else
        return new KeyValuePair<TKey, TValue[]>(default(TKey), null);
    }
    public virtual KeyValuePair<TKey, TValue[]> BtnFastNext(TKey key, ref BtnFastDuplValue btnFast)
    {
      int middle = btnFast.fastMiddle;
      if ( (btnFast.version == BtnVersion) && (btnFast.fastPage != null) && (middle > 0) &&
           (middle < btnFast.fastPage.iDataCount) && (BtnCompares(key, btnFast.fastPage.aData[middle].key, _objCompares) == 0))
      {
        middle = ++btnFast.fastMiddle;
        return new KeyValuePair<TKey, TValue[]>(btnFast.fastPage.aData[middle].key, btnFast.fastPage.aData[middle].value);
      }
      else
      {
        bool bNext = false;
        if (_BtnNext(ref key, out TValue[] valueOut, _btnRoot, ref bNext, out btnFast) != null)
          return new KeyValuePair<TKey, TValue[]>(key, valueOut);
        else
          return new KeyValuePair<TKey, TValue[]>(default(TKey), null);
      }
    }
#else
    public virtual (TKey key, TValue[] value) BtnNext(TKey key)
    {
      bool bNext = false;
      if (_BtnNext(ref key, out TValue[] valueOut, _btnRoot, ref bNext, out _btnFast) != null)
        return (key: key, value: valueOut);
      else
        return (key: default(TKey), value: null);
    }
    public virtual (TKey key, TValue[] value) BtnFastNext(TKey key, ref BtnFastKeyDuplValue btnFast)
    {
      int middle = btnFast.fastMiddle;
      if ( (btnFast.version == BtnVersion) && (btnFast.fastPage != null) && (middle > 0) &&
           (middle < btnFast.fastPage.iDataCount) && (BtnCompares(key, btnFast.fastPage.aData[middle].key, _objCompares) == 0))
      {
        middle = ++btnFast.fastMiddle;
        return (key: btnFast.fastPage.aData[middle].key, value: btnFast.fastPage.aData[middle].value);
      }
      else
      {
        bool bNext = false;
        if (_BtnNext(ref key, out TValue[] valueOut, _btnRoot, ref bNext, out btnFast) != null)
          return (key: key, value: valueOut);
        else
          return (key: default(TKey), value: null);
      }
    }
#endif
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////            BtnPrev           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected bool? _BtnPrev(ref TKey key, out TValue[] value, BtnDuplValuePage kvPage, ref bool bNext, out BtnFastDuplValue btnFast)
    {
      bool? bNullResult = null;
      if (kvPage == null)
      {
        value = null;
        btnFast = default(BtnFastDuplValue);
        bNext = false;
      }
      else
      {
        int iResultCmp;
        int middle, low = 1, high = kvPage.iDataCount;
        BtnDuplValuePage QQ = null;
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
            if (middle <= 0)
              QQ = kvPage.kvPageNextRight[0];
            else
              QQ = kvPage.kvPageNextRight[middle];
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
              btnFast = default(BtnFastDuplValue);
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
              value = null;
              btnFast = default(BtnFastDuplValue);
            }
          }
        }
        else
        {
          if (kvPage.kvPageNextRight == null)
            QQ = null;
          else if (high <= 0)
            QQ = kvPage.kvPageNextRight[0];        // left
          else
            QQ = kvPage.kvPageNextRight[high];       // right
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
    public virtual bool? BtnPrev(ref TKey key, out TValue[] value)
    {
      bool bNext = false;
      return _BtnPrev(ref key, out value, _btnRoot, ref bNext, out _btnFast);
    }
    //////////////////////////
    public virtual bool? BtnFastPrev(ref TKey key, out TValue[] value, ref BtnFastDuplValue btnFast)
    {
      int middle = btnFast.fastMiddle;
      if ( (btnFast.version == BtnVersion) && (btnFast.fastPage != null) && (middle > 1) &&
           (middle <= btnFast.fastPage.iDataCount) && (BtnCompares(key, btnFast.fastPage.aData[middle].key, _objCompares) == 0))
      {
        middle = --btnFast.fastMiddle;
        key = btnFast.fastPage.aData[middle].key;
        value = btnFast.fastPage.aData[middle].value;
        return true;
      }
      else
      {
        bool bNext = false;
        return _BtnPrev(ref key, out value, _btnRoot, ref bNext, out btnFast);
      }
    }
    //////////////////////////
#if KEY_VALUE_PAIR
    public virtual KeyValuePair<TKey, TValue[]> BtnPrev(TKey key)
    {
      bool bNext = false;
      if (_BtnPrev(ref key, out TValue[] valueOut, _btnRoot, ref bNext, out _btnFast) != null)
        return new KeyValuePair<TKey, TValue[]>(key, valueOut);
      else
        return new KeyValuePair<TKey, TValue[]>(default(TKey), null);
    }
    public virtual KeyValuePair<TKey, TValue[]> BtnFastPrev(TKey key, ref BtnFastDuplValue btnFast)
    {
      int middle = btnFast.fastMiddle;
      if ( (btnFast.version == BtnVersion) && (btnFast.fastPage != null) && (middle > 1) &&
           (middle <= btnFast.fastPage.iDataCount) && (BtnCompares(key, btnFast.fastPage.aData[middle].key, _objCompares) == 0))
      {
        middle = --btnFast.fastMiddle;
        return new KeyValuePair<TKey, TValue[]>(btnFast.fastPage.aData[middle].key, btnFast.fastPage.aData[middle].value);
      }
      else
      {
        bool bNext = false;
        if (_BtnPrev(ref key, out TValue[] valueOut, _btnRoot, ref bNext, out btnFast) != null)
          return new KeyValuePair<TKey, TValue[]>(key, valueOut);
        else
          return new KeyValuePair<TKey, TValue[]>(default(TKey), null);
      }
    }
#else
    public virtual (TKey key, TValue[] value) BtnPrev(TKey key)
    {
      bool bNext = false;
      if (_BtnPrev(ref key, out TValue[] valueOut, _btnRoot, ref bNext, out _btnFast) != null)
        return (key: key, value: valueOut);
      else
        return (key: default(TKey), value: null);
    }
    public virtual (TKey key, TValue[] value) BtnFastPrev(TKey key, ref BtnFastKeyDuplValue btnFast)
    {
      int middle = btnFast.fastMiddle;
      if ( (btnFast.version == BtnVersion) && (btnFast.fastPage != null) && (middle > 1) &&
           (middle <= btnFast.fastPage.iDataCount) && (BtnCompares(key, btnFast.fastPage.aData[middle].key, _objCompares) == 0))
      {
        middle = --btnFast.fastMiddle;
        return (key: btnFast.fastPage.aData[middle].key, value: btnFast.fastPage.aData[middle].value);
      }
      else
      {
        bool bNext = false;
        if (_BtnPrev(ref key, out TValue[] valueOut, _btnRoot, ref bNext, out btnFast) != null)
          return (key: key, value: valueOut);
        else
          return (key: default(TKey), value: null);
      }
    }
#endif
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////           BtnSearch          //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected bool? _BtnSearch(ref TKey key, out TValue[] value, BtnDuplValuePage kvPage, ref bool bNext, out BtnFastDuplValue btnFast)
    {
      bool? bNullResult = null;
      if (kvPage == null)
      {
        value = null;
        btnFast = default(BtnFastDuplValue);
        bNext = true;
      }
      else
      {
        int iResultCmp;
        int middle, low = 1, high = kvPage.iDataCount;
        BtnDuplValuePage QQ = null;
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
            btnFast = default(BtnFastDuplValue);
          bNext = false;
          bNullResult = new bool?(true);
        }
        else
        {
          if (kvPage.kvPageNextRight == null)
            QQ = null;
          else if (high <= 0)
            QQ = kvPage.kvPageNextRight[0];          // left
          else
            QQ = kvPage.kvPageNextRight[high];       // right
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
    public virtual bool? BtnSearch(ref TKey key, out TValue[] value)
    {
      bool bNext = false;
      return _BtnSearch(ref key, out value, _btnRoot, ref bNext, out _btnFast);
    }
    //////////////////////////
    public virtual bool? BtnFastSearch(ref TKey key, out TValue[] value, out BtnFastDuplValue btnFast)
    {
      bool bNext = false;
      return _BtnSearch(ref key, out value, _btnRoot, ref bNext, out btnFast);
    }
    //////////////////////////
#if KEY_VALUE_PAIR
    public virtual KeyValuePair<TKey, TValue[]> BtnSearch(TKey key)
    {
      bool bNext = false;
      if (_BtnSearch(ref key, out TValue[] valueOut, _btnRoot, ref bNext, out _btnFast) != null)
        return new KeyValuePair<TKey, TValue[]>(key, valueOut);
      else
        return new KeyValuePair<TKey, TValue[]>(default(TKey), null);
    }
    public virtual KeyValuePair<TKey, TValue[]> BtnFastSearch(TKey key, out BtnFastDuplValue btnFast)
    {
      bool bNext = false;
      if (_BtnSearch(ref key, out TValue[] valueOut, _btnRoot, ref bNext, out btnFast) != null)
        return new KeyValuePair<TKey, TValue[]>(key, valueOut);
      else
        return new KeyValuePair<TKey, TValue[]>(default(TKey), null);
    }
#else
    public virtual (TKey key, TValue[] value) BtnSearch(TKey key)
    {
      bool bNext = false;
      if (_BtnSearch(ref key, out TValue[] valueOut, _btnRoot, ref bNext, out _btnFast) != null)
        return (key: key, value: valueOut);
      else
        return (key: default(TKey), value: null);
    }
    public virtual (TKey key, TValue[] value) BtnFastSearch(TKey key, out BtnFastKeyDuplValue btnFast)
    {
      bool bNext = false;
      if (_BtnSearch(ref key, out TValue[] valueOut, _btnRoot, ref bNext, out btnFast) != null)
        return (key: key, value: valueOut);
      else
        return (key: default(TKey), value: null);
    }
#endif
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////         BtnSearchPrev        //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected bool? _BtnSearchPrev(ref TKey key, out TValue[] value, BtnDuplValuePage kvPage, ref bool bNext, out BtnFastDuplValue btnFast)
    {
      bool? bNullResult = null;
      if (kvPage == null)
      {
        value = null;
        btnFast = default(BtnFastDuplValue);
        bNext = true;
      }
      else
      {
        int iResultCmp;
        int middle, low = 1, high = kvPage.iDataCount;
        BtnDuplValuePage QQ = null;
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
            btnFast = default(BtnFastDuplValue);
          bNullResult = new bool?(true);
        }
        else
        {
          if (kvPage.kvPageNextRight == null)
            QQ = null;
          else if (high <= 0)
            QQ = kvPage.kvPageNextRight[0];          // left
          else
            QQ = kvPage.kvPageNextRight[high];       // right
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
    public virtual bool? BtnSearchPrev(ref TKey key, out TValue[] value)
    {
      bool bNext = false;
      return _BtnSearchPrev(ref key, out value, _btnRoot, ref bNext, out _btnFast);
    }
    //////////////////////////
    public virtual bool? BtnFastSearchPrev(ref TKey key, out TValue[] value, out BtnFastDuplValue btnFast)
    {
      bool bNext = false;
      return _BtnSearchPrev(ref key, out value, _btnRoot, ref bNext, out btnFast);
    }
    //////////////////////////
#if KEY_VALUE_PAIR
    public virtual KeyValuePair<TKey, TValue[]> BtnSearchPrev(TKey key)
    {
      bool bNext = false;
      if (_BtnSearchPrev(ref key, out TValue[] valueOut, _btnRoot, ref bNext, out _btnFast) != null)
        return new KeyValuePair<TKey, TValue[]>(key, valueOut);
      else
        return new KeyValuePair<TKey, TValue[]>(default(TKey), null);
    }
    public virtual KeyValuePair<TKey, TValue[]> BtnFastSearchPrev(TKey key, out BtnFastDuplValue btnFast)
    {
      bool bNext = false;
      if (_BtnSearchPrev(ref key, out TValue[] valueOut, _btnRoot, ref bNext, out btnFast) != null)
        return new KeyValuePair<TKey, TValue[]>(key, valueOut);
      else
        return new KeyValuePair<TKey, TValue[]>(default(TKey), null);
    }
#else
    public virtual (TKey key, TValue[] value) BtnSearchPrev(TKey key)
    {
      bool bNext = false;
      if (_BtnSearchPrev(ref key, out TValue[] valueOut, _btnRoot, ref bNext, out _btnFast) != null)
        return (key: key, value: valueOut);
      else
        return (key: default(TKey), value: null);
    }
    public virtual (TKey key, TValue[] value) BtnFastSearchPrev(TKey key, out BtnFastKeyDuplValue btnFast)
    {
      bool bNext = false;
      if (_BtnSearchPrev(ref key, out TValue[] valueOut, _btnRoot, ref bNext, out btnFast) != null)
        return (key: key, value: valueOut);
      else
        return (key: default(TKey), value: null);
    }
#endif
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////         BtnUpdate          //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    private bool? _BtnUpdate(ref BtnDuplValue kvUpdate, BtnDuplValuePage kvPage, object objUpdates)
    {
      bool? bNullResult = null;
      if (kvPage != null)
      {
        int iResultCmp;
        int middle, low = 1, high = kvPage.iDataCount;
        BtnDuplValuePage QQ = null;
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
          if (kvPage.kvPageNextRight == null)      // last level
            QQ = null;
          else if (high <= 0)
            QQ = kvPage.kvPageNextRight[0];        // left
          else
            QQ = kvPage.kvPageNextRight[high];       // right
          bNullResult = _BtnUpdate(ref kvUpdate, QQ, objUpdates);
        }
      }
      return bNullResult;
    }
    //////////////////////////
    public bool? BtnUpdate(TKey key, ref TValue[] value, object objUpdates)
    {
      BtnDuplValue keyValue;
      keyValue.key = key;
      keyValue.value = value;
      BtnDuplValuePage kvPageDown = _btnRoot;
      bool? bNullResult = null;

      bNullResult = _BtnUpdate(ref keyValue, kvPageDown, objUpdates);
      if (bNullResult != null)
        value = keyValue.value;
      else
        value = null;

      return bNullResult; // update = false, else null;
    }
    //////////////////////////
    public bool? BtnUpdate(TKey key, TValue[] value, object objUpdates)
    {
      return BtnUpdate(key, ref value, objUpdates);
    }
    //////////////////////////
    public bool? BtnUpdate(TKey key, ref TValue[] value)
    {
      return BtnUpdate(key, ref value, null);
    }
    //////////////////////////
    public bool? BtnUpdate(TKey key, TValue[] value)
    {
      return BtnUpdate(key, ref value, null);
    }
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////           BtnUsedKeys           ///////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected void _BtnUsedKeys(ref uint Count, BtnDuplValuePage QQ)
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
    public FcsDuplValueFastBTreeN<TKey, TValue>.BtnEnumerator GetEnumeratorEx(bool reverse)
    {
      return new BtnEnumerator(this, null, null, reverse, -2);
    }
    //////////////////////////
    public FcsDuplValueFastBTreeN<TKey, TValue>.BtnEnumerator GetEnumeratorEx(bool reverse, int maxCount)
    {
      return new BtnEnumerator(this, null, null, reverse, maxCount);
    }
    //////////////////////////
    public FcsDuplValueFastBTreeN<TKey, TValue>.BtnEnumerator GetEnumeratorEx(TKey? keyLo, TKey? keyHi, bool reverse)
    {
      return new BtnEnumerator(this, keyLo, keyHi, reverse, -3);
    }
    //////////////////////////
    public FcsDuplValueFastBTreeN<TKey, TValue>.BtnEnumerator GetEnumeratorEx(TKey? keyLo, TKey? keyHi, bool reverse, int maxCount)
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
    public IEnumerator<KeyValuePair<TKey, TValue[]>> GetEnumerator()
    {
      return new BtnEnumerator(this, null, null, false, -1);
    }
    //////////////////////////
    public class BtnEnumerator : IEnumerator<KeyValuePair<TKey, TValue[]>>
    {
      // constructor
      private FcsDuplValueFastBTreeN<TKey, TValue> _btn = null;
      private TKey? _keyLo;
      private TKey? _keyHi;
      private int _maxCount;
      private bool _reverse;
      // locals
      private bool _bOK;
      private TKey _key;
      private TValue[] _value;
      private int _count;
      private BtnFastDuplValue _btnFast;
      //////////////////////////
      public BtnEnumerator(FcsDuplValueFastBTreeN<TKey, TValue> btn, TKey? keyLo, TKey? keyHi, bool reverse, int maxCount)
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

        if (_btnFast.version == int.MinValue)
        {
          _btnFast = default(BtnFastDuplValue);
          if ((_keyLo == null) && (!_reverse))
            _bOK = (_btn.BtnFastFirst(out _key, out _value, out _btnFast) != null);
          else if ((_keyHi == null) && (_reverse))
            _bOK = (_btn.BtnFastLast(out _key, out _value, out _btnFast) != null);
          else
          {
            if (_reverse)
            {
              _key = _keyHi.GetValueOrDefault();
              _bOK = (_btn.BtnFastSearchPrev(ref _key, out _value, out _btnFast) != null);
              if ((_keyLo != null) && (_bOK))
                _bOK = (_key.CompareTo(_keyLo.GetValueOrDefault()) >= 0);
            }
            else
            {
              _key = _keyLo.GetValueOrDefault();
              _bOK = (_btn.BtnFastSearch(ref _key, out _value, out _btnFast) != null);
              if ((_keyHi != null) && (_bOK))
                _bOK = (_key.CompareTo(_keyHi.GetValueOrDefault()) <= 0);
            }
          }
        }
        else
        {
          if (_reverse)
          {
            _bOK = (_btn.BtnFastPrev(ref _key, out _value, ref _btnFast) != null);
            if ((_keyLo != null) && (_bOK))
              _bOK = (_key.CompareTo(_keyLo.GetValueOrDefault()) >= 0);
          }
          else
          {
            _bOK = (_btn.BtnFastNext(ref _key, out _value, ref _btnFast) != null);
            if ((_keyHi != null) && (_bOK))
              _bOK = (_key.CompareTo(_keyHi.GetValueOrDefault()) <= 0);
          }
        }
        return _bOK;
      }
      //////////////////////////
      public KeyValuePair<TKey, TValue[]> Current
      {
        get
        {
          if (!_bOK)
            _value = null;
          return new KeyValuePair<TKey, TValue[]>(_key, _value);
        }
      }
      //////////////////////////
      object IEnumerator.Current
      {
        get
        {
          if (!_bOK)
            return null;
          return new KeyValuePair<TKey, TValue[]>(_key, _value);
        }
      }
      //////////////////////////
      public void Dispose() { _btn = null; _btnFast.Dispose(); }
      //////////////////////////
      public void Reset()
      {
        _count = _maxCount;
        _bOK = true;
        _btnFast = default(BtnFastDuplValue);
        _btnFast.version = int.MinValue;
      }
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////         !IEnumerator         //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  }
}
