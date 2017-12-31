// Licensed to the KUBDAT® under one or more agreements.
// The KUBDAT® licenses this file to you under the Apache-2.0 license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections;
using System.Collections.Generic;

namespace FriendlyCSharp.Databases
{
  public class FcsKeyFastBTreeN<TKeyValue> : IEnumerable<TKeyValue>
  {
    protected const int _btnDefaultBTreeN = 32;
    //////////////////////////
    private object _btnLockAdd;
    protected object LockAdd { get => _btnLockAdd; set => _btnLockAdd = value; }
    //////////////////////////
    private KeyPage _btnRoot = null;
    protected KeyPage Root { get => _btnRoot; set => _btnRoot = value; }
    //////////////////////////
    protected KeyFast _btnFast;
    protected object _objCmp = null;
    protected Func<TKeyValue, TKeyValue, object, object, int> _funcCmp;
    protected object _objUp = null;
    protected KeyUpdateFunc _funcUp;
    public delegate bool KeyUpdateFunc(TKeyValue keyAdd, ref TKeyValue keyUpdate, object objUpdate);
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
    public struct KeyFast : IDisposable
    {
      internal int version;
      internal int fastMiddle;
      internal KeyPage fastPage;
      //////////////////////////
      public int Version { get => version; set => version = 0; } 
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
    protected internal class KeyPage : IDisposable, ICloneable
    {
      public UInt32 flags;
      public bool bUpdatedValue;
      public int iDataCount;
      public TKeyValue[] aData;
      public KeyPage[] kvPageNextRight;
      //////////////////////////
      public KeyPage(int iPageN, bool bPageNext)
      {
        bUpdatedValue = false;
        flags = 0;
        iDataCount = 0;
        aData = new TKeyValue[(iPageN * 2) + 1];
        if (bPageNext)
          kvPageNextRight = new KeyPage[(iPageN * 2) + 1];
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
        KeyPage root = (KeyPage)this.MemberwiseClone();
        aData = (TKeyValue[])aData.Clone();
        if (kvPageNextRight != null)
        {
          for(int ii = 0; ii <= root.iDataCount; ii++)
          {
            if (root.kvPageNextRight[ii] != null)
              root.kvPageNextRight[ii] = (KeyPage)root.kvPageNextRight[ii].Clone();
          }
        }
        return root;
      }
    }
    //////////////////////////
    public FcsKeyFastBTreeN(Func<TKeyValue, TKeyValue, object, object, int> funcCmp) : this(funcCmp, null, null, null, _btnDefaultBTreeN)
    {
    }
    //////////////////////////
    public FcsKeyFastBTreeN(Func<TKeyValue, TKeyValue, object, object, int> funcCmp, int btnBTreeN) : this(funcCmp, null, null, null, btnBTreeN)
    {
    }
    //////////////////////////
    public FcsKeyFastBTreeN(Func<TKeyValue, TKeyValue, object, object, int> funcCmp, KeyUpdateFunc funcUp) : this(funcCmp, null, funcUp, null, _btnDefaultBTreeN)
    {
    }
    //////////////////////////
    public FcsKeyFastBTreeN(Func<TKeyValue, TKeyValue, object, object, int> funcCmp, KeyUpdateFunc funcUp, object objUp) : this(funcCmp, null, funcUp, objUp, _btnDefaultBTreeN)
    {
    }
    //////////////////////////
    public FcsKeyFastBTreeN(Func<TKeyValue, TKeyValue, object, object, int> funcCmp, KeyUpdateFunc funcUp, int btnBTreeN) : this(funcCmp, null, funcUp, null, btnBTreeN)
    {
    }
    //////////////////////////
    public FcsKeyFastBTreeN(Func<TKeyValue, TKeyValue, object, object, int> funcCmp, KeyUpdateFunc funcUp, object objUp, int btnBTreeN) : this(funcCmp, null, funcUp, objUp, btnBTreeN)
    {
    }
    //////////////////////////
    public FcsKeyFastBTreeN(Func<TKeyValue, TKeyValue, object, object, int> funcCmp, object objCmp) : this(funcCmp, objCmp, null, null, _btnDefaultBTreeN)
    {
    }
    //////////////////////////
    public FcsKeyFastBTreeN(Func<TKeyValue, TKeyValue, object, object, int> funcCmp, object objCmp, int btnBTreeN) : this(funcCmp, objCmp, null, null, btnBTreeN)
    {
    }
    //////////////////////////
    public FcsKeyFastBTreeN(Func<TKeyValue, TKeyValue, object, object, int> funcCmp, object objCmp, KeyUpdateFunc funcUp) : this(funcCmp, objCmp, funcUp, null, _btnDefaultBTreeN)
    {
    }
    //////////////////////////
    public FcsKeyFastBTreeN(Func<TKeyValue, TKeyValue, object, object, int> funcCmp, object objCmp, KeyUpdateFunc funcUp, object objUp) : this(funcCmp, objCmp, funcUp, objUp, _btnDefaultBTreeN)
    {
    }
    //////////////////////////
    public FcsKeyFastBTreeN(Func<TKeyValue, TKeyValue, object, object, int> funcCmp, object objCmp, KeyUpdateFunc funcUp, object objUp, int btnBTreeN)
    {
      if ((btnBTreeN <= 0) || (btnBTreeN > _btnMaxBTreeN))
        throw new ArgumentOutOfRangeException();
      _btnUpdatedRoot = false;
      _btnVersion = 0;
      _btnVersionPage = 0;
      _btnFast = default(KeyFast);
      _btnLockAdd = new object();
      _btnRoot = null;
      _btnBTreeN = btnBTreeN;
      _objCmp = objCmp;
      _funcCmp = funcCmp;
      _objUp = objUp;
      _funcUp = funcUp;
    }
    //////////////////////////
    public TKeyValue this[TKeyValue key]
    {
      get
      {
        TKeyValue keyL = key;
        if (BtnFind(ref keyL) == true)
          return keyL;
        return default(TKeyValue);
      }
      set
      {
        BtnAdd(key);
      }
    }
    //////////////////////////
    public TKeyValue this[TKeyValue key, bool bNext]
    {
      get
      {
        if (!bNext)
          return BtnSearch(key);
        return BtnNext(key);
      }
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////           BtnAdd           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    private bool? _BtnAdd(ref TKeyValue keyAdd, out TKeyValue keyUp, ref KeyPage keyPageUp, ref bool bUp)
    {
      bool? bNullResult = null;
      if (keyPageUp == null)
      {  //  Pridani
        bUp = true;
        keyUp = keyAdd;
      }
      else
      {
        int iResultCmp;
        int middle, low = 1, high = keyPageUp.iDataCount;
        KeyPage QQ = null;
        // binary search
        do
        {
          middle = (low + high);
          middle >>= 1;   // deleno 2
          iResultCmp = _funcCmp(keyAdd, keyPageUp.aData[middle], _objCmp, null);
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
          keyUp = default(TKeyValue);
          // bVyslNull je pri update = false, jinak null - viz predchozi radek; add = true - viz vyse
          if ((_funcUp != null) && (_funcUp(keyAdd, ref keyPageUp.aData[middle], _objUp)))
            keyPageUp.bUpdatedValue = true;
          keyAdd = keyPageUp.aData[middle];
          bNullResult = new bool?(false);
        }
        else
        {
          if (keyPageUp.kvPageNextRight == null)      // last level
            QQ = null;
          else if (high > 0)
            QQ = keyPageUp.kvPageNextRight[high];     // right
          else
            QQ = keyPageUp.kvPageNextRight[0];        // left
          bNullResult = _BtnAdd(ref keyAdd, out keyUp, ref QQ, ref bUp);
          if (bUp)
          {
            if (keyPageUp.iDataCount < (BtnBTreeN * 2))     // pridej kvAdd do prave vetve, je volne misto
            {
              _btnVersion++;
              if (keyPageUp.kvPageNextRight != null)
                _btnUpdatedRoot = true;
              else
                keyPageUp.bUpdatedValue = true;
              bUp = false;
              for (int ii = keyPageUp.iDataCount + 1; ii > high + 1; ii--)
              {
                keyPageUp.aData[ii] = keyPageUp.aData[ii - 1];
                if (keyPageUp.kvPageNextRight != null)
                  keyPageUp.kvPageNextRight[ii] = keyPageUp.kvPageNextRight[ii - 1];
              }
              keyPageUp.aData[high + 1] = keyUp;
              if (QQ != null)
              {
                if (high > 0)
                  keyPageUp.kvPageNextRight[high + 1] = QQ;
                else
                  keyPageUp.kvPageNextRight[1] = QQ;
              }
              keyPageUp.iDataCount++;
              bNullResult = new bool?(true);
            }
            else
            {
              _btnVersionPage++;
              _btnUpdatedRoot = true;
              KeyPage kvPageRight = null;
              TKeyValue kvUpLocal;
              KeyPage kvPageNew = new KeyPage(BtnBTreeN, keyPageUp.kvPageNextRight != null);
              if (high <= BtnBTreeN)
              {
                if (keyPageUp.kvPageNextRight != null)
                  kvPageRight = keyPageUp.kvPageNextRight[BtnBTreeN];
                if (high == BtnBTreeN)
                  kvUpLocal = keyUp;
                else
                {
                  kvUpLocal = keyPageUp.aData[BtnBTreeN];
                  for (int ii = BtnBTreeN; ii >= high + 2; ii--)
                  {
                    keyPageUp.aData[ii] = keyPageUp.aData[ii - 1];
                    if (keyPageUp.kvPageNextRight != null)
                      keyPageUp.kvPageNextRight[ii] = keyPageUp.kvPageNextRight[ii - 1];
                  }
                  keyPageUp.aData[high + 1] = keyUp;
                }
                for (int ii = 1; ii <= BtnBTreeN; ii++)
                {
                  kvPageNew.aData[ii] = keyPageUp.aData[ii + BtnBTreeN];
                  if (kvPageNew.kvPageNextRight != null)
                    kvPageNew.kvPageNextRight[ii] = keyPageUp.kvPageNextRight[ii + BtnBTreeN];
                }
                if (keyPageUp.kvPageNextRight != null)
                {
                  if (high < BtnBTreeN)
                  {
                    kvPageNew.kvPageNextRight[0] = kvPageRight;
                    keyPageUp.kvPageNextRight[high + 1] = QQ;
                  }
                  else
                    kvPageNew.kvPageNextRight[0] = QQ;
                }
              }
              else
              {
                high -= BtnBTreeN;
                kvUpLocal = keyPageUp.aData[BtnBTreeN + 1];
                if (keyPageUp.kvPageNextRight != null)
                  kvPageRight = keyPageUp.kvPageNextRight[BtnBTreeN + 1];
                for (int ii = 1; ii < high; ii++)
                {
                  kvPageNew.aData[ii] = keyPageUp.aData[ii + BtnBTreeN + 1];
                  if (kvPageNew.kvPageNextRight != null)
                    kvPageNew.kvPageNextRight[ii] = keyPageUp.kvPageNextRight[ii + BtnBTreeN + 1];
                }
                kvPageNew.aData[high] = keyUp;
                for (int ii = high + 1; ii <= BtnBTreeN; ii++)
                {
                  kvPageNew.aData[ii] = keyPageUp.aData[ii + BtnBTreeN];
                  if (kvPageNew.kvPageNextRight != null)
                    kvPageNew.kvPageNextRight[ii] = keyPageUp.kvPageNextRight[ii + BtnBTreeN];
                }
                if (kvPageNew.kvPageNextRight != null)
                {
                  kvPageNew.kvPageNextRight[0] = kvPageRight;
                  kvPageNew.kvPageNextRight[high] = QQ;
                }
              }
              keyUp = kvUpLocal;

              keyPageUp.iDataCount = BtnBTreeN;
              kvPageNew.iDataCount = BtnBTreeN;
              keyPageUp = kvPageNew;
            }
          }
        }
      }
      return bNullResult;
    }
    //////////////////////////
    public bool? BtnAddNoLock(ref TKeyValue key)
    {
      bool bUp = false;
      KeyPage keyPageDown = _btnRoot;
      bool? bNullResult = null;

      bNullResult = _BtnAdd(ref key, out TKeyValue keyUp, ref keyPageDown, ref bUp);
      if (bUp)
      {
        bUp = false;
        _btnVersion++;
        _btnVersionPage++;
        _btnUpdatedRoot = true;
        KeyPage QQ = new KeyPage(BtnBTreeN, keyPageDown != null) { iDataCount = 1 };
        QQ.aData[1] = keyUp;
        if (QQ.kvPageNextRight != null)
        {
          QQ.kvPageNextRight[0] = _btnRoot;
          QQ.kvPageNextRight[1] = keyPageDown;
        }
        _btnRoot = QQ;
        bNullResult = new bool?(true);
      }
      return bNullResult; // add = true, update = false, else null;
    }
    //////////////////////////
    public bool? BtnAdd(ref TKeyValue key)
    {
      lock (_btnLockAdd)
      {
        return BtnAddNoLock(ref key);
      }
    }
    //////////////////////////
    public bool? BtnAdd(TKeyValue key)
    {
      lock (_btnLockAdd)
      {
        return BtnAddNoLock(ref key);
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
        _btnFast.Dispose();
        if (bRunGC)
          GC.GetTotalMemory(true);
      }
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////            BtnFind           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected bool? _BtnFind(ref TKeyValue key, KeyPage keyPage, out KeyFast btnFast, object findCmp)
    {
      bool? bNullResult = null;
      if (keyPage == null)
        btnFast = default(KeyFast);
      else
      {
        int iResultCmp;
        int middle, low = 1, high = keyPage.iDataCount;
        KeyPage QQ = null;
        // binary search
        do
        {
          middle = (low + high);
          middle >>= 1;   // deleno 2
          iResultCmp = _funcCmp(key, keyPage.aData[middle], _objCmp, findCmp);
          if (iResultCmp < 0)
            high = middle - 1;
          else if (iResultCmp > 0)
            low = middle + 1;
        }
        while ((high >= low) && (iResultCmp != 0));
        // zpracuj vysledek
        if (iResultCmp == 0)
        {
          key = keyPage.aData[middle];
          if (keyPage.kvPageNextRight == null)
          {
            btnFast.version = BtnVersion;
            btnFast.fastMiddle = middle;
            btnFast.fastPage = keyPage;
          }
          else
            btnFast = default(KeyFast);
          bNullResult = new bool?(true);
        }
        if (keyPage.kvPageNextRight == null)
          QQ = null;
        else if (high > 0)
          QQ = keyPage.kvPageNextRight[high];         // right
        else
          QQ = keyPage.kvPageNextRight[0];            // left
        bNullResult = _BtnFind(ref key, QQ, out btnFast, findCmp);
      }
      return bNullResult;
    }
    //////////////////////////
    public virtual bool? BtnFind(ref TKeyValue key)
    {
      return _BtnFind(ref key, _btnRoot, out _btnFast, null);
    }
    //////////////////////////
    public virtual bool? BtnFastFind(ref TKeyValue key, out KeyFast btnFast)
    {
      return _BtnFind(ref key, _btnRoot, out btnFast, null);
    }
    ////////////////////////// 
    public virtual TKeyValue BtnFind(TKeyValue key)
    {
      if (_BtnFind(ref key, _btnRoot, out _btnFast, null) != null)
        return key;
      else
        return default(TKeyValue);
    }
    ////////////////////////// 
    public virtual TKeyValue BtnFastFind(TKeyValue key, out KeyFast btnFast)
    {
      if (_BtnFind(ref key, _btnRoot, out btnFast, null) != null)
        return key;
      else
        return default(TKeyValue);
    }
    ////////////////////////// 
    public virtual bool? BtnFind(object findCmp, ref TKeyValue key)
    {
      return _BtnFind(ref key, _btnRoot, out _btnFast, findCmp);
    }
    ////////////////////////// 
    public virtual bool? BtnFastFind(object findCmp, ref TKeyValue key, out KeyFast btnFast)
    {
      return _BtnFind(ref key, _btnRoot, out btnFast, findCmp);
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////           BtnFirst           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected bool? _BtnFirst(out TKeyValue key, out KeyFast btnFast)
    {
      KeyPage QQ = _btnRoot;
      while ((QQ != null) && (QQ.kvPageNextRight != null))
        QQ = QQ.kvPageNextRight[0];

      if (QQ != null)
      {
        key = QQ.aData[1];
        btnFast.version = BtnVersion;
        btnFast.fastMiddle = 1;
        btnFast.fastPage = QQ;
        return new bool?(true);
      }
      else
      {
        key = default(TKeyValue);
        btnFast = default(KeyFast);
        return null;
      }
    }
    public virtual bool? BtnFirst(out TKeyValue key)
    {
      return _BtnFirst(out key, out _btnFast);
    }
    //////////////////////////
    public virtual bool? BtnFastFirst(out TKeyValue key, out KeyFast btnFast)
    {
      return _BtnFirst(out key, out btnFast);
    }
    //////////////////////////
    protected TKeyValue _BtnFirst(out KeyFast btnFast)
    {
      KeyPage QQ = _btnRoot;
      while ((QQ != null) && (QQ.kvPageNextRight != null))
        QQ = QQ.kvPageNextRight[0];

      if (QQ != null)
      {
        btnFast.version = BtnVersion;
        btnFast.fastMiddle = 1;
        btnFast.fastPage = QQ;
        return QQ.aData[1];
      }
      else
      {
        btnFast = default(KeyFast);
        return default(TKeyValue);
      }
    }
    public virtual TKeyValue BtnFirst()
    {
      return _BtnFirst(out _btnFast);
    }
    public virtual TKeyValue BtnFastFirst(out KeyFast btnFast)
    {
      return _BtnFirst(out btnFast);
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////            BtnLast           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected bool? _BtnLast(out TKeyValue key, out KeyFast btnFast)
    {
      KeyPage QQ = _btnRoot;
      while ((QQ != null) && (QQ.kvPageNextRight != null))
        QQ = QQ.kvPageNextRight[QQ.iDataCount];

      if (QQ != null)
      {
        key = QQ.aData[QQ.iDataCount];
        btnFast.version = BtnVersion;
        btnFast.fastMiddle = QQ.iDataCount;
        btnFast.fastPage = QQ;
        return new bool?(true);
      }
      else
      {
        key = default(TKeyValue);
        btnFast = default(KeyFast);
        return null;
      }
    }
    public virtual bool? BtnLast(out TKeyValue key)
    {
      return _BtnLast(out key, out _btnFast);
    }
    //////////////////////////
    public virtual bool? BtnFastLast(out TKeyValue key, out KeyFast btnFast)
    {
      return _BtnLast(out key, out btnFast);
    }
    //////////////////////////
    protected TKeyValue _BtnLast(out KeyFast btnFast)
    {
      KeyPage QQ = _btnRoot;
      while ((QQ != null) && (QQ.kvPageNextRight != null))
        QQ = QQ.kvPageNextRight[QQ.iDataCount];

      if (QQ != null)
      {
        btnFast.version = BtnVersion;
        btnFast.fastMiddle = QQ.iDataCount;
        btnFast.fastPage = QQ;
        return QQ.aData[QQ.iDataCount];
      }
      else
      {
        btnFast = default(KeyFast);
        return default(TKeyValue);
      }
    }
    public virtual TKeyValue BtnLast()
    {
      return _BtnLast(out _btnFast);
    }
    public virtual TKeyValue BtnFastLast(out KeyFast btnFast)
    {
      return _BtnLast(out btnFast);
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////            BtnNext           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected bool? _BtnNext(ref TKeyValue key, KeyPage keyPage, ref bool bNext, out KeyFast btnFast)
    {
      bool? bNullResult = null;
      if (keyPage == null)
      {
        btnFast = default(KeyFast);
        bNext = false;
      }
      else
      {
        int iResultCmp;
        int middle, low = 1, high = keyPage.iDataCount;
        KeyPage QQ = null;
        // binary search
        do
        {
          middle = (low + high);
          middle >>= 1;   // deleno 2
          iResultCmp = _funcCmp(key, keyPage.aData[middle], _objCmp, null);
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
          if (keyPage.kvPageNextRight != null)
          {
            QQ = keyPage.kvPageNextRight[middle];
            while ((QQ != null) && (QQ.kvPageNextRight != null))
              QQ = QQ.kvPageNextRight[0];
            key = QQ.aData[1];
            if (QQ.kvPageNextRight == null)
            {
              btnFast.version = BtnVersion;
              btnFast.fastMiddle = 1;
              btnFast.fastPage = QQ;
            }
            else
              btnFast = default(KeyFast);
            bNullResult = new bool?(true);
          }
          else
          {
            if (middle < keyPage.iDataCount)
            {
              key = keyPage.aData[middle + 1];
              btnFast.version = BtnVersion;
              btnFast.fastMiddle = middle + 1;
              btnFast.fastPage = keyPage;
              bNullResult = new bool?(true);
            }
            else
            {
              bNext = true;
              btnFast = default(KeyFast);
            }
          }
        }
        else
        {
          if (keyPage.kvPageNextRight == null)
            QQ = null;
          else if (high > 0)
            QQ = keyPage.kvPageNextRight[high];       // right
          else
            QQ = keyPage.kvPageNextRight[0];          // left
          bNullResult = _BtnNext(ref key, QQ, ref bNext, out btnFast);
          if ((bNext == true) && ((iResultCmp < 0) || (middle < keyPage.iDataCount)))
          {
            if ((iResultCmp > 0) && (middle < keyPage.iDataCount))
              middle++;
            key = keyPage.aData[middle];
            if (keyPage.kvPageNextRight == null)
            {
              btnFast.version = BtnVersion;
              btnFast.fastMiddle = middle;
              btnFast.fastPage = keyPage;
            }
            bNext = false;
            bNullResult = new bool?(true);
          }
        }
      }
      return bNullResult;
    }
    //////////////////////////
    public virtual bool? BtnNext(ref TKeyValue key)
    {
      bool bNext = false;
      return _BtnNext(ref key, _btnRoot, ref bNext, out _btnFast);
    }
    //////////////////////////
    public virtual bool? BtnFastNext(ref TKeyValue key, ref KeyFast btnFast)
    {
      int middle = btnFast.fastMiddle;
      if ( (btnFast.version == BtnVersion) && (btnFast.fastPage != null) && (middle > 0) && (middle < btnFast.fastPage.iDataCount))
      {
        key = btnFast.fastPage.aData[++btnFast.fastMiddle];
        return true;
      }
      else
      {
        bool bNext = false;
        return _BtnNext(ref key, _btnRoot, ref bNext, out btnFast);
      }
    }
    //////////////////////////
    public virtual TKeyValue BtnNext(TKeyValue key)
    {
      bool bNext = false;
      if (_BtnNext(ref key, _btnRoot, ref bNext, out _btnFast) != null)
        return key;
      else
        return default(TKeyValue);
    }
    public virtual TKeyValue BtnFastNext(TKeyValue key, ref KeyFast btnFast)
    {
      int middle = btnFast.fastMiddle;
      if ( (btnFast.version == BtnVersion) && (btnFast.fastPage != null) && (middle > 0) && (middle < btnFast.fastPage.iDataCount))
        return btnFast.fastPage.aData[++btnFast.fastMiddle];
      else
      {
        bool bNext = false;
        if (_BtnNext(ref key, _btnRoot, ref bNext, out btnFast) != null)
          return key;
        else
          return default(TKeyValue);
      }
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////            BtnPrev           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected bool? _BtnPrev(ref TKeyValue key, KeyPage keyPage, ref bool bNext, out KeyFast btnFast)
    {
      bool? bNullResult = null;
      if (keyPage == null)
      {
        btnFast = default(KeyFast);
        bNext = false;
      }
      else
      {
        int iResultCmp;
        int middle, low = 1, high = keyPage.iDataCount;
        KeyPage QQ = null;
        // binary search
        do
        {
          middle = (low + high);
          middle >>= 1;   // deleno 2
          iResultCmp = _funcCmp(key, keyPage.aData[middle], _objCmp, null);
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
          if (keyPage.kvPageNextRight != null)
          {
            middle--;
            if (middle > 0)
              QQ = keyPage.kvPageNextRight[middle];
            else
              QQ = keyPage.kvPageNextRight[0];
            while ((QQ != null) && (QQ.kvPageNextRight != null))
              QQ = QQ.kvPageNextRight[QQ.iDataCount];
            key = QQ.aData[QQ.iDataCount];
            if (QQ.kvPageNextRight == null)
            {
              btnFast.version = BtnVersion;
              btnFast.fastMiddle = QQ.iDataCount;
              btnFast.fastPage = QQ;
            }
            else
              btnFast = default(KeyFast);
            bNullResult = new bool?(true);
          }
          else
          {
            if (middle > 1)
            {
              key = keyPage.aData[middle - 1];
              btnFast.version = BtnVersion;
              btnFast.fastMiddle = middle - 1;
              btnFast.fastPage = keyPage;
              bNullResult = new bool?(true);
            }
            else
            {
              bNext = true;
              btnFast = default(KeyFast);
            }
          }
        }
        else
        {
          if (keyPage.kvPageNextRight == null)
            QQ = null;
          else if (high > 0)
            QQ = keyPage.kvPageNextRight[high];       // right
          else
            QQ = keyPage.kvPageNextRight[0];          // left
          bNullResult = _BtnPrev(ref key, QQ, ref bNext, out btnFast);
          if ((bNext == true) && ((iResultCmp >= 0) || (middle > 1)))
          {
            if ((iResultCmp < 0) && (middle > 1))
              middle--;
            key = keyPage.aData[middle];
            if (keyPage.kvPageNextRight == null)
            {
              btnFast.version = BtnVersion;
              btnFast.fastMiddle = middle;
              btnFast.fastPage = keyPage;
            }
            bNext = false;
            bNullResult = new bool?(true);
          }
        }
      }
      return bNullResult;
    }
    //////////////////////////
    public virtual bool? BtnPrev(ref TKeyValue key)
    {
      bool bNext = false;
      return _BtnPrev(ref key, _btnRoot, ref bNext, out _btnFast);
    }
    //////////////////////////
    public virtual bool? BtnFastPrev(ref TKeyValue key, ref KeyFast btnFast)
    {
      int middle = btnFast.fastMiddle;
      if ( (btnFast.version == BtnVersion) && (btnFast.fastPage != null) && (middle > 1) && (middle <= btnFast.fastPage.iDataCount))
      {
        key = btnFast.fastPage.aData[--btnFast.fastMiddle];
        return true;
      }
      else
      {
        bool bNext = false;
        return _BtnPrev(ref key, _btnRoot, ref bNext, out btnFast);
      }
    }
    //////////////////////////
    public virtual TKeyValue BtnPrev(TKeyValue key)
    {
      bool bNext = false;
      if (_BtnPrev(ref key, _btnRoot, ref bNext, out _btnFast) != null)
        return key;
      else
        return default(TKeyValue);
    }
    public virtual TKeyValue BtnFastPrev(TKeyValue key, ref KeyFast btnFast)
    {
      int middle = btnFast.fastMiddle;
      if ( (btnFast.version == BtnVersion) && (btnFast.fastPage != null) && (middle > 1) && (middle <= btnFast.fastPage.iDataCount))
      {
        return btnFast.fastPage.aData[--btnFast.fastMiddle];
      }
      else
      {
        bool bNext = false;
        if (_BtnPrev(ref key, _btnRoot, ref bNext, out btnFast) != null)
          return key;
        else
          return default(TKeyValue);
      }
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////           BtnSearch          //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected bool? _BtnSearch(ref TKeyValue key, KeyPage keyPage, ref bool bNext, out KeyFast btnFast, object searchCmp)
    {
      bool? bNullResult = null;
      if (keyPage == null)
      {
        btnFast = default(KeyFast);
        bNext = true;
      }
      else
      {
        int iResultCmp;
        int middle, low = 1, high = keyPage.iDataCount;
        KeyPage QQ = null;
        // binary search
        do
        {
          middle = (low + high);
          middle >>= 1;   // deleno 2
          iResultCmp = _funcCmp(key, keyPage.aData[middle], _objCmp, searchCmp);
          if (iResultCmp < 0)
            high = middle - 1;
          else if (iResultCmp > 0)
            low = middle + 1;
        }
        while ((high >= low) && (iResultCmp != 0));
        // zpracuj vysledek
        if (iResultCmp == 0)
        {
          key = keyPage.aData[middle];
          if (keyPage.kvPageNextRight == null)
          {
            btnFast.version = BtnVersion;
            btnFast.fastMiddle = middle;
            btnFast.fastPage = keyPage;
          }
          else
            btnFast = default(KeyFast);
          bNext = false;
          bNullResult = new bool?(true);
        }
        else
        {
          if (keyPage.kvPageNextRight == null)
            QQ = null;
          else if (high > 0)
            QQ = keyPage.kvPageNextRight[high];       // right
          else
            QQ = keyPage.kvPageNextRight[0];          // left
          bNullResult = _BtnSearch(ref key, QQ, ref bNext, out btnFast, searchCmp);
          if ((bNext == true) && ((iResultCmp < 0) || (middle < keyPage.iDataCount)))
          {
            if ((iResultCmp > 0) && (middle < keyPage.iDataCount))
              middle++;
            key = keyPage.aData[middle];
            if (keyPage.kvPageNextRight == null)
            {
              btnFast.version = BtnVersion;
              btnFast.fastMiddle = middle;
              btnFast.fastPage = keyPage;
            }
            bNext = false;
            bNullResult = new bool?(false);
          }
        }
      }
      return bNullResult;
    }
    //////////////////////////
    public virtual bool? BtnSearch(ref TKeyValue key)
    {
      bool bNext = false;
      return _BtnSearch(ref key, _btnRoot, ref bNext, out _btnFast, null);
    }
    //////////////////////////
    public virtual bool? BtnFastSearch(ref TKeyValue key, out KeyFast btnFast)
    {
      bool bNext = false;
      return _BtnSearch(ref key, _btnRoot, ref bNext, out btnFast, null);
    }
    //////////////////////////
    public virtual TKeyValue BtnSearch(TKeyValue key)
    {
      bool bNext = false;
      if (_BtnSearch(ref key, _btnRoot, ref bNext, out _btnFast, null) != null)
        return key;
      else
        return default(TKeyValue);
    }
    public virtual TKeyValue BtnFastSearch(TKeyValue key, out KeyFast btnFast)
    {
      bool bNext = false;
      if (_BtnSearch(ref key, _btnRoot, ref bNext, out btnFast, null) != null)
        return key;
      else
        return default(TKeyValue);
    }
    //////////////////////////
    public virtual bool? BtnSearch(object searchCmp, ref TKeyValue key)
    {
      bool bNext = false;
      return _BtnSearch(ref key, _btnRoot, ref bNext, out _btnFast, searchCmp);
    }
    //////////////////////////
    public virtual bool? BtnFastSearch(object searchCmp, ref TKeyValue key, out KeyFast btnFast)
    {
      bool bNext = false;
      return _BtnSearch(ref key, _btnRoot, ref bNext, out btnFast, searchCmp);
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////         BtnSearchPrev        //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected bool? _BtnSearchPrev(ref TKeyValue key, KeyPage keyPage, ref bool bNext, out KeyFast btnFast, object searchCmp)
    {
      bool? bNullResult = null;
      if (keyPage == null)
      {
        btnFast = default(KeyFast);
        bNext = true;
      }
      else
      {
        int iResultCmp;
        int middle, low = 1, high = keyPage.iDataCount;
        KeyPage QQ = null;
        // binary search
        do
        {
          middle = (low + high);
          middle >>= 1;   // deleno 2
          iResultCmp = _funcCmp(key, keyPage.aData[middle], _objCmp, searchCmp);
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
          key = keyPage.aData[middle];
          if (keyPage.kvPageNextRight == null)
          {
            btnFast.version = BtnVersion;
            btnFast.fastMiddle = middle;
            btnFast.fastPage = keyPage;
          }
          else
            btnFast = default(KeyFast);
          bNullResult = new bool?(true);
        }
        else
        {
          if (keyPage.kvPageNextRight == null)
            QQ = null;
          else if (high > 0)
            QQ = keyPage.kvPageNextRight[high];       // right
          else
            QQ = keyPage.kvPageNextRight[0];          // left
          bNullResult = _BtnSearchPrev(ref key, QQ, ref bNext, out btnFast, searchCmp);
          if ((bNext == true) && ((iResultCmp >= 0) || (middle > 1)))
          {
            if ((iResultCmp < 0) && (middle > 1))
              middle--;
            key = keyPage.aData[middle];
            if (keyPage.kvPageNextRight == null)
            {
              btnFast.version = BtnVersion;
              btnFast.fastMiddle = middle;
              btnFast.fastPage = keyPage;
            }
            bNext = false;
            bNullResult = new bool?(false);
          }
        }
      }
      return bNullResult;
    }
    //////////////////////////
    public virtual bool? BtnSearchPrev(ref TKeyValue key)
    {
      bool bNext = false;
      return _BtnSearchPrev(ref key, _btnRoot, ref bNext, out _btnFast, null);
    }
    //////////////////////////
    public virtual bool? BtnFastSearchPrev(ref TKeyValue key, out KeyFast btnFast)
    {
      bool bNext = false;
      return _BtnSearchPrev(ref key, _btnRoot, ref bNext, out btnFast, null);
    }
    //////////////////////////
    public virtual TKeyValue BtnSearchPrev(TKeyValue key)
    {
      bool bNext = false;
      if (_BtnSearchPrev(ref key, _btnRoot, ref bNext, out _btnFast, null) != null)
        return key;
      else
        return default(TKeyValue);
    }
    public virtual TKeyValue BtnFastSearchPrev(TKeyValue key, out KeyFast btnFast)
    {
      bool bNext = false;
      if (_BtnSearchPrev(ref key, _btnRoot, ref bNext, out btnFast, null) != null)
        return key;
      else
        return default(TKeyValue);
    }
    //////////////////////////
    public virtual bool? BtnSearchPrev(object searchCmp, ref TKeyValue key)
    {
      bool bNext = false;
      return _BtnSearchPrev(ref key, _btnRoot, ref bNext, out _btnFast, searchCmp);
    }
    //////////////////////////
    public virtual bool? BtnFastSearchPrev(object searchCmp, ref TKeyValue key, out KeyFast btnFast)
    {
      bool bNext = false;
      return _BtnSearchPrev(ref key, _btnRoot, ref bNext, out btnFast, searchCmp) != null;
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////         BtnUpdate          //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    private bool? _BtnUpdate(ref TKeyValue keyUpdate, KeyPage keyPage)
    {
      bool? bNullResult = null;
      if (keyPage != null)
      {
        int iResultCmp;
        int middle, low = 1, high = keyPage.iDataCount;
        KeyPage QQ = null;
        // binary search
        do
        {
          middle = (low + high);
          middle >>= 1;   // deleno 2
          iResultCmp = _funcCmp(keyUpdate, keyPage.aData[middle], _objCmp, null);
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
          if ((_funcUp != null) && (_funcUp(keyUpdate, ref keyPage.aData[middle], _objUp)))
            keyPage.bUpdatedValue = true;
          keyUpdate = keyPage.aData[middle];
          bNullResult = new bool?(false);
        }
        else
        {
          if (keyPage.kvPageNextRight == null)        // last level
            QQ = null;
          else if (high > 0)
            QQ = keyPage.kvPageNextRight[high];       // right
          else
            QQ = keyPage.kvPageNextRight[0];          // left
          bNullResult = _BtnUpdate(ref keyUpdate, QQ);
        }
      }
      return bNullResult;
    }
    //////////////////////////
    public bool? BtnUpdate(ref TKeyValue key)
    {
      // update = false, else null;
      return _BtnUpdate(ref key, _btnRoot);
    }
    //////////////////////////
    public bool? BtnUpdate(TKeyValue key)
    {
      // update = false, else null;
      return _BtnUpdate(ref key, _btnRoot);
    }
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////           BtnUsedKeys           ///////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected void _BtnUsedKeys(ref uint Count, KeyPage QQ)
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
    public FcsKeyFastBTreeN<TKeyValue>.KeyEnumerator GetEnumeratorFastEx(bool reverse)
    {
      return new KeyEnumerator(this, default(TKeyValue), default(TKeyValue), reverse, -2);
    }
    //////////////////////////
    public FcsKeyFastBTreeN<TKeyValue>.KeyEnumerator GetEnumeratorFastEx(bool reverse, int maxCount)
    {
      return new KeyEnumerator(this, default(TKeyValue), default(TKeyValue), reverse, maxCount);
    }
    //////////////////////////
    public FcsKeyFastBTreeN<TKeyValue>.KeyEnumerator GetEnumeratorFastEx(TKeyValue keyLo, TKeyValue keyHi, bool reverse)
    {
      return new KeyEnumerator(this, keyLo, keyHi, reverse, -3);
    }
    //////////////////////////
    public FcsKeyFastBTreeN<TKeyValue>.KeyEnumerator GetEnumeratorFastEx(TKeyValue keyLo, TKeyValue keyHi, bool reverse, int maxCount)
    {
      return new KeyEnumerator(this, keyLo, keyHi, reverse, maxCount);
    }
    //////////////////////////
    IEnumerator IEnumerable.GetEnumerator()
    {
      // call the generic version of the method
      return GetEnumerator();
    }
    //////////////////////////
    public IEnumerator<TKeyValue> GetEnumerator()
    {
      return new KeyEnumerator(this, default(TKeyValue), default(TKeyValue), false, -1);
    }
    //////////////////////////
    public class KeyEnumerator : IEnumerator<TKeyValue>
    {
      // constructor
      private FcsKeyFastBTreeN<TKeyValue> _btn = null;
      private TKeyValue _keyLo;
      private TKeyValue _keyHi;
      private int _maxCount;
      private bool _reverse;
      // locals
      private bool _bOK;
      private TKeyValue _key;
      private int _count;
      private KeyFast _btnFast;
      //////////////////////////
      public KeyEnumerator(FcsKeyFastBTreeN<TKeyValue> btn, TKeyValue keyLo, TKeyValue keyHi, bool reverse, int maxCount)
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
          _btnFast = default(KeyFast);
          if ((_keyLo.Equals(default(TKeyValue))) && (!_reverse))
            _bOK = (_btn.BtnFastFirst(out _key, out _btnFast) != null);
          else if ((_keyHi.Equals(default(TKeyValue))) && (_reverse))
            _bOK = (_btn.BtnFastLast(out _key, out _btnFast) != null);
          else
          {
            if (_reverse)
            {
              _key = _keyHi;
              _bOK = (_btn.BtnFastSearchPrev(ref _key, out _btnFast) != null);
              if ((!_keyLo.Equals(default(TKeyValue))) && (_bOK))
                _bOK = (_btn._funcCmp(_key, _keyLo, _btn._objCmp, null) >= 0);
            }
            else
            {
              _key = _keyLo;
              _bOK = (_btn.BtnFastSearch(ref _key, out _btnFast) != null);
              if ((!_keyHi.Equals(default(TKeyValue))) && (_bOK))
                _bOK = (_btn._funcCmp(_key, _keyHi, _btn._objCmp, null) <= 0);
            }
          }
        }
        else
        {
          if (_reverse)
          {
            _bOK = (_btn.BtnFastPrev(ref _key, ref _btnFast) != null);
            if ((!_keyLo.Equals(default(TKeyValue))) && (_bOK))
              _bOK = (_btn._funcCmp(_key, _keyLo, _btn._objCmp, null) >= 0);
          }
          else
          {
            _bOK = (_btn.BtnFastNext(ref _key, ref _btnFast) != null);
            if ((!_keyHi.Equals(default(TKeyValue))) && (_bOK))
              _bOK = (_btn._funcCmp(_key, _keyHi, _btn._objCmp, null) <= 0);
          }
        }
        return _bOK;
      }
      //////////////////////////
      public TKeyValue Current
      {
        get
        {
          if (!_bOK)
            return default(TKeyValue);
          return _key;
        }
      }
      //////////////////////////
      object IEnumerator.Current
      {
        get
        {
          if (!_bOK)
            return default(TKeyValue);
          return _key;
        }
      }
      //////////////////////////
      public void Dispose() { _btn = null; _btnFast.Dispose(); }
      //////////////////////////
      public void Reset()
      {
        _count = _maxCount;
        _bOK = true;
        _btnFast = default(KeyFast);
        _btnFast.version = int.MinValue;
      }
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////         !IEnumerator         //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  }
}
