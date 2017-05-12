// Licensed to the KUBDAT® under one or more agreements.
// The KUBDAT® licenses this file to you under the Apache-2.0 license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections;
using System.Collections.Generic;

namespace FriendlyCSharp.Databases
{
  public class FcsKeyFastBTreeN<TKey> : IEnumerable<TKey?> where TKey : struct
  {
    protected const int _btnDefaultBTreeN = 32;
    protected object _btnLockAdd;
    protected BtnKeyPage _btnRoot = null;
    protected BtnFastKey _btnFast;
    private object _objCmp = null;
    private Func<TKey, TKey, object, int> _funcCmp;
    private object _objUp = null;
    private FuncUpdate _funcUp;
    public delegate bool FuncUpdate(TKey keyAdd, ref TKey keyUpdate, object objUpdate);
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
    public struct BtnFastKey : IDisposable
    {
      internal int version;
      internal int fastMiddle;
      internal BtnKeyPage fastPage;
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
    protected internal class BtnKeyPage
    {
      public UInt32 flags;
      public bool bUpdatedValue;
      public int iDataCount;
      public TKey[] aData;
      public BtnKeyPage[] kvPageNextRight;
      public long[] aPos;
      //////////////////////////
      public BtnKeyPage(int iPageN, bool bPageNext)
      {
        bUpdatedValue = false;
        flags = 0;
        iDataCount = 0;
        aData = new TKey[(iPageN * 2) + 1];
        if (bPageNext)
          kvPageNextRight = new BtnKeyPage[(iPageN * 2) + 1];
        else
          kvPageNextRight = null;
        aPos = null;
      }
    }
    //////////////////////////
    public FcsKeyFastBTreeN(Func<TKey, TKey, object, int> funcCmp) : this(funcCmp, null, null, null, _btnDefaultBTreeN)
    {
    }
    //////////////////////////
    public FcsKeyFastBTreeN(Func<TKey, TKey, object, int> funcCmp, int btnBTreeN) : this(funcCmp, null, null, null, btnBTreeN)
    {
    }
    //////////////////////////
    public FcsKeyFastBTreeN(Func<TKey, TKey, object, int> funcCmp, FuncUpdate funcUp) : this(funcCmp, null, funcUp, null, _btnDefaultBTreeN)
    {
    }
    //////////////////////////
    public FcsKeyFastBTreeN(Func<TKey, TKey, object, int> funcCmp, FuncUpdate funcUp, object objUp) : this(funcCmp, null, funcUp, objUp, _btnDefaultBTreeN)
    {
    }
    //////////////////////////
    public FcsKeyFastBTreeN(Func<TKey, TKey, object, int> funcCmp, FuncUpdate funcUp, int btnBTreeN) : this(funcCmp, null, funcUp, null, btnBTreeN)
    {
    }
    //////////////////////////
    public FcsKeyFastBTreeN(Func<TKey, TKey, object, int> funcCmp, FuncUpdate funcUp, object objUp, int btnBTreeN) : this(funcCmp, null, funcUp, objUp, btnBTreeN)
    {
    }
    //////////////////////////
    public FcsKeyFastBTreeN(Func<TKey, TKey, object, int> funcCmp, object objCmp) : this(funcCmp, objCmp, null, null, _btnDefaultBTreeN)
    {
    }
    //////////////////////////
    public FcsKeyFastBTreeN(Func<TKey, TKey, object, int> funcCmp, object objCmp, int btnBTreeN) : this(funcCmp, objCmp, null, null, btnBTreeN)
    {
    }
    //////////////////////////
    public FcsKeyFastBTreeN(Func<TKey, TKey, object, int> funcCmp, object objCmp, FuncUpdate funcUp) : this(funcCmp, objCmp, funcUp, null, _btnDefaultBTreeN)
    {
    }
    //////////////////////////
    public FcsKeyFastBTreeN(Func<TKey, TKey, object, int> funcCmp, object objCmp, FuncUpdate funcUp, object objUp) : this(funcCmp, objCmp, funcUp, objUp, _btnDefaultBTreeN)
    {
    }
    //////////////////////////
    public FcsKeyFastBTreeN(Func<TKey, TKey, object, int> funcCmp, object objCmp, FuncUpdate funcUp, object objUp, int btnBTreeN)
    {
      if ((btnBTreeN <= 0) || (btnBTreeN > _btnMaxBTreeN))
        throw new ArgumentOutOfRangeException();
      _btnUpdatedRoot = false;
      _btnVersion = 0;
      _btnVersionPage = 0;
      _btnFast = default(BtnFastKey);
      _btnLockAdd = new object();
      _btnRoot = null;
      _btnBTreeN = btnBTreeN;
      _objCmp = objCmp;
      _funcCmp = funcCmp;
      _objUp = objUp;
      _funcUp = funcUp;
    }
    //////////////////////////
    public TKey this[TKey key]
    {
      get
      {
        TKey keyL = key;
        if (BtnFind(ref keyL) == true)
          return keyL;
        return default(TKey);
      }
      set
      {
        BtnAdd(key);
      }
    }
    //////////////////////////
    public TKey this[TKey key, bool bNext]
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
    private bool? _BtnAdd(ref TKey keyAdd, out TKey keyUp, ref BtnKeyPage keyPageUp, ref bool bUp)
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
        BtnKeyPage QQ = null;
        // binary search
        do
        {
          middle = (low + high);
          middle >>= 1;   // deleno 2
          iResultCmp = _funcCmp(keyAdd, keyPageUp.aData[middle], _objCmp);
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
          keyUp = default(TKey);
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
              BtnKeyPage kvPageRight = null;
              TKey kvUpLocal;
              BtnKeyPage kvPageNew = new BtnKeyPage(BtnBTreeN, keyPageUp.kvPageNextRight != null);
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
    public bool? BtnAdd(ref TKey key)
    {
      bool bUp = false;
      BtnKeyPage keyPageDown = _btnRoot;
      bool? bNullResult = null;

      lock (_btnLockAdd)
      {
        bNullResult = _BtnAdd(ref key, out TKey keyUp, ref keyPageDown, ref bUp);
        if (bUp)
        {
          bUp = false;
          _btnVersion++;
          _btnVersionPage++;
          _btnUpdatedRoot = true;
          BtnKeyPage QQ = new BtnKeyPage(BtnBTreeN, keyPageDown != null) { iDataCount = 1 };
          QQ.aData[1] = keyUp;
          if (QQ.kvPageNextRight != null)
          {
            QQ.kvPageNextRight[0] = _btnRoot;
            QQ.kvPageNextRight[1] = keyPageDown;
          }
          _btnRoot = QQ;
          bNullResult = new bool?(true);
        }
      }
      return bNullResult; // add = true, update = false, else null;
    }
    //////////////////////////
    public bool? BtnAdd(TKey key)
    {
      return BtnAdd(ref key);
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////         BtnDeleteAll         //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public void BtnDeleteAll()
    {
      BtnDeleteAll(false);
    }
    //////////////////////////
    public void BtnDeleteAll(bool bRunGC)
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
    protected bool? _BtnFind(ref TKey key, BtnKeyPage keyPage, out BtnFastKey btnFast)
    {
      bool? bNullResult = null;
      if (keyPage == null)
        btnFast = default(BtnFastKey);
      else
      {
        int iResultCmp;
        int middle, low = 1, high = keyPage.iDataCount;
        BtnKeyPage QQ = null;
        // binary search
        do
        {
          middle = (low + high);
          middle >>= 1;   // deleno 2
          iResultCmp = _funcCmp(key, keyPage.aData[middle], _objCmp);
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
            btnFast = default(BtnFastKey);
          bNullResult = new bool?(true);
        }
        if (keyPage.kvPageNextRight == null)
          QQ = null;
        else if (high > 0)
          QQ = keyPage.kvPageNextRight[high];         // right
        else
          QQ = keyPage.kvPageNextRight[0];            // left
        bNullResult = _BtnFind(ref key, QQ, out btnFast);
      }
      return bNullResult;
    }
    //////////////////////////
    public virtual bool? BtnFind(ref TKey key)
    {
      return _BtnFind(ref key, _btnRoot, out _btnFast);
    }
    //////////////////////////
    public virtual bool? BtnFastFind(ref TKey key, out BtnFastKey btnFast)
    {
      return _BtnFind(ref key, _btnRoot, out btnFast);
    }
    ////////////////////////// 
    public virtual TKey BtnFind(TKey key)
    {
      if (_BtnFind(ref key, _btnRoot, out _btnFast) != null)
        return key;
      else
        return default(TKey);
    }
    ////////////////////////// 
    public virtual TKey BtnFastFind(TKey key, out BtnFastKey btnFast)
    {
      if (_BtnFind(ref key, _btnRoot, out btnFast) != null)
        return key;
      else
        return default(TKey);
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////           BtnFirst           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected bool? _BtnFirst(out TKey key, out BtnFastKey btnFast)
    {
      BtnKeyPage QQ = _btnRoot;
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
        key = default(TKey);
        btnFast = default(BtnFastKey);
        return null;
      }
    }
    public virtual bool? BtnFirst(out TKey key)
    {
      return _BtnFirst(out key, out _btnFast);
    }
    //////////////////////////
    public virtual bool? BtnFastFirst(out TKey key, out BtnFastKey btnFast)
    {
      return _BtnFirst(out key, out btnFast);
    }
    //////////////////////////
    protected TKey _BtnFirst(out BtnFastKey btnFast)
    {
      BtnKeyPage QQ = _btnRoot;
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
        btnFast = default(BtnFastKey);
        return default(TKey);
      }
    }
    public virtual TKey BtnFirst()
    {
      return _BtnFirst(out _btnFast);
    }
    public virtual TKey BtnFastFirst(out BtnFastKey btnFast)
    {
      return _BtnFirst(out btnFast);
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////            BtnLast           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected bool? _BtnLast(out TKey key, out BtnFastKey btnFast)
    {
      BtnKeyPage QQ = _btnRoot;
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
        key = default(TKey);
        btnFast = default(BtnFastKey);
        return null;
      }
    }
    public virtual bool? BtnLast(out TKey key)
    {
      return _BtnLast(out key, out _btnFast);
    }
    //////////////////////////
    public virtual bool? BtnFastLast(out TKey key, out BtnFastKey btnFast)
    {
      return _BtnLast(out key, out btnFast);
    }
    //////////////////////////
    protected TKey _BtnLast(out BtnFastKey btnFast)
    {
      BtnKeyPage QQ = _btnRoot;
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
        btnFast = default(BtnFastKey);
        return default(TKey);
      }
    }
    public virtual TKey BtnLast()
    {
      return _BtnLast(out _btnFast);
    }
    public virtual TKey BtnFastLast(out BtnFastKey btnFast)
    {
      return _BtnLast(out btnFast);
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////            BtnNext           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected bool? _BtnNext(ref TKey key, BtnKeyPage keyPage, ref bool bNext, out BtnFastKey btnFast)
    {
      bool? bNullResult = null;
      if (keyPage == null)
      {
        btnFast = default(BtnFastKey);
        bNext = false;
      }
      else
      {
        int iResultCmp;
        int middle, low = 1, high = keyPage.iDataCount;
        BtnKeyPage QQ = null;
        // binary search
        do
        {
          middle = (low + high);
          middle >>= 1;   // deleno 2
          iResultCmp = _funcCmp(key, keyPage.aData[middle], _objCmp);
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
              btnFast = default(BtnFastKey);
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
              btnFast = default(BtnFastKey);
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
    public virtual bool? BtnNext(ref TKey key)
    {
      bool bNext = false;
      return _BtnNext(ref key, _btnRoot, ref bNext, out _btnFast);
    }
    //////////////////////////
    public virtual bool? BtnFastNext(ref TKey key, ref BtnFastKey btnFast)
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
    public virtual TKey BtnNext(TKey key)
    {
      bool bNext = false;
      if (_BtnNext(ref key, _btnRoot, ref bNext, out _btnFast) != null)
        return key;
      else
        return default(TKey);
    }
    public virtual TKey BtnFastNext(TKey key, ref BtnFastKey btnFast)
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
          return default(TKey);
      }
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////            BtnPrev           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected bool? _BtnPrev(ref TKey key, BtnKeyPage keyPage, ref bool bNext, out BtnFastKey btnFast)
    {
      bool? bNullResult = null;
      if (keyPage == null)
      {
        btnFast = default(BtnFastKey);
        bNext = false;
      }
      else
      {
        int iResultCmp;
        int middle, low = 1, high = keyPage.iDataCount;
        BtnKeyPage QQ = null;
        // binary search
        do
        {
          middle = (low + high);
          middle >>= 1;   // deleno 2
          iResultCmp = _funcCmp(key, keyPage.aData[middle], _objCmp);
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
              btnFast = default(BtnFastKey);
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
              btnFast = default(BtnFastKey);
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
    public virtual bool? BtnPrev(ref TKey key)
    {
      bool bNext = false;
      return _BtnPrev(ref key, _btnRoot, ref bNext, out _btnFast);
    }
    //////////////////////////
    public virtual bool? BtnFastPrev(ref TKey key, ref BtnFastKey btnFast)
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
    public virtual TKey BtnPrev(TKey key)
    {
      bool bNext = false;
      if (_BtnPrev(ref key, _btnRoot, ref bNext, out _btnFast) != null)
        return key;
      else
        return default(TKey);
    }
    public virtual TKey BtnFastPrev(TKey key, ref BtnFastKey btnFast)
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
          return default(TKey);
      }
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////           BtnSearch          //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected bool? _BtnSearch(ref TKey key, BtnKeyPage keyPage, ref bool bNext, out BtnFastKey btnFast)
    {
      bool? bNullResult = null;
      if (keyPage == null)
      {
        btnFast = default(BtnFastKey);
        bNext = true;
      }
      else
      {
        int iResultCmp;
        int middle, low = 1, high = keyPage.iDataCount;
        BtnKeyPage QQ = null;
        // binary search
        do
        {
          middle = (low + high);
          middle >>= 1;   // deleno 2
          iResultCmp = _funcCmp(key, keyPage.aData[middle], _objCmp);
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
            btnFast = default(BtnFastKey);
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
          bNullResult = _BtnSearch(ref key, QQ, ref bNext, out btnFast);
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
    public virtual bool? BtnSearch(ref TKey key)
    {
      bool bNext = false;
      return _BtnSearch(ref key, _btnRoot, ref bNext, out _btnFast);
    }
    //////////////////////////
    public virtual bool? BtnFastSearch(ref TKey key, out BtnFastKey btnFast)
    {
      bool bNext = false;
      return _BtnSearch(ref key, _btnRoot, ref bNext, out btnFast);
    }
    //////////////////////////
    public virtual TKey BtnSearch(TKey key)
    {
      bool bNext = false;
      if (_BtnSearch(ref key, _btnRoot, ref bNext, out _btnFast) != null)
        return key;
      else
        return default(TKey);
    }
    public virtual TKey BtnFastSearch(TKey key, out BtnFastKey btnFast)
    {
      bool bNext = false;
      if (_BtnSearch(ref key, _btnRoot, ref bNext, out btnFast) != null)
        return key;
      else
        return default(TKey);
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////         BtnSearchPrev        //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected bool? _BtnSearchPrev(ref TKey key, BtnKeyPage keyPage, ref bool bNext, out BtnFastKey btnFast)
    {
      bool? bNullResult = null;
      if (keyPage == null)
      {
        btnFast = default(BtnFastKey);
        bNext = true;
      }
      else
      {
        int iResultCmp;
        int middle, low = 1, high = keyPage.iDataCount;
        BtnKeyPage QQ = null;
        // binary search
        do
        {
          middle = (low + high);
          middle >>= 1;   // deleno 2
          iResultCmp = _funcCmp(key, keyPage.aData[middle], _objCmp);
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
            btnFast = default(BtnFastKey);
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
          bNullResult = _BtnSearchPrev(ref key, QQ, ref bNext, out btnFast);
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
    public virtual bool? BtnSearchPrev(ref TKey key)
    {
      bool bNext = false;
      return _BtnSearchPrev(ref key, _btnRoot, ref bNext, out _btnFast);
    }
    //////////////////////////
    public virtual bool? BtnFastSearchPrev(ref TKey key, out BtnFastKey btnFast)
    {
      bool bNext = false;
      return _BtnSearchPrev(ref key, _btnRoot, ref bNext, out btnFast);
    }
    //////////////////////////
    public virtual TKey BtnSearchPrev(TKey key)
    {
      bool bNext = false;
      if (_BtnSearchPrev(ref key, _btnRoot, ref bNext, out _btnFast) != null)
        return key;
      else
        return default(TKey);
    }
    public virtual TKey BtnFastSearchPrev(TKey key, out BtnFastKey btnFast)
    {
      bool bNext = false;
      if (_BtnSearchPrev(ref key, _btnRoot, ref bNext, out btnFast) != null)
        return key;
      else
        return default(TKey);
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////         BtnUpdate          //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    private bool? _BtnUpdate(ref TKey keyUpdate, BtnKeyPage keyPage)
    {
      bool? bNullResult = null;
      if (keyPage != null)
      {
        int iResultCmp;
        int middle, low = 1, high = keyPage.iDataCount;
        BtnKeyPage QQ = null;
        // binary search
        do
        {
          middle = (low + high);
          middle >>= 1;   // deleno 2
          iResultCmp = _funcCmp(keyUpdate, keyPage.aData[middle], _objCmp);
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
    public bool? BtnUpdate(ref TKey key)
    {
      // update = false, else null;
      return _BtnUpdate(ref key, _btnRoot);
    }
    //////////////////////////
    public bool? BtnUpdate(TKey key)
    {
      // update = false, else null;
      return _BtnUpdate(ref key, _btnRoot);
    }
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////           BtnUsedKeys           ///////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    protected void _BtnUsedKeys(ref uint Count, BtnKeyPage QQ)
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
    public FcsKeyFastBTreeN<TKey>.BtnKeyEnumeratorFast GetEnumeratorFastEx(bool reverse)
    {
      return new BtnKeyEnumeratorFast(this, null, null, reverse, -2);
    }
    //////////////////////////
    public FcsKeyFastBTreeN<TKey>.BtnKeyEnumeratorFast GetEnumeratorFastEx(bool reverse, int maxCount)
    {
      return new BtnKeyEnumeratorFast(this, null, null, reverse, maxCount);
    }
    //////////////////////////
    public FcsKeyFastBTreeN<TKey>.BtnKeyEnumeratorFast GetEnumeratorFastEx(TKey? keyLo, TKey? keyHi, bool reverse)
    {
      return new BtnKeyEnumeratorFast(this, keyLo, keyHi, reverse, -3);
    }
    //////////////////////////
    public FcsKeyFastBTreeN<TKey>.BtnKeyEnumeratorFast GetEnumeratorFastEx(TKey? keyLo, TKey? keyHi, bool reverse, int maxCount)
    {
      return new BtnKeyEnumeratorFast(this, keyLo, keyHi, reverse, maxCount);
    }
    //////////////////////////
    IEnumerator IEnumerable.GetEnumerator()
    {
      // call the generic version of the method
      return GetEnumerator();
    }
    //////////////////////////
    public IEnumerator<TKey?> GetEnumerator()
    {
      return new BtnKeyEnumeratorFast(this, null, null, false, -1);
    }
    //////////////////////////
    public class BtnKeyEnumeratorFast : IEnumerator<TKey?>
    {
      // constructor
      private FcsKeyFastBTreeN<TKey> _btn = null;
      private TKey? _keyLo;
      private TKey? _keyHi;
      private int _maxCount;
      private bool _reverse;
      // locals
      private bool _bOK;
      private TKey _key;
      private int _count;
      private BtnFastKey _btnFast;
      //////////////////////////
      public BtnKeyEnumeratorFast(FcsKeyFastBTreeN<TKey> btn, TKey? keyLo, TKey? keyHi, bool reverse, int maxCount)
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
          _btnFast = default(BtnFastKey);
          if ((_keyLo == null) && (!_reverse))
            _bOK = (_btn.BtnFastFirst(out _key, out _btnFast) != null);
          else if ((_keyHi == null) && (_reverse))
            _bOK = (_btn.BtnFastLast(out _key, out _btnFast) != null);
          else
          {
            if (_reverse)
            {
              _key = _keyHi.GetValueOrDefault();
              _bOK = (_btn.BtnFastSearchPrev(ref _key, out _btnFast) != null);
              if ((_keyLo != null) && (_bOK))
                _bOK = (_btn._funcCmp(_key, _keyLo.GetValueOrDefault(), _btn._objCmp) >= 0);
            }
            else
            {
              _key = _keyLo.GetValueOrDefault();
              _bOK = (_btn.BtnFastSearch(ref _key, out _btnFast) != null);
              if ((_keyHi != null) && (_bOK))
                _bOK = (_btn._funcCmp(_key, _keyHi.GetValueOrDefault(), _btn._objCmp) <= 0);
            }
          }
        }
        else
        {
          if (_reverse)
          {
            _bOK = (_btn.BtnFastPrev(ref _key, ref _btnFast) != null);
            if ((_keyLo != null) && (_bOK))
              _bOK = (_btn._funcCmp(_key, _keyLo.GetValueOrDefault(), _btn._objCmp) >= 0);
          }
          else
          {
            _bOK = (_btn.BtnFastNext(ref _key, ref _btnFast) != null);
            if ((_keyHi != null) && (_bOK))
              _bOK = (_btn._funcCmp(_key, _keyHi.GetValueOrDefault(), _btn._objCmp) <= 0);
          }
        }
        return _bOK;
      }
      //////////////////////////
      public TKey? Current
      {
        get
        {
          if (!_bOK)
            return null;
          return _key;
        }
      }
      //////////////////////////
      object IEnumerator.Current
      {
        get
        {
          if (!_bOK)
            return null;
          return _key;
        }
      }
      //////////////////////////
      public void Dispose() { _btn = null; }
      //////////////////////////
      public void Reset()
      {
        _count = _maxCount;
        _bOK = true;
        _btnFast = default(BtnFastKey);
        _btnFast.version = int.MinValue;
      }
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////         !IEnumerator         //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  }
}
