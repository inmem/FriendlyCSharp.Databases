// Licensed to the KUBDAT® under one or more agreements.
// The KUBDAT® licenses this file to you under the Apache-2.0 license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections;
using System.Collections.Generic;

namespace FriendlyCSharp.Databases
{
  public partial class FcsFastBTreeN<TKey, TValue> : FcsBTreeN<TKey, TValue>, IEnumerable<KeyValuePair<TKey, TValue>?>
                                                     where TKey : struct, IComparable<TKey>
  {
    //////////////////////////
    public FcsFastBTreeN(uint allocIdxFast) : base(_btnDefaultBTreeN, allocIdxFast, null)
    {
    }
    //////////////////////////
    protected FcsFastBTreeN(uint allocIdxFast, object objCmp) : base(_btnDefaultBTreeN, allocIdxFast, objCmp)
    {
    }
    //////////////////////////
    public FcsFastBTreeN(int btnBTreeN, uint allocIdxFast) : base(btnBTreeN, allocIdxFast, null)
    {
    }
    //////////////////////////
    protected FcsFastBTreeN(int btnBTreeN, uint allocIdxFast, object objCmp) : base(btnBTreeN, allocIdxFast, objCmp)
    {
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////            BtnFind           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public virtual bool? BtnFastFind(TKey key, out TValue value, uint idxFast)
    {
      return _BtnFind(ref key, out value, _btnRoot, out _btnKVFast[idxFast]);
    }
    //////////////////////////
    protected virtual bool? BtnFastFind(TKey key, out TValue value, out KeyValueFast btnFast)
    {
      return _BtnFind(ref key, out value, _btnRoot, out btnFast);
    }
    //////////////////////////
// Specify if necessary, the symbol of:   Project -> Properties -> Build -> Conditional Compilation Symbols 
#if KEY_VALUE_PAIR
    public virtual KeyValuePair<TKey, TValue>? BtnFastFind(TKey key, uint idxFast)
    {
      if (_BtnFind(ref key, out TValue valueOut, _btnRoot, out _btnKVFast[idxFast]) == true)
        return new KeyValuePair<TKey, TValue>(key, valueOut);
      else
        return null;
    }
#else
    public virtual (TKey key, TValue value)? BtnFastFind(TKey key, uint idxFast)
    {
      if (_BtnFind(ref key, out TValue valueOut, _btnRoot, out _btnKVFast[idxFast]) == true)
        return (key: key, value: valueOut);
      else
        return null;
    }
#endif
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////           BtnFirst           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public virtual bool? BtnFastFirst(out TKey key, out TValue value, uint idxFast)
    {
      return _BtnFirst(out key, out value, out _btnKVFast[idxFast]);
    }
    //////////////////////////
    protected virtual bool? BtnFastFirst(out TKey key, out TValue value, out KeyValueFast btnFast)
    {
      return _BtnFirst(out key, out value, out btnFast);
    }
    //////////////////////////
#if KEY_VALUE_PAIR
    public virtual KeyValuePair<TKey, TValue>? BtnFastFirst(uint idxFast)
    {
      return _BtnFirst(out _btnKVFast[idxFast]);
    }
#else
    public virtual (TKey key, TValue value)? BtnFastFirst(uint idxFast)
    {
      return _BtnFirst(out _btnKVFast[idxFast]);
    }
#endif
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////            BtnLast           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public virtual bool? BtnFastLast(out TKey key, out TValue value, uint idxFast)
    {
      return _BtnLast(out key, out value, out _btnKVFast[idxFast]);
    }
    //////////////////////////
    protected virtual bool? BtnFastLast(out TKey key, out TValue value, out KeyValueFast btnFast)
    {
      return _BtnLast(out key, out value, out btnFast);
    }
    //////////////////////////
#if KEY_VALUE_PAIR
    public virtual KeyValuePair<TKey, TValue>? BtnFastLast(uint idxFast)
    {
      return _BtnLast(out _btnKVFast[idxFast]);
    }
#else
    public virtual (TKey key, TValue value)? BtnFastLast(uint idxFast)
    {
      return _BtnLast(out _btnKVFast[idxFast]);
    }
#endif
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////            BtnNext           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public virtual bool? BtnFastNext(ref TKey key, out TValue value, uint idxFast)
    {
      int middle = _btnKVFast[idxFast].fastMiddle;
      KeyValueFast btnFast = _btnKVFast[idxFast];
      if ( (btnFast.version == BtnVersion) && (btnFast.fastPage != null) && (middle > 0) &&
           (middle < btnFast.fastPage.iDataCount) && (BtnCompares(key, btnFast.fastPage.aData[middle].key, _objCompares) == 0))
      {
        middle = ++_btnKVFast[idxFast].fastMiddle;
        key = btnFast.fastPage.aData[middle].key;
        value = btnFast.fastPage.aData[middle].value;
        return true;
      }
      else
      {
        bool bNext = false;
        return _BtnNext(ref key, out value, _btnRoot, ref bNext, out _btnKVFast[idxFast]);
      }
    }
    //////////////////////////
    protected virtual bool? BtnFastNext(ref TKey key, out TValue value, ref KeyValueFast btnFast)
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
        return _BtnNext(ref key, out value, _btnRoot, ref bNext,  out btnFast);
      }
    }
    //////////////////////////
#if KEY_VALUE_PAIR
    public virtual KeyValuePair<TKey, TValue>? BtnFastNext(TKey key, uint idxFast)
    {
      int middle = _btnKVFast[idxFast].fastMiddle;
      KeyValueFast btnFast = _btnKVFast[idxFast];
      if ( (btnFast.version == BtnVersion) && (btnFast.fastPage != null) && (middle > 0) &&
           (middle < btnFast.fastPage.iDataCount) && (BtnCompares(key, btnFast.fastPage.aData[middle].key, _objCompares) == 0))
      {
        middle = ++_btnKVFast[idxFast].fastMiddle;
        return new KeyValuePair<TKey, TValue>(btnFast.fastPage.aData[middle].key, btnFast.fastPage.aData[middle].value);
      }
      else
      {
        bool bNext = false;
        if (_BtnNext(ref key, out TValue valueOut, _btnRoot, ref bNext, out _btnKVFast[idxFast]) == true)
          return new KeyValuePair<TKey, TValue>(key, valueOut);
        else
          return null;
      }
    }
#else
    public virtual (TKey key, TValue value)? BtnFastNext(TKey key, uint idxFast)
    {
      int middle = _btnKVFast[idxFast].fastMiddle;
      BtnFastKeyValue btnFast = _btnKVFast[idxFast];
      if ( (btnFast.version == BtnVersion) && (btnFast.fastPage != null) && (middle > 0) &&
           (middle < btnFast.fastPage.iDataCount) && (BtnCompares(key, btnFast.fastPage.aData[middle].key, _objCompares) == 0))
      {
        middle = ++_btnKVFast[idxFast].fastMiddle;
        return (key: btnFast.fastPage.aData[middle].key, value: btnFast.fastPage.aData[middle].value);
      }
      else
      {
        bool bNext = false;
        if (_BtnNext(ref key, out TValue valueOut, _btnRoot, ref bNext, out _btnKVFast[idxFast]) == true)
          return (key: key, value: valueOut);
        else
          return null;
      }
    }
#endif
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////            BtnPrev           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public virtual bool? BtnFastPrev(ref TKey key, out TValue value, uint idxFast)
    {
      int middle = _btnKVFast[idxFast].fastMiddle;
      KeyValueFast btnFast = _btnKVFast[idxFast];
      if ( (btnFast.version == BtnVersion) && (btnFast.fastPage != null) && (middle > 1) &&
           (middle <= btnFast.fastPage.iDataCount) && (BtnCompares(key, btnFast.fastPage.aData[middle].key, _objCompares) == 0) )
      {
        middle = --_btnKVFast[idxFast].fastMiddle;
        key = btnFast.fastPage.aData[middle].key;
        value = btnFast.fastPage.aData[middle].value;
        return true;
      }
      else
      {
        bool bNext = false;
        return _BtnPrev(ref key, out value, _btnRoot, ref bNext, out _btnKVFast[idxFast]);
      }
    }
    //////////////////////////
    protected virtual bool? BtnFastPrev(ref TKey key, out TValue value, ref KeyValueFast btnFast)
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
    public virtual KeyValuePair<TKey, TValue>? BtnFastPrev(TKey key, uint idxFast)
    {
      int middle = _btnKVFast[idxFast].fastMiddle;
      KeyValueFast btnFast = _btnKVFast[idxFast];
      if ( (btnFast.version == BtnVersion) && (btnFast.fastPage != null) && (middle > 1) &&
           (middle <= btnFast.fastPage.iDataCount) && (BtnCompares(key, btnFast.fastPage.aData[middle].key, _objCompares) == 0))
      {
        middle = --_btnKVFast[idxFast].fastMiddle;
        return new KeyValuePair<TKey, TValue>(btnFast.fastPage.aData[middle].key, btnFast.fastPage.aData[middle].value);
      }
      else
      {
        bool bNext = false;
        if (_BtnPrev(ref key, out TValue valueOut, _btnRoot, ref bNext, out _btnKVFast[idxFast]) == true)
          return new KeyValuePair<TKey, TValue>(key, valueOut);
        else
          return null;
      }
    }
#else
    public virtual (TKey key, TValue value)? BtnFastPrev(TKey key, uint idxFast)
    {
      int middle = _btnKVFast[idxFast].fastMiddle;
      BtnFastKeyValue btnFast = _btnKVFast[idxFast];
      if ( (btnFast.version == BtnVersion) && (btnFast.fastPage != null) && (middle > 1) &&
           (middle <= btnFast.fastPage.iDataCount) && (BtnCompares(key, btnFast.fastPage.aData[middle].key, _objCompares) == 0))
      {
        middle = --_btnKVFast[idxFast].fastMiddle;
        return (key: btnFast.fastPage.aData[middle].key, value: btnFast.fastPage.aData[middle].value);
      }
      else
      {
        bool bNext = false;
        if (_BtnNext(ref key, out TValue valueOut, _btnRoot, ref bNext, out _btnKVFast[idxFast]) == true)
          return (key: key, value: valueOut);
        else
          return null;
      }
    }
#endif
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////           BtnSearch          //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public virtual bool? BtnFastSearch(ref TKey key, out TValue value, uint idxFast)
    {
      bool bNext = false;
      return _BtnSearch(ref key, out value, _btnRoot, ref bNext, out _btnKVFast[idxFast]);
    }
    //////////////////////////
    protected virtual bool? BtnFastSearch(ref TKey key, out TValue value, out KeyValueFast btnFast)
    {
      bool bNext = false;
      return _BtnSearch(ref key, out value, _btnRoot, ref bNext, out btnFast);
    }
    //////////////////////////
#if KEY_VALUE_PAIR
    public virtual KeyValuePair<TKey, TValue>? BtnFastSearch(TKey key, uint idxFast)
    {
      bool bNext = false;
      if (_BtnSearch(ref key, out TValue valueOut, _btnRoot, ref bNext, out _btnKVFast[idxFast]) == true)
        return new KeyValuePair<TKey, TValue>(key, valueOut);
      else
        return null;
    }
#else
    public virtual (TKey key, TValue value)? BtnFastSearch(TKey key, uint idxFast)
    {
      bool bNext = false;
      if (_BtnSearch(ref key, out TValue valueOut, _btnRoot, ref bNext, out _btnKVFast[idxFast]) == true)
        return (key: key, value: valueOut);
      else
        return null;
    }
#endif
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////         BtnSearchPrev        //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public virtual bool? BtnFastSearchPrev(ref TKey key, out TValue value, uint idxFast)
    {
      bool bNext = false;
      return _BtnSearchPrev(ref key, out value, _btnRoot, ref bNext, out _btnKVFast[idxFast]);
    }
    //////////////////////////
    protected virtual bool? BtnFastSearchPrev(ref TKey key, out TValue value, out KeyValueFast btnFast)
    {
      bool bNext = false;
      return _BtnSearchPrev(ref key, out value, _btnRoot, ref bNext, out btnFast);
    }
    //////////////////////////
#if KEY_VALUE_PAIR
    public virtual KeyValuePair<TKey, TValue>? BtnFastSearchPrev(TKey key, uint idxFast)
    {
      bool bNext = false;
      if (_BtnSearchPrev(ref key, out TValue valueOut, _btnRoot, ref bNext, out _btnKVFast[idxFast]) == true)
        return new KeyValuePair<TKey, TValue>(key, valueOut);
      else
        return null;
    }
#else
    public virtual (TKey key, TValue value)? BtnFastSearchPrev(TKey key, uint idxFast)
    {
      bool bNext = false;
      if (_BtnSearchPrev(ref key, out TValue valueOut, _btnRoot, ref bNext, out _btnKVFast[idxFast]) == true)
        return (key: key, value: valueOut);
      else
        return null;
    }
#endif
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////         IEnumerator          //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public FcsFastBTreeN<TKey, TValue>.BtnEnumeratorFast GetEnumeratorFastEx(bool reverse)
    {
      return new BtnEnumeratorFast(this, null, null, reverse, -2);
    }
    //////////////////////////
    public FcsFastBTreeN<TKey, TValue>.BtnEnumeratorFast GetEnumeratorFastEx(bool reverse, int maxCount)
    {
      return new BtnEnumeratorFast(this, null, null, reverse, maxCount);
    }
    //////////////////////////
    public FcsFastBTreeN<TKey, TValue>.BtnEnumeratorFast GetEnumeratorFastEx(TKey? keyLo, TKey? keyHi, bool reverse)
    {
      return new BtnEnumeratorFast(this, keyLo, keyHi, reverse, -3);
    }
    //////////////////////////
    public FcsFastBTreeN<TKey, TValue>.BtnEnumeratorFast GetEnumeratorFastEx(TKey? keyLo, TKey? keyHi, bool reverse, int maxCount)
    {
      return new BtnEnumeratorFast(this, keyLo, keyHi, reverse, maxCount);
    }
    //////////////////////////
    public new IEnumerator<KeyValuePair<TKey, TValue>?> GetEnumerator()
    {
      return new BtnEnumeratorFast(this, null, null, false, -1);
    }
    //////////////////////////
    public class BtnEnumeratorFast : IEnumerator<KeyValuePair<TKey, TValue>?>
    {
      // constructor
      private FcsFastBTreeN<TKey, TValue> _btn = null;
      private TKey?  _keyLo;
      private TKey?  _keyHi;
      private int    _maxCount;
      private bool   _reverse;
      // locals
      private bool   _bOK;
      private TKey   _key;
      private TValue _value;
      private int    _count;
      private KeyValueFast _btnFast;
      //////////////////////////
      public BtnEnumeratorFast(FcsFastBTreeN<TKey, TValue> btn, TKey? keyLo, TKey? keyHi, bool reverse, int maxCount)
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
          _btnFast = default(KeyValueFast);
          if ((_keyLo == null) && (!_reverse))
            _bOK = (_btn.BtnFastFirst(out _key, out _value, out _btnFast) != null);
          else if ((_keyHi == null) && (_reverse))
            _bOK = (_btn.BtnFastLast(out _key, out _value, out _btnFast) != null);
          else
          {
            if (_reverse)
            {
              _key = _keyHi.GetValueOrDefault();
              _bOK = (_btn.BtnFastSearchPrev(ref _key, out _value, out _btnFast)!=null);
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
      public KeyValuePair<TKey, TValue>? Current
      {
        get
        {
          if (!_bOK)
            return null;
          return new KeyValuePair<TKey, TValue>(_key, _value);
        }
      }
      //////////////////////////
      object IEnumerator.Current
      {
        get
        {
          if (!_bOK)
            return null;
          return new KeyValuePair<TKey, TValue>(_key, _value);
        }
      }
      //////////////////////////
      public void Dispose() { _btn = null; _btnFast.Dispose(); }
      //////////////////////////
      public void Reset()
      {
        _count = _maxCount;
        _bOK = true;
        _btnFast = default(KeyValueFast);
        _btnFast.version = int.MinValue;
      }
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////         !IEnumerator         //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  }
}
