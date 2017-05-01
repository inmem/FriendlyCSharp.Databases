// Licensed to the KUBDAT® under one or more agreements.
// The KUBDAT® licenses this file to you under the Apache-2.0 license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;

namespace FriendlyCSharp.Databases
{
  public partial class FcsLockBTreeN<TKey, TValue> : FcsBTreeN<TKey, TValue>
                                                      where TKey : struct, IComparable<TKey>
  {
    private object _btnLock;
    //////////////////////////
    public FcsLockBTreeN() : base(_btnDefaultBTreeN, 1, null)
    {
      _btnLock = _btnLockAdd;
    }
    //////////////////////////
    protected FcsLockBTreeN(object objCmp) : base(_btnDefaultBTreeN, 1, objCmp)
    {
      _btnLock = _btnLockAdd;
    }
    //////////////////////////
    public FcsLockBTreeN(int btnBTreeN) : base(btnBTreeN, 1, null)
    {
      _btnLock = _btnLockAdd;
    }
    //////////////////////////
    protected FcsLockBTreeN(int btnBTreeN, object objCmp) : base(btnBTreeN, 1, objCmp)
    {
      _btnLock = _btnLockAdd;
    }
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////            BtnFind           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public override bool? BtnFind(TKey key, out TValue value)
    {
      lock (_btnLock)
        return _BtnFind(ref key, out value, _btnRoot, out _btnKVFast[0]);
    }
    //////////////////////////
// Specify if necessary, the symbol of:   Project -> Properties -> Build -> Conditional Compilation Symbols 
#if KEY_VALUE_PAIR
    public override KeyValuePair<TKey, TValue>? BtnFind(TKey key)
    {
      lock (_btnLock)
      {
        if (_BtnFind(ref key, out TValue valueOut, _btnRoot, out _btnKVFast[0]) == true)
          return new KeyValuePair<TKey, TValue>(key, valueOut);
        else
          return null;
      }
    }
#else
    public override (TKey key, TValue value)? BtnFind(TKey key)
    {
      lock (_btnLock)
      {
        if (_BtnFind(ref key, out TValue valueOut, _btnRoot, out _btnKVFast[0]) == true)
          return (key: key, value: valueOut);
        else
          return null;
      }
    }
#endif
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////           BtnFirst           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public override bool? BtnFirst(out TKey key, out TValue value)
    {
      lock (_btnLock)
        return _BtnFirst(out key, out value, out _btnKVFast[0]);
    }
    //////////////////////////
#if KEY_VALUE_PAIR
    public override KeyValuePair<TKey, TValue>? BtnFirst()
    {
      lock (_btnLock)
        return _BtnFirst(out _btnKVFast[0]);
    }
#else
    public override (TKey key, TValue value)? BtnFirst()
    {
      lock (_btnLock)
        return _BtnFirst(out _btnKVFast[0]);
    }
#endif
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////            BtnLast           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public override bool? BtnLast(out TKey key, out TValue value)
    {
      lock (_btnLock)
        return _BtnLast(out key, out value, out _btnKVFast[0]);
    }
    //////////////////////////
#if KEY_VALUE_PAIR
    public override KeyValuePair<TKey, TValue>? BtnLast()
    {
      lock (_btnLock)
        return _BtnLast(out _btnKVFast[0]);
    }
#else
    public override (TKey key, TValue value)? BtnLast()
    {
      lock (_btnLock)
        return _BtnLast(out _btnKVFast[0]);
    }
#endif
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////            BtnNext           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public override bool? BtnNext(ref TKey key, out TValue value)
    {
      lock (_btnLock)
      {
        bool bNext = false;
        return _BtnNext(ref key, out value, _btnRoot, ref bNext, out _btnKVFast[0]);
      }
    }
    //////////////////////////
#if KEY_VALUE_PAIR
    public override KeyValuePair<TKey, TValue>? BtnNext(TKey key)
    {
      lock (_btnLock)
      {
        bool bNext = false;
        if (_BtnNext(ref key, out TValue valueOut, _btnRoot, ref bNext, out _btnKVFast[0]) == true)
          return new KeyValuePair<TKey, TValue>(key, valueOut);
        else
          return null;
      }
    }
#else
    public override (TKey key, TValue value)? BtnNext(TKey key)
    {
      lock (_btnLock)
      {
        bool bNext = false;
        if (_BtnNext(ref key, out TValue valueOut, _btnRoot, ref bNext, out _btnKVFast[0]) == true)
          return (key: key, value: valueOut);
        else
          return null;
      }
    }
#endif
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////            BtnPrev           //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public override bool? BtnPrev(ref TKey key, out TValue value)
    {
      lock (_btnLock)
      {
        bool bNext = false;
        return _BtnPrev(ref key, out value, _btnRoot, ref bNext, out _btnKVFast[0]);
      }
    }
    //////////////////////////
#if KEY_VALUE_PAIR
    public override KeyValuePair<TKey, TValue>? BtnPrev(TKey key)
    {
      lock (_btnLock)
      {
        bool bNext = false;
        if (_BtnPrev(ref key, out TValue valueOut, _btnRoot, ref bNext, out _btnKVFast[0]) == true)
          return new KeyValuePair<TKey, TValue>(key, valueOut);
        else
          return null;
      }
    }
#else
    public override (TKey key, TValue value)? BtnPrev(TKey key)
    {
      lock (_btnLock)
      {
        bool bNext = false;
        if (_BtnPrev(ref key, out TValue valueOut, _btnRoot, ref bNext, out _btnKVFast[0]) == true)
          return (key: key, value: valueOut);
        else
          return null;
      }
    }
#endif
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////           BtnSearch          //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public override bool? BtnSearch(ref TKey key, out TValue value)
    {
      bool bNext = false;
      lock (_btnLock)
        return _BtnSearch(ref key, out value, _btnRoot, ref bNext, out _btnKVFast[0]);
    }
    //////////////////////////
#if KEY_VALUE_PAIR
    public override KeyValuePair<TKey, TValue>? BtnSearch(TKey key)
    {
      lock (_btnLock)
      {
        bool bNext = false;
        if (_BtnSearch(ref key, out TValue valueOut, _btnRoot, ref bNext, out _btnKVFast[0]) == true)
          return new KeyValuePair<TKey, TValue>(key, valueOut);
        else
          return null;
      }
    }
#else
    public override (TKey key, TValue value)? BtnSearch(TKey key)
    {
      lock (_btnLock)
      {
        bool bNext = false;
        if (_BtnSearch(ref key, out TValue valueOut, _btnRoot, ref bNext, out _btnKVFast[0]) == true)
          return (key: key, value: valueOut);
        else
          return null;
      }
    }
#endif
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////         BtnSearchPrev        //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //////////////////////////
    public override bool? BtnSearchPrev(ref TKey key, out TValue value)
    {
      bool bNext = false;
      lock (_btnLock)
        return _BtnSearchPrev(ref key, out value, _btnRoot, ref bNext, out _btnKVFast[0]);
    }
    //////////////////////////
#if KEY_VALUE_PAIR
    public override KeyValuePair<TKey, TValue>? BtnSearchPrev(TKey key)
    {
      lock (_btnLock)
      {
        bool bNext = false;
        if (_BtnSearchPrev(ref key, out TValue valueOut, _btnRoot, ref bNext, out _btnKVFast[0]) == true)
          return new KeyValuePair<TKey, TValue>(key, valueOut);
        else
          return null;
      }
    }
#else
    public override (TKey key, TValue value)? BtnSearchPrev(TKey key)
    {
      lock (_btnLock)
      {
        bool bNext = false;
        if (_BtnSearchPrev(ref key, out TValue valueOut, _btnRoot, ref bNext, out _btnKVFast[0]) == true)
          return (key: key, value: valueOut);
        else
          return null;
      }
    }
#endif
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////           BtnUsedKeys           ///////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public override uint BtnUsedKeys()
    {
      lock (_btnLock)
        return BtnUsedKeys();
    }
  }
}
