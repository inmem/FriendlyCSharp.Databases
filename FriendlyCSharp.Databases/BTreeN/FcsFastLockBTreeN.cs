// Licensed to the KUBDAT® under one or more agreements.
// The KUBDAT® licenses this file to you under the Apache-2.0 license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;

namespace FriendlyCSharp.Databases
{
  public partial class FcsFastLockBTreeN<TKey, TValue> : FcsFastBTreeN<TKey, TValue>
                                                          where TKey : struct, IComparable<TKey>
  {
    private object _btnLock;
    //////////////////////////
    public FcsFastLockBTreeN(uint idxFast) : base(_btnDefaultBTreeN, idxFast, null)
    {
      _btnLock = _btnLockAdd;
    }
    //////////////////////////
    protected FcsFastLockBTreeN(uint idxFast, object objCmp) : base(_btnDefaultBTreeN, idxFast, objCmp)
    {
      _btnLock = _btnLockAdd;
    }
    //////////////////////////
    public FcsFastLockBTreeN(int btnBTreeN, uint idxFast) : base(btnBTreeN, idxFast, null)
    {
      _btnLock = _btnLockAdd;
    }
    //////////////////////////
    protected FcsFastLockBTreeN(int btnBTreeN, uint idxFast, object objCmp) : base(btnBTreeN, idxFast, objCmp)
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
    //////////////////////////
    public override bool? BtnFastFind(TKey key, out TValue value, uint idxFast)
    {
      lock (_btnLock)
        return base.BtnFastFind(key, out value, idxFast);
    }
    //////////////////////////
    protected override bool? BtnFastFind(TKey key, out TValue value, out KeyValueFast btnFast)
    {
      lock (_btnLock)
        return base.BtnFastFind(key, out value, out btnFast);
    }
    //////////////////////////
#if KEY_VALUE_PAIR
    public override KeyValuePair<TKey, TValue>? BtnFastFind(TKey key, uint idxFast)
    {
      lock (_btnLock)
        return base.BtnFastFind(key, idxFast);
    }
#else
    public override (TKey key, TValue value)? BtnFastFind(TKey key, uint idxFast)
    {
      lock (_btnLock)
        return base.BtnFastFind(key, idxFast);
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
    //////////////////////////
    public override bool? BtnFastFirst(out TKey key, out TValue value, uint idxFast)
    {
      lock (_btnLock)
        return base.BtnFastFirst(out key, out value, idxFast);
    }
    //////////////////////////
    protected override bool? BtnFastFirst(out TKey key, out TValue value, out KeyValueFast btnFast)
    {
      lock (_btnLock)
        return base.BtnFastFirst(out key, out value, out btnFast);
    }
    //////////////////////////
#if KEY_VALUE_PAIR
    public override KeyValuePair<TKey, TValue>? BtnFastFirst(uint idxFast)
    {
      lock (_btnLock)
        return base.BtnFastFirst(idxFast);
    }
#else
    public override (TKey key, TValue value)? BtnFastFirst(uint idxFast)
    {
      lock (_btnLock)
        return base.BtnFastFirst(idxFast);
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
    //////////////////////////
    public override bool? BtnFastLast(out TKey key, out TValue value, uint idxFast)
    {
      lock (_btnLock)
        return base.BtnFastLast(out key, out value, idxFast);
    }
    //////////////////////////
    protected override bool? BtnFastLast(out TKey key, out TValue value, out KeyValueFast btnFast)
    {
      lock (_btnLock)
        return base.BtnFastLast(out key, out value, out btnFast);
    }
    //////////////////////////
#if KEY_VALUE_PAIR
    public override KeyValuePair<TKey, TValue>? BtnFastLast(uint idxFast)
    {
      lock (_btnLock)
        return base.BtnFastLast(idxFast);
    }
#else
    public override (TKey key, TValue value)? BtnFastLast(uint idxFast)
    {
      lock (_btnLock)
        return base.BtnFastLast(idxFast);
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
    //////////////////////////
    public override bool? BtnFastNext(ref TKey key, out TValue value, uint idxFast)
    {
      lock (_btnLock)
        return base.BtnFastNext(ref key, out value, idxFast);
    }
    //////////////////////////
    protected override bool? BtnFastNext(ref TKey key, out TValue value, ref KeyValueFast btnFast)
    {
      lock (_btnLock)
        return base.BtnFastNext(ref key, out value, ref btnFast);
    }
    //////////////////////////
#if KEY_VALUE_PAIR
    public override KeyValuePair<TKey, TValue>? BtnFastNext(TKey key, uint idxFast)
    {
      lock (_btnLock)
        return base.BtnFastNext(key, idxFast);
    }
#else
    public override (TKey key, TValue value)? BtnFastNext(TKey key, uint idxFast)
    {
      lock (_btnLock)
        return base.BtnFastNext(key, idxFast);
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
    //////////////////////////
    public override bool? BtnFastPrev(ref TKey key, out TValue value, uint idxFast)
    {
      lock (_btnLock)
        return base.BtnFastPrev(ref key, out value, idxFast);
    }
    //////////////////////////
    protected override bool? BtnFastPrev(ref TKey key, out TValue value, ref KeyValueFast btnFast)
    {
      lock (_btnLock)
        return base.BtnFastPrev(ref key, out value, ref btnFast);
    }
    //////////////////////////
#if KEY_VALUE_PAIR
    public override KeyValuePair<TKey, TValue>? BtnFastPrev(TKey key, uint idxFast)
    {
      lock (_btnLock)
        return base.BtnFastPrev(key, idxFast);
    }
#else
    public override (TKey key, TValue value)? BtnFastPrev(TKey key, uint idxFast)
    {
      lock (_btnLock)
        return base.BtnFastPrev(key, idxFast);
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
    //////////////////////////
    public override bool? BtnFastSearch(ref TKey key, out TValue value, uint idxFast)
    {
      lock (_btnLock)
        return base.BtnFastSearch(ref key, out value, idxFast);
    }
    //////////////////////////
    protected override bool? BtnFastSearch(ref TKey key, out TValue value, out KeyValueFast btnFast)
    {
      lock (_btnLock)
        return base.BtnFastSearch(ref key, out value, out btnFast);
    }
    //////////////////////////
#if KEY_VALUE_PAIR
    public override KeyValuePair<TKey, TValue>? BtnFastSearch(TKey key, uint idxFast)
    {
      lock (_btnLock)
        return base.BtnFastSearch(key, idxFast);
    }
#else
    public override (TKey key, TValue value)? BtnFastSearch(TKey key, uint idxFast)
    {
      lock (_btnLock)
        return base.BtnFastSearch(key, idxFast);
    }
#endif
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////         BtnSearchPrev        //////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
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
    //////////////////////////
    public override bool? BtnFastSearchPrev(ref TKey key, out TValue value, uint idxFast)
    {
      lock (_btnLock)
        return base.BtnFastSearchPrev(ref key, out value, idxFast);
    }
    //////////////////////////
    protected override bool? BtnFastSearchPrev(ref TKey key, out TValue value, out KeyValueFast btnFast)
    {
      lock (_btnLock)
        return base.BtnFastSearchPrev(ref key, out value, out btnFast);
    }
    //////////////////////////
#if KEY_VALUE_PAIR
    public override KeyValuePair<TKey, TValue>? BtnFastSearchPrev(TKey key, uint idxFast)
    {
      lock (_btnLock)
        return base.BtnFastSearchPrev(key, idxFast);
    }
#else
    public override (TKey key, TValue value)? BtnFastSearchPrev(TKey key, uint idxFast)
    {
      lock (_btnLock)
        return base.BtnFastSearchPrev(key, idxFast);
    }
#endif
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////           BtnUsedKeys           ///////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public override uint BtnUsedKeys()
    {
      lock (_btnLock)
      {
        return base.BtnUsedKeys();
      }
    }
  }
}
